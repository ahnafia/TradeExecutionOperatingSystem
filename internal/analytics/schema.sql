-- Schema for the analytics store described in internal/analytics.
--
-- This is a copy of the trading core's outcomes, not a second source of truth for them
-- (ARCHITECTURE.md §8.1 guarantee 10). Nothing here participates in accepting an order,
-- sizing a reservation, or settling a fill; if a query against these tables ever ends up on
-- that path, that is a bug in the caller, not a feature of this schema.
--
-- Money and quantity columns are stored exactly as they travel on orders.outcomes: Int64
-- minor units (cents) and Int64 in 1e-6-share units, matching internal/money's convention.
-- No rounding, no floats, no unit conversion happens on the way in — only in query output,
-- where a VWAP or a rate is inherently a ratio and ClickHouse's float aggregates are the
-- right tool for it (see internal/analytics's package doc).
--
-- Both tables use ReplacingMergeTree because internal/eventlog is at-least-once
-- (eventlog.go's doc comment) and this package deliberately keeps no durable offset
-- checkpoint of its own (see Ingester's doc comment for why). A restarted ingester
-- replays part or all of orders.outcomes' retained window and re-inserts rows it has
-- already written; ReplacingMergeTree collapses rows that share a sort key during a
-- background merge, and a query that cannot wait for that merge can force it with the
-- FINAL modifier. That is a query-time cost traded deliberately against not needing a
-- second durable offset store that would have to be reconciled with the trading core's.

-- fills is one row per FILL_HALF message — i.e. two rows per execution, one per account,
-- exactly as the wire format produces them (events.go's FillHalf doc comment explains why
-- a fill is decomposed into two unilateral halves). Queries that want one row per
-- execution rather than one per participant filter side = 'TAKER'; queries that want each
-- account's own participation (e.g. traded notional per account) do not.
--
-- event_id is the sort key's tiebreaker because it is the one identifier in the wire
-- format that is guaranteed both unique and deterministic under replay (ARCHITECTURE.md
-- §5.3): a redelivered FILL_HALF carries the same event_id, symbol, and timestamp bucket
-- every time, so duplicates always land in the same ReplacingMergeTree sort group instead
-- of scattering across the table.
CREATE TABLE IF NOT EXISTS fills
(
    fill_id    UUID,
    event_id   UUID,
    shard_id   Int32,
    symbol     LowCardinality(String),
    book_seq   UInt64,
    side       LowCardinality(String), -- TAKER | MAKER
    account_id UUID,
    order_id   UUID,
    price      Int64,                  -- money.Minor: minor units per whole share
    qty        Int64,                  -- money.Qty: 1e-6-share units
    buying     UInt8,                  -- 1 if this side acquired shares, 0 if it gave them up
    ts         DateTime64(3, 'UTC')    -- ingest-time, not event-time; see Ingester's doc comment
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts, event_id);

-- order_events covers ORDER_DISPOSITION and CANCEL_OUTCOME, the two orders.outcomes
-- message types that report an order's fate rather than a specific execution. They share
-- a table because both answer the same analytical question — "why did this order not end
-- up fully filled" — and splitting them would force every rejection/cancel-rate query to
-- union two tables for no benefit.
--
-- kind distinguishes which wire message produced the row: DISPOSITION or CANCEL_OUTCOME.
-- disposition carries Disposition.Disposition (RESTED | CANCELLED | COMPLETE) for a
-- DISPOSITION row, or a normalized CANCELLED | CANCEL_REJECTED for a CANCEL_OUTCOME row
-- (derived from CancelOutcome.Cancelled, since that message has no disposition field of
-- its own). reason carries whichever reason string the source message set
-- (COLLAR_BREACH, BOOK_EXHAUSTED, ALREADY_FILLED, ...), empty when the message left it
-- unset.
--
-- Neither wire message carries an identifier of its own (unlike FillHalf's event_id), so
-- the dedup key falls back to the one thing that is guaranteed unique per delivery
-- attempt: the (kafka_partition, kafka_offset) position the ingester read it from. A
-- redelivery of the same record after an ingester restart lands at the same offset every
-- time, so it collapses correctly; two distinct real events for the same order (e.g. a
-- RESTED disposition followed later by a COMPLETE one) do not, because they occupy
-- different offsets.
CREATE TABLE IF NOT EXISTS order_events
(
    order_id        UUID,
    account_id      UUID,
    symbol          LowCardinality(String),
    kind            LowCardinality(String), -- DISPOSITION | CANCEL_OUTCOME
    disposition     LowCardinality(String), -- see comment above
    reason          LowCardinality(String), -- COLLAR_BREACH, BOOK_EXHAUSTED, ALREADY_FILLED, ...
    remaining       Int64,                  -- money.Qty: 1e-6-share units
    kafka_partition Int32,
    kafka_offset    Int64,
    ts              DateTime64(3, 'UTC')    -- ingest-time, not event-time; see Ingester's doc comment
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts, order_id, kafka_partition, kafka_offset);
