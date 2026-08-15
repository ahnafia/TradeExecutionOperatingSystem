# Distributed Trading & Portfolio Management System

All six phases of the plan in [ARCHITECTURE.md](ARCHITECTURE.md).

A partitioned trading core with a double-entry ledger as the source of truth, a
symbol-sharded matching engine behind an event log, and a set of invariants that stay at
zero while the system is being broken on purpose.

```
make chaos          # the headline: run under injected faults, print the block
make demo           # the whole path end to end, narrated
make serve          # continuous load with /metrics on :9464
make observability  # Prometheus + Grafana on :3001
```

`make chaos` restarts the matching engine mid-flight, restarts the core's outcome
consumers, and delivers 5% of all records twice. Every number in the block is still zero:

```
Orders tracked:              2537      Unbalanced txns:            0
Orders terminal:             2537      Orphaned fill halves:       0
Lost:                           0      Unpaired clearing:          0
Duplicated:                     0      Money conservation Δ:       0
Fills:                       1766      Position/ledger drift:      0
Duplicate records suppressed:  22      Replay/projection drift:    0
Max recovery time:          1.94s      Share conservation:         0
```

The 22 suppressed duplicates matter as much as the zeros: they are the evidence that
at-least-once delivery actually happened and was absorbed, rather than never occurring.

## What Phase 1 establishes

| | |
|---|---|
| **One durable write on the hot path** | accept is a single transaction: risk check, reservation, order — all under the account row lock |
| **Serialized writer per account** | `SELECT … FOR UPDATE` on the account row is the mutex *and* allocates `account_seq`, the replay watermark |
| **A real book, not a stubbed fill** | price-time priority, resting limit orders, partial fills, collar enforcement, restart recovery |
| **Bilateral fills** | each fill settles as two half-transactions against a `CLEARING` counterparty; each balances alone, and the pair nets to zero |
| **Deterministic event identity** | `uuid_v5(shard ‖ venue ‖ symbol ‖ book_seq ‖ side)`, so a fill re-derived after a crash is idempotent rather than money-creating |
| **Cost ≤ reserved** | the collar the reservation was sized against is enforced in the book, so cash cannot go negative |

## Architecture

```
client → trading core ──outbox──► orders.inbound ──► matching engine
           (sharded by            (keyed by symbol)     (sharded by symbol,
            account_id,                                  routes across venues)
            fenced by a                                        │
            partition lease)                                   │
                  ▲                                            │
                  └──────── orders.outcomes ◄──────────────────┘
                            (keyed by account_id;
                             TWO messages per fill,
                             one per counterparty)
```

Three properties hold the design together:

- **One durable write on the hot path.** Accept is a single transaction — risk check,
  reservation, order, and the intent to publish — under the account's row lock. There is
  no RPC whose failure could leave an order reserved but unrouted, because there is no RPC.
- **Deterministic event identity.** `uuid_v5(shard ‖ venue ‖ symbol ‖ book_seq ‖ side)`.
  A matching engine that crashes re-derives the fills it already produced, with the same
  identities, and the ledger recognises and ignores them.
- **Offset-in-transaction.** The consumer offset advances in the same transaction as the
  state change it authorizes. The log delivers at least once; the database makes the
  effect happen once.

## What Phase 2 adds

| | |
|---|---|
| **Two-phase cancel** | `PENDING_CANCEL` names the interval where the book has not yet ruled; the reservation is released only on its verdict, never on the request |
| **Proportional release** | collar headroom comes back as each partial fill settles, against the price and fee rate the reservation was actually taken under — not current config |
| **Bounded recovery** | versioned canonical snapshots; `recover(snapshot + tail)` is byte-identical to `replay(genesis)` |
| **A complete ledger** | cost basis and realized P&L are reconstructible from postings alone, so opening positions are *bought* from `EXTERNAL` rather than granted |
| **Invariants as metrics** | Prometheus endpoint, with in-flight settlement separated from genuine violations |
| **Crash sweep** | cancels stranded between request and verdict are re-driven on restart |

## What Phases 3–6 add

| | |
|---|---|
| **Matching is its own service** (P3) | behind an event log, with two interchangeable transports: an in-process log with real offsets, and Redpanda/Kafka over franz-go. Tests run the same consumer code against both |
| **Transactional outbox** (P3) | accept and the intent to publish commit together; no 2PC across Postgres and a broker |
| **Book snapshots** (P3) | a shard's durable state *is* its snapshot; recovery replays the log tail and regenerates fills idempotently |
| **Partition leases** (P4) | epoch asserted inside every write transaction, so a process that lost its partition cannot commit — regardless of what the gateway or the log believe |
| **Real partitioning** (P4) | one database per partition, routed by the same hash the log partitioner uses |
| **Cross-partition reconciler** (P4) | conservation stops being SQL once the two halves of a fill are in two databases |
| **Multiple venues + SOR** (P5) | deterministic best-price routing, sweeping across venues; makers choose where they rest |
| **Chaos controller** (P6) | fault injection with an external oracle and the verification block |
| **Metrics + dashboards** (P6) | Prometheus, Grafana provisioning, invariant gauges in the top row |
| **ClickHouse analytics** (P6) | batched ingestion off the outcome stream, explicitly never read for a financial decision |

The market maker is an ordinary client with an ordinary account. It has no privileged path
into the book — which is what keeps replay deterministic despite a random price simulator,
and what means admitting real accounts later changes nothing in the engine.

## Commands

```
tradectl migrate                                   create the schema
tradectl demo                                      the full scenario
tradectl open-account <label>
tradectl deposit <account> <dollars>
tradectl seed-shares <account> <sym> <shares> <px>
tradectl submit <account> <sym> buy|sell market|limit <shares> [price] [ioc|gtc]
tradectl cancel <order-id>
tradectl positions <account>
tradectl balances <account>
tradectl book <symbol>
tradectl invariants                                the verification block
tradectl replay [account]                          rebuild from the ledger and compare
tradectl snapshot                                  checkpoint every account
tradectl resolve-cancels                           re-drive cancels stranded by a crash
tradectl serve [addr]                              run under load with /metrics
tradectl chaos [orders]                            fault injection + verification block
```

`serve` runs the engine under continuous synthetic load — market makers quoting, takers
crossing, cancels racing fills, snapshots being written — with the invariant gauges on
`/metrics` and a plain-text summary on `/`. Invariant gauges pinned at zero on an idle
system prove nothing; this is what makes them mean something.

Accounts can be named by label or UUID. Postgres runs on **5433** (5432 was taken).

## Tests

```
make test     # unit + property + integration
make race     # the same, under the race detector
```

The ones that matter are properties, not examples:

- money and shares conserved across arbitrary random activity
- realized cost never exceeds the amount reserved at accept time
- concurrent buys on one account cannot overdraw it
- re-applying a fill three times moves no money
- `replay(genesis) == replay(snapshot + tail)`, byte-identical, against persisted snapshots
- cost basis and realized P&L reconstructible from postings alone
- a cancel request never releases a reservation; only the book's verdict does
- all three cancel/fill interleavings, forced rather than raced for
- a stranded fill half is caught after the settle window and ignored inside it
- book replay reproduces the identical sequence of fill identities
- fee arithmetic telescopes, so slicing an order never costs more than not slicing it
- a republished record never doubles the book
- redelivering the entire outcome stream moves no money
- a matching engine restart regenerates identical fill identities
- a writer that lost its partition lease cannot commit, on any write path
- a fill spanning two partition databases reconciles; a stranded half is caught
- the router takes the best price across venues, sweeping in price order

## Layout

```
cmd/tradectl        engine + CLI + scenario runner (one binary until Phase 3)
internal/money      int64 minor units and 1e-6 share units; exact 128-bit scaled multiply
internal/book       pure price-time-priority book — no clock, no random, no network
internal/ids        deterministic UUIDv5 event identity
internal/engine     accept path, half-fill settlement, cancels, invariants, replay, snapshots
internal/metrics    Prometheus registry; the engine sees only an Observer interface
internal/marketdata conflating tick cache + random-walk simulator
internal/mm         simulated market maker (an ordinary client)
internal/eventlog   ordered partitioned log: MemLog + KafkaLog, same semantics
internal/events     versioned wire contract between core and matching
internal/outbox     transactional outbox relay
internal/matching   the matching engine service, venues, smart order router
internal/cluster    partition routing, leases, cross-partition reconciler
internal/pipeline   wiring + drain
internal/chaos      fault injection and the verification block
internal/analytics  ClickHouse ingestion and queries
deploy/             Prometheus, Grafana provisioning, dashboards
migrations          schema, embedded in the binary
```

## Known limits

Deliberate, and each has a phase attached in [ARCHITECTURE.md](ARCHITECTURE.md#14-build-order):

- **No gRPC gateway.** Routing is a library (`cluster.Router`) used by the CLI and the
  harness. A gateway process is where auth lives, which is Part 2.
- **JSON on the wire, not protobuf.** `protoc` is unavailable here; the properties the
  design depends on — a version on every envelope, unknown fields ignored, names never
  reused — are preserved and enforced. See `internal/events`.
- **Snapshots use a canonical binary encoding, not protobuf**, for the same reason.
- **Analytics carries ingest-time, not event-time.** Deliberate: a regenerated fill would
  stamp a new wall-clock time and break replay determinism. See ARCHITECTURE.md §0.7.9.
- **The reconciler holds fill ids in memory.** O(fills) per run. Fine at this scale, and
  an incremental cursor is the obvious next step.
- **The synchronous drain is slow over Kafka.** `Drain` waits a poll window per round, so
  `chaos` takes ~3 minutes over Redpanda for what runs in ~20 seconds in-process. Both pass
  identically; the drain exists so tests and the CLI can be deterministic, and a real
  deployment uses `Run` (background loops) instead. Verified both ways:

  ```
  make chaos                                   # in-process log
  TRADING_KAFKA=localhost:19092 make chaos ORDERS=800   # real broker
  ```
- **Duplicate injection needs a log we control.** Both the in-process and Postgres logs
  expose the hook. A real broker cannot be asked to duplicate, so over Kafka duplication is
  induced by killing the relay between produce and mark — which is what the outbox is
  designed to survive, but it is not scripted here.

## Transports

| | when |
|---|---|
| **Postgres log** (default) | anything that outlives one process, including the CLI. One database, no broker, same semantics |
| **Kafka / Redpanda** | `TRADING_KAFKA=localhost:19092`. Real throughput and retention |
| **In-process log** | tests only, which own their whole database. Never for anything whose state outlives the process — see ARCHITECTURE.md §0.7.15 |

Tests use their own `trading_test` database and create it on demand, so `go test` cannot
destroy a live session.
- **Order book levels are a sorted slice.** O(levels) insert. Correct, and not yet fast.
- **Self-trading is permitted.** The ledger handles it correctly — both halves land in the
  same account and net out — but a real venue would prevent it.
