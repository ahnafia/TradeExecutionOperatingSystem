package analytics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/events"
)

// IngesterConfig tunes batching and where the Ingester starts reading.
type IngesterConfig struct {
	// DSN is the ClickHouse connection string. Empty uses DefaultDSN.
	DSN string

	// BatchSize and BatchInterval bound how long a decoded row waits before being
	// written, and whichever limit is crossed first triggers the flush. This is the
	// entire reason this package reaches for ClickHouse instead of, say, writing one row
	// per outcome straight into Postgres: ClickHouse is built around large, infrequent
	// block inserts, and a stream of single-row inserts creates enough parts that
	// background merges cannot keep up, degrading both write and read paths.
	BatchSize     int
	BatchInterval time.Duration

	// StartOffsets seeds the per-partition read position on Recover. A partition absent
	// from this map starts at offset 0 — the oldest record orders.outcomes still retains
	// (7d, per ARCHITECTURE.md §7), not necessarily the true beginning of the topic. This
	// is the "simple local map" this package uses in place of a durable checkpoint; see
	// Ingester's doc comment for why that trade is acceptable here and would not be on
	// the trading core's consumer.
	StartOffsets map[int32]int64

	// IdlePoll is how long Run waits between passes when the last one consumed nothing.
	IdlePoll time.Duration
}

func (c IngesterConfig) withDefaults() IngesterConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 10_000
	}
	if c.BatchInterval <= 0 {
		c.BatchInterval = time.Second
	}
	if c.IdlePoll <= 0 {
		c.IdlePoll = 200 * time.Millisecond
	}
	return c
}

// Ingester consumes every partition of orders.outcomes and batches decoded rows into
// ClickHouse's fills and order_events tables.
//
// # Single-goroutine pump, not one goroutine per partition
//
// PumpOnce visits every owned partition in one pass, on one goroutine, the same shape
// internal/matching.Service uses for its own consume loop and for the same reason: the
// batch-on-size-or-time decision is made once, in one place, against a buffer every
// partition feeds. A per-partition-goroutine design would need its own coordination to
// keep that decision meaningful across partitions (a "batch full" signal from partition 3
// while partition 7's goroutine is mid-append is a race), and that coordination would
// buy nothing here — orders.outcomes has no cross-partition ordering requirement to
// preserve (eventlog.go's doc comment), so there is no correctness reason to parallelize
// the reads, only a throughput one this package does not need yet.
//
// # No durable offset checkpoint
//
// Every other consumer in this system that must not reprocess work durably records its
// offset next to the state it produced: the core in the same Postgres transaction
// (§8.1 guarantee 8), the matching engine in its book snapshot (matching.go's doc
// comment). Both of those consumers write state whose duplication would be a correctness
// bug — a re-applied fill is money, a re-derived book fork is a wrong book. A re-inserted
// analytics row is neither: it either sums into the same aggregate it already sums into,
// or it is the ReplacingMergeTree-deduplicated same row it already was (schema.sql). That
// difference is what makes an in-memory offset map, wiped on every restart, an honest
// trade rather than a shortcut: on restart Recover starts over from StartOffsets (default:
// the oldest retained record) and replays whatever the topic still holds, and the schema's
// dedup key absorbs the resulting duplicates. It costs one bounded burst of re-ingestion
// per restart; it saves this package from needing — and from ever getting to disagree
// with — a second offset store alongside the core's.
type Ingester struct {
	cfg  IngesterConfig
	log  eventlog.Log
	conn driver.Conn

	mu      sync.Mutex
	parts   map[int32]*ingestPartition
	fillBuf []fillRow
	evtBuf  []orderEventRow
	pending int
	since   time.Time

	// OnFlush is called after each successful write, for metrics — mirrors
	// outbox.Relay.OnPublish.
	OnFlush func(table string, n int, lag time.Duration)
}

type ingestPartition struct {
	partition int32
	reader    eventlog.Reader
	next      int64
}

// fillRow is one FILL_HALF message, shaped for the fills table's column order.
type fillRow struct {
	FillID    uuid.UUID
	EventID   uuid.UUID
	ShardID   int32
	Symbol    string
	BookSeq   uint64
	Side      string
	AccountID uuid.UUID
	OrderID   uuid.UUID
	Price     int64
	Qty       int64
	Buying    uint8
	TS        time.Time
}

// orderEventRow is one ORDER_DISPOSITION or CANCEL_OUTCOME message, shaped for the
// order_events table's column order.
type orderEventRow struct {
	OrderID        uuid.UUID
	AccountID      uuid.UUID
	Symbol         string
	Kind           string
	Disposition    string
	Reason         string
	Remaining      int64
	KafkaPartition int32
	KafkaOffset    int64
	TS             time.Time
}

// NewIngester connects to ClickHouse and prepares an ingester. Call Recover before Run or
// PumpOnce.
//
// Connecting and verifying reachability here, rather than lazily on first use, is what
// lets a caller decide once, at startup, whether to run without analytics rather than
// finding out mid-stream: analytics is explicitly best-effort (package doc) and must never
// be the reason the trading core fails to start.
func NewIngester(log eventlog.Log, cfg IngesterConfig) (*Ingester, error) {
	conn, err := connect(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("analytics: new ingester: %w", err)
	}
	return &Ingester{
		cfg:   cfg.withDefaults(),
		log:   log,
		conn:  conn,
		parts: map[int32]*ingestPartition{},
		since: time.Now(),
	}, nil
}

// Recover opens a reader on every partition of orders.outcomes, positioned at
// cfg.StartOffsets (default: offset 0, the oldest retained record).
func (in *Ingester) Recover(ctx context.Context) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	total := in.log.Partitions(eventlog.TopicOrdersOutcomes)
	for p := int32(0); p < total; p++ {
		offset := in.cfg.StartOffsets[p]
		reader, err := in.log.Reader(eventlog.TopicOrdersOutcomes, p, offset)
		if err != nil {
			return fmt.Errorf("analytics: reader for orders.outcomes partition %d: %w", p, err)
		}
		in.parts[p] = &ingestPartition{partition: p, reader: reader, next: offset}
	}
	return nil
}

// PumpOnce fetches whatever is currently available on every partition, decodes it, and
// flushes to ClickHouse once the size or time batch limit is crossed — whichever comes
// first. It returns how many log records it consumed (not rows written: a batch may flush
// mid-call, or not at all, independent of how many records this particular call saw).
func (in *Ingester) PumpOnce(ctx context.Context) (int, error) {
	in.mu.Lock()
	defer in.mu.Unlock()

	total := 0
	for _, part := range in.parts {
		recs, err := part.reader.Fetch(ctx, in.cfg.BatchSize)
		if err != nil {
			return total, fmt.Errorf("analytics: fetch orders.outcomes partition %d: %w", part.partition, err)
		}
		for _, rec := range recs {
			if err := in.decode(rec); err != nil {
				// Refusing to advance past a record this package cannot read, rather than
				// skipping it, matches events.Decode's own rule: silently losing a row
				// leaves no trace of why an aggregate came out short, and a wrong VWAP
				// that fails loudly is far better than one that fails quietly.
				return total, fmt.Errorf("analytics: orders.outcomes partition %d offset %d: %w",
					rec.Partition, rec.Offset, err)
			}
			part.next = rec.Offset + 1
			total++
		}
	}
	if err := in.flushIfDue(ctx); err != nil {
		return total, err
	}
	return total, nil
}

// Run pumps until ctx is cancelled, then flushes whatever is buffered before returning —
// otherwise the last partial batch would sit in memory and be lost, silently understating
// every aggregate until the next restart's replay happened to cover the same ground.
func (in *Ingester) Run(ctx context.Context, onErr func(error)) {
	for {
		n, err := in.PumpOnce(ctx)
		if err != nil && ctx.Err() == nil && onErr != nil {
			onErr(err)
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			in.mu.Lock()
			if ferr := in.flush(context.WithoutCancel(ctx)); ferr != nil && onErr != nil {
				onErr(ferr)
			}
			in.mu.Unlock()
			return
		case <-time.After(in.cfg.IdlePoll):
		}
	}
}

// Close closes every partition reader and the ClickHouse connection. Buffered rows that
// were never flushed are dropped — callers that need a clean shutdown should cancel Run's
// context and let it flush first, rather than calling Close directly on a running pump.
func (in *Ingester) Close() error {
	in.mu.Lock()
	defer in.mu.Unlock()
	for _, part := range in.parts {
		_ = part.reader.Close()
	}
	return in.conn.Close()
}

// decode turns one orders.outcomes record into a buffered row.
//
// Message types this ingester does not recognise are skipped rather than treated as an
// error. orders.outcomes is specified to carry more shapes than these two over time
// (ARCHITECTURE.md §7 lists FILL_HALF, CANCELLED, CANCEL_REJECTED, and EXPIRED as an
// evolving set under one topic), and a forward-compatible addition to that set should
// never take the analytics pipeline down — that would be a worse failure than an
// incomplete view, and it is precisely the asymmetry events.Decode's SchemaVersion check
// exists to allow: refuse what cannot be READ, not what merely is not yet MODELED here.
func (in *Ingester) decode(rec eventlog.Record) error {
	env, err := events.Decode(rec.Value)
	if err != nil {
		return err
	}

	// Ingest-time, not event-time: none of OrderAccepted, FillHalf, Disposition, or
	// CancelOutcome carries a timestamp on the wire (internal/events/events.go). Using
	// the moment this ingester happened to observe the record is the only option
	// available without changing that contract, and it means ts reflects consumer lag as
	// much as it reflects when the book actually decided something — a gap worth knowing
	// about before trusting a time-bucketed query's edges too precisely.
	now := time.Now().UTC()

	switch env.Type {
	case events.TypeFillHalf:
		msg, err := events.Into[events.FillHalf](env)
		if err != nil {
			return err
		}
		buying := uint8(0)
		if msg.Buying {
			buying = 1
		}
		in.fillBuf = append(in.fillBuf, fillRow{
			FillID:    msg.FillID,
			EventID:   msg.EventID,
			ShardID:   int32(msg.ShardID),
			Symbol:    msg.Symbol,
			BookSeq:   msg.BookSeq,
			Side:      msg.Side,
			AccountID: msg.AccountID,
			OrderID:   msg.OrderID,
			Price:     msg.Price,
			Qty:       msg.Qty,
			Buying:    buying,
			TS:        now,
		})
		in.pending++

	case events.TypeDisposition:
		msg, err := events.Into[events.Disposition](env)
		if err != nil {
			return err
		}
		in.evtBuf = append(in.evtBuf, orderEventRow{
			OrderID:        msg.OrderID,
			AccountID:      msg.AccountID,
			Symbol:         msg.Symbol,
			Kind:           "DISPOSITION",
			Disposition:    msg.Disposition,
			Reason:         msg.Reason,
			Remaining:      msg.Remaining,
			KafkaPartition: rec.Partition,
			KafkaOffset:    rec.Offset,
			TS:             now,
		})
		in.pending++

	case events.TypeCancelOutcome:
		msg, err := events.Into[events.CancelOutcome](env)
		if err != nil {
			return err
		}
		// CancelOutcome has no disposition field of its own (§5.6: a verdict of
		// Cancelled=false means the fill won the race, not that anything failed), so the
		// column is normalized from the one bool the message does carry.
		disposition := "CANCEL_REJECTED"
		if msg.Cancelled {
			disposition = "CANCELLED"
		}
		in.evtBuf = append(in.evtBuf, orderEventRow{
			OrderID:        msg.OrderID,
			AccountID:      msg.AccountID,
			Symbol:         msg.Symbol,
			Kind:           "CANCEL_OUTCOME",
			Disposition:    disposition,
			Reason:         msg.Reason,
			Remaining:      msg.Remaining,
			KafkaPartition: rec.Partition,
			KafkaOffset:    rec.Offset,
			TS:             now,
		})
		in.pending++

	case events.TypeOrderAccepted, events.TypeCancelRequested:
		// These are orders.inbound message types (eventlog.go's TopicOrdersInbound doc
		// comment); orders.outcomes should never carry them, but a routing bug upstream
		// is not this package's job to diagnose by crashing the ingester. Silently
		// dropping is the right choice ONLY because it is scoped to a bug class analytics
		// itself cannot mask (an upstream misroute leaves its own trace on the producing
		// side); decode's error path above still refuses anything it cannot even parse.
	}
	return nil
}

// flushIfDue writes the buffered rows if either batch limit has been crossed.
func (in *Ingester) flushIfDue(ctx context.Context) error {
	due := in.pending >= in.cfg.BatchSize ||
		(in.pending > 0 && time.Since(in.since) >= in.cfg.BatchInterval)
	if !due {
		return nil
	}
	return in.flush(ctx)
}

// flush writes whatever is buffered to ClickHouse. On a partial failure — fills wrote but
// order_events did not, or vice versa — the buffers are left un-cleared so the next call
// retries the whole thing; ReplacingMergeTree absorbs the resulting duplicate of whichever
// half already landed (schema.sql), which is a cheaper failure mode than losing rows that
// never get retried.
func (in *Ingester) flush(ctx context.Context) error {
	if in.pending == 0 {
		return nil
	}
	lag := time.Since(in.since)

	if len(in.fillBuf) > 0 {
		if err := in.writeFills(ctx, in.fillBuf); err != nil {
			return fmt.Errorf("analytics: flush %d fills row(s): %w", len(in.fillBuf), err)
		}
		if in.OnFlush != nil {
			in.OnFlush("fills", len(in.fillBuf), lag)
		}
	}
	if len(in.evtBuf) > 0 {
		if err := in.writeOrderEvents(ctx, in.evtBuf); err != nil {
			return fmt.Errorf("analytics: flush %d order_events row(s): %w", len(in.evtBuf), err)
		}
		if in.OnFlush != nil {
			in.OnFlush("order_events", len(in.evtBuf), lag)
		}
	}

	in.fillBuf = nil
	in.evtBuf = nil
	in.pending = 0
	in.since = time.Now()
	return nil
}

func (in *Ingester) writeFills(ctx context.Context, rows []fillRow) error {
	batch, err := in.conn.PrepareBatch(ctx,
		"INSERT INTO fills (fill_id, event_id, shard_id, symbol, book_seq, side, account_id, order_id, price, qty, buying, ts)")
	if err != nil {
		return fmt.Errorf("prepare fills batch: %w", err)
	}
	defer batch.Close()
	for _, r := range rows {
		if err := batch.Append(
			r.FillID, r.EventID, r.ShardID, r.Symbol, r.BookSeq, r.Side,
			r.AccountID, r.OrderID, r.Price, r.Qty, r.Buying, r.TS,
		); err != nil {
			return fmt.Errorf("append fills row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send fills batch: %w", err)
	}
	return nil
}

func (in *Ingester) writeOrderEvents(ctx context.Context, rows []orderEventRow) error {
	batch, err := in.conn.PrepareBatch(ctx,
		"INSERT INTO order_events (order_id, account_id, symbol, kind, disposition, reason, remaining, kafka_partition, kafka_offset, ts)")
	if err != nil {
		return fmt.Errorf("prepare order_events batch: %w", err)
	}
	defer batch.Close()
	for _, r := range rows {
		if err := batch.Append(
			r.OrderID, r.AccountID, r.Symbol, r.Kind, r.Disposition, r.Reason,
			r.Remaining, r.KafkaPartition, r.KafkaOffset, r.TS,
		); err != nil {
			return fmt.Errorf("append order_events row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send order_events batch: %w", err)
	}
	return nil
}
