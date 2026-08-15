// Package outbox publishes the transactional outbox into the event log.
//
// This is the alternative to a distributed transaction across Postgres and Kafka, and the
// trade it makes is worth stating plainly. Two-phase commit across a database and a broker
// is expensive, poorly supported, and — because the broker's participation is usually a
// lie — not actually atomic. The outbox instead makes the DATABASE the only thing that has
// to be atomic: an order and the intent to publish it commit together, and publishing is a
// separate step that can fail, retry, and duplicate.
//
// What that buys: an order acknowledged as ACCEPTED is durable, full stop. What it costs:
// at-least-once delivery, which every consumer downstream must tolerate. That cost is paid
// once, in the design, rather than every time something crashes.
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/eventlog"
)

// Relay tails the outbox table into the log.
type Relay struct {
	pool  *pgxpool.Pool
	log   eventlog.Log
	batch int

	// OnPublish is called after each successful batch, for metrics.
	OnPublish func(n int, lag time.Duration)
}

// New builds a relay. batch bounds how many rows one pass publishes.
func New(pool *pgxpool.Pool, log eventlog.Log, batch int) *Relay {
	if batch <= 0 {
		batch = 256
	}
	return &Relay{pool: pool, log: log, batch: batch}
}

// PumpOnce publishes at most one batch, returning how many rows it published.
//
// Rows are published in id order and the whole batch is produced before any row is marked,
// which together preserve per-key ordering: two orders for the same symbol reach the book
// in the order they were accepted. A relay that published concurrently, or marked rows as
// it went, could invert them — and inverting two orders for one symbol is inverting price-
// time priority, which is the one thing a matching engine may not get wrong.
//
// Exactly one relay should run per outbox. Ordering here is a property of there being a
// single publisher, not of anything the database enforces.
func (r *Relay) PumpOnce(ctx context.Context) (int, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, topic, key, payload, created_at
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT $1`, r.batch)
	if err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}

	var (
		ids     []int64
		recs    []eventlog.Record
		oldest  time.Time
		hasRows bool
	)
	for rows.Next() {
		var (
			id        int64
			topic     string
			key       string
			payload   []byte
			createdAt time.Time
		)
		if err := rows.Scan(&id, &topic, &key, &payload, &createdAt); err != nil {
			rows.Close()
			return 0, err
		}
		if !hasRows {
			oldest, hasRows = createdAt, true
		}
		ids = append(ids, id)
		recs = append(recs, eventlog.Record{Topic: topic, Key: key, Value: payload})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		return 0, nil
	}

	if err := r.log.Produce(ctx, recs...); err != nil {
		// Nothing is marked, so the whole batch is retried. Some of it may already be in
		// the log; that is the at-least-once this design accepts and consumers absorb.
		return 0, fmt.Errorf("publish %d record(s): %w", len(recs), err)
	}

	if _, err := r.pool.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		// The records ARE published; failing to mark them means they publish again on the
		// next pass. Duplicates, not loss — the correct direction to fail in.
		return 0, fmt.Errorf("mark %d row(s) published: %w", len(ids), err)
	}

	if r.OnPublish != nil {
		r.OnPublish(len(recs), time.Since(oldest))
	}
	return len(recs), nil
}

// Drain publishes until the outbox is empty.
func (r *Relay) Drain(ctx context.Context) (int, error) {
	total := 0
	for {
		n, err := r.PumpOnce(ctx)
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}

// Run publishes continuously until ctx is cancelled.
func (r *Relay) Run(ctx context.Context, interval time.Duration, onErr func(error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		n, err := r.PumpOnce(ctx)
		if err != nil && ctx.Err() == nil && onErr != nil {
			onErr(err)
		}
		// A full batch means there is probably more waiting; go straight round again
		// rather than sleeping while a backlog builds.
		if n == r.batch {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Lag reports the age of the oldest unpublished row and how many are waiting.
//
// This number is load-bearing, not merely informative: the matching engine's duplicate
// suppression is a bounded window, and it is only sound while relay lag stays well inside
// it. A relay that falls far enough behind turns a republished record into a second
// resting order. See ARCHITECTURE.md §5.2.
func (r *Relay) Lag(ctx context.Context) (backlog int64, age time.Duration, err error) {
	var oldest *time.Time
	err = r.pool.QueryRow(ctx, `
		SELECT count(*), min(created_at) FROM outbox WHERE published_at IS NULL`).
		Scan(&backlog, &oldest)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox lag: %w", err)
	}
	if oldest != nil {
		age = time.Since(*oldest)
	}
	return backlog, age, nil
}
