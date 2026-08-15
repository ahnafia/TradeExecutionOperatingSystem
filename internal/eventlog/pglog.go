package eventlog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgLog is a durable event log backed by Postgres.
//
// It exists because a durable consumer offset pointing into a non-durable stream loses
// data silently — see migrations/0008. Anything that runs as more than one process over
// time, which includes a CLI invoked twice, needs the log to outlive the process that
// wrote to it.
//
// It is not a toy. Per-partition ordering, gap-free offsets, and replay from an arbitrary
// position all hold, so a single-Postgres deployment is a real deployment. What it does
// not have is a broker's throughput or its retention management, which is what KafkaLog is
// for. Both satisfy the same interface, and nothing above them can tell which is running.
type PgLog struct {
	pool      *pgxpool.Pool
	parts     map[string]int32
	duplicate func(Record) bool
}

// DuplicateWhen installs a chaos hook. When it returns true for a record, that record is
// appended twice at consecutive offsets — exactly what an at-least-once relay does when it
// crashes between producing and marking the row published.
//
// It lives here as well as on MemLog so that fault injection does not force a different
// log implementation. Running chaos against an ephemeral log while the database holds
// durable offsets pointing into a different log is its own silent failure, and having one
// durable log with an injection hook removes the temptation.
func (l *PgLog) DuplicateWhen(fn func(Record) bool) { l.duplicate = fn }

// NewPgLog prepares the topics, creating partition rows for each.
func NewPgLog(ctx context.Context, pool *pgxpool.Pool, topics map[string]int32) (*PgLog, error) {
	l := &PgLog{pool: pool, parts: map[string]int32{}}
	for topic, n := range topics {
		if n < 1 {
			n = 1
		}
		l.parts[topic] = n
		for p := int32(0); p < n; p++ {
			if _, err := pool.Exec(ctx, `
				INSERT INTO log_partitions (topic, partition) VALUES ($1, $2)
				ON CONFLICT (topic, partition) DO NOTHING`, topic, p); err != nil {
				return nil, fmt.Errorf("prepare %s/%d: %w", topic, p, err)
			}
		}
	}
	return l, nil
}

func (l *PgLog) Partitions(topic string) int32 {
	if n, ok := l.parts[topic]; ok {
		return n
	}
	return 1
}

// Produce appends records atomically.
//
// The whole batch commits together, which is what preserves the relay's ordering
// guarantee: two orders for one symbol are published in outbox id order, and a partial
// batch would let a later one land without an earlier one.
func (l *PgLog) Produce(ctx context.Context, recs ...Record) error {
	if len(recs) == 0 {
		return nil
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, r := range recs {
		n, ok := l.parts[r.Topic]
		if !ok {
			return ErrUnknownTopic{Topic: r.Topic}
		}
		p := PartitionFor(r.Key, n)

		// Allocate under a row lock so concurrent producers cannot be handed the same
		// offset. FOR UPDATE also serializes them into a definite order, which is the
		// order consumers will see.
		var offset int64
		if err := tx.QueryRow(ctx, `
			UPDATE log_partitions SET next_offset = next_offset + 1
			 WHERE topic = $1 AND partition = $2
			RETURNING next_offset - 1`, r.Topic, p).Scan(&offset); err != nil {
			if err == pgx.ErrNoRows {
				return fmt.Errorf("topic %s has no partition %d", r.Topic, p)
			}
			return fmt.Errorf("allocate offset: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO log_records (topic, partition, "offset", key, value)
			VALUES ($1, $2, $3, $4, $5)`, r.Topic, p, offset, r.Key, r.Value); err != nil {
			return fmt.Errorf("append to %s/%d: %w", r.Topic, p, err)
		}

		if l.duplicate != nil && l.duplicate(r) {
			var dup int64
			if err := tx.QueryRow(ctx, `
				UPDATE log_partitions SET next_offset = next_offset + 1
				 WHERE topic = $1 AND partition = $2
				RETURNING next_offset - 1`, r.Topic, p).Scan(&dup); err != nil {
				return fmt.Errorf("allocate duplicate offset: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO log_records (topic, partition, "offset", key, value)
				VALUES ($1, $2, $3, $4, $5)`, r.Topic, p, dup, r.Key, r.Value); err != nil {
				return fmt.Errorf("append duplicate: %w", err)
			}
		}
	}
	return tx.Commit(ctx)
}

func (l *PgLog) Reader(topic string, partition int32, fromOffset int64) (Reader, error) {
	if _, ok := l.parts[topic]; !ok {
		return nil, ErrUnknownTopic{Topic: topic}
	}
	if fromOffset < 0 {
		fromOffset = 0
	}
	return &pgReader{log: l, topic: topic, partition: partition, next: fromOffset}, nil
}

// Close is a no-op: the pool belongs to whoever opened it.
func (l *PgLog) Close() error { return nil }

// Len reports how many records a partition holds, for status output.
func (l *PgLog) Len(ctx context.Context, topic string, partition int32) (int64, error) {
	var n int64
	err := l.pool.QueryRow(ctx,
		`SELECT coalesce(next_offset, 0) FROM log_partitions WHERE topic = $1 AND partition = $2`,
		topic, partition).Scan(&n)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return n, err
}

type pgReader struct {
	log       *PgLog
	topic     string
	partition int32
	next      int64
}

// Fetch returns whatever is available from the current position.
//
// It does not block. A consumer loop that needs to wait sleeps between empty fetches; a
// drain loop needs an empty result to mean "caught up" promptly, and making that case
// cheap is what keeps a synchronous drain fast enough to be usable from a CLI.
func (r *pgReader) Fetch(ctx context.Context, max int) ([]Record, error) {
	if max <= 0 {
		max = 256
	}
	rows, err := r.log.pool.Query(ctx, `
		SELECT "offset", key, value FROM log_records
		 WHERE topic = $1 AND partition = $2 AND "offset" >= $3
		 ORDER BY "offset" LIMIT $4`, r.topic, r.partition, r.next, max)
	if err != nil {
		return nil, fmt.Errorf("fetch %s/%d: %w", r.topic, r.partition, err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		rec := Record{Topic: r.topic, Partition: r.partition}
		if err := rows.Scan(&rec.Offset, &rec.Key, &rec.Value); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		r.next = out[len(out)-1].Offset + 1
	}
	return out, nil
}

func (r *pgReader) Close() error { return nil }
