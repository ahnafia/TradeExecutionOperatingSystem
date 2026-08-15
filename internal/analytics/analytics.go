// Package analytics is the read side of the event log: it ingests orders.outcomes into
// ClickHouse (schema.sql) and answers aggregate queries — VWAP, top accounts by notional,
// fill time series, rejection/cancel rates — against the copy it built.
//
// # Why this must never be read for a financial decision
//
// ARCHITECTURE.md §8.1 states the deal for guarantee 10 plainly: "Eventual consistency for
// analytics. ClickHouse lags ≤ 5s p99. Never read for a financial decision." Everything
// upstream of this package — cash, positions, reservations, the ledger — is written inside
// one Postgres transaction per partition and is authoritative the instant that transaction
// commits (§8.1 guarantees 5 and 7). Nothing in this package shares that transaction. A
// row here can be a few seconds stale, briefly duplicated until a ReplacingMergeTree merge
// catches up, or briefly absent after this package's own ingester restarts (see below).
// None of that is a defect; it is the cost of keeping analytics off the hot path instead
// of coupling it to the trading core's commit. If a future caller is tempted to check a
// query from this package before accepting an order, sizing a reservation, or releasing
// one, that is precisely the mistake this comment exists to head off — use the core's
// Postgres state for that, always.
//
// # Offsets are not the core's offsets
//
// The trading core tracks consumed offsets in the consumer_offsets table, in the same
// transaction as the state change they represent (§8.1 guarantee 8) — offset-in-txn is
// what turns Kafka's at-least-once delivery into effectively-exactly-once application.
// This package does not touch that table, on purpose: analytics is explicitly
// eventually-consistent (§8.1 guarantee 10) and giving it a seat at that transaction, or
// making it depend on that table's contents, would quietly upgrade its guarantee to
// something the rest of this comment says it does not have — or worse, let an analytics
// stall block the financial path that table protects. See Ingester's doc comment for how
// this package tracks its own read position instead, and what that trade costs.
package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultDSN is used by callers that do not supply their own. It targets a local,
// single-node, unauthenticated ClickHouse — what docker-compose and local dev run — not a
// production endpoint.
// DefaultDSN is a development default matching the throwaway container in
// docker-compose.yml. It is not a secret and is not meant to be reused; a real deployment
// passes its own DSN and keeps the password out of the connection string entirely.
const DefaultDSN = "clickhouse://localhost:9000/trading"

// connectTimeout bounds how long the constructors below wait for the initial Ping. It is
// short on purpose: "ClickHouse is unreachable" is the constructor's job to report
// promptly so a caller can decide to run without analytics (see package doc), not to
// retry silently and make a caller's own startup hang.
const connectTimeout = 5 * time.Second

// connect opens a ClickHouse connection and verifies it with a Ping before returning it.
//
// Verifying eagerly, rather than letting the first real query surface a dead connection,
// is what makes the "constructor returns a clear error if ClickHouse is unreachable"
// contract true: a caller finds out at startup, in one place, instead of at an arbitrary
// later insert or query buried in a background goroutine's error channel.
func connect(dsn string) (driver.Conn, error) {
	if dsn == "" {
		dsn = DefaultDSN
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn %q: %w", dsn, err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse unreachable at %q: %w", dsn, err)
	}
	return conn, nil
}
