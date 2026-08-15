package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/matching"
	"github.com/ahnafia/trading-system/internal/money"
	"github.com/ahnafia/trading-system/internal/outbox"
	"github.com/ahnafia/trading-system/migrations"
)

// Tests get their OWN database.
//
// They truncate every table on setup, and pointing that at the database the CLI uses means
// `go test` silently destroys whatever you were doing. Sharing one database between a
// destructive test suite and a live system is the kind of thing that is obviously wrong
// once it bites and completely invisible until then.
const (
	baseDSN = "postgres://trading:trading@localhost:5433/trading?sslmode=disable"
	testDSN = "postgres://trading:trading@localhost:5433/trading_test?sslmode=disable"
)

// ensureTestDB creates the test database if it does not exist.
func ensureTestDB(ctx context.Context, t *testing.T) {
	t.Helper()
	admin, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Skipf("no database at %s: %v", baseDSN, err)
	}
	defer admin.Close()
	if err := admin.Ping(ctx); err != nil {
		t.Skipf("database unreachable (run `docker compose up -d`): %v", err)
	}
	var exists bool
	if err := admin.QueryRow(ctx,
		`SELECT true FROM pg_database WHERE datname = 'trading_test'`).Scan(&exists); err != nil {
		if _, err := admin.Exec(ctx, `CREATE DATABASE trading_test`); err != nil {
			t.Fatalf("create test database: %v", err)
		}
	}
}

// rig is the whole system, wired for tests.
//
// It builds the pipeline by hand rather than importing internal/pipeline, because that
// package imports this one. The wiring is the same: an in-process log carrying real
// offsets, a relay, a matching shard, and outcome consumers. Nothing here bypasses the
// log, so a test that passes is a statement about the deployed topology and not about a
// convenient shortcut.
type rig struct {
	t         *testing.T
	eng       *Engine
	md        *marketdata.Cache
	log       *eventlog.MemLog
	relay     *outbox.Relay
	match     *matching.Service
	consumers []*OutcomeConsumer
	pool      *pgxpool.Pool
	venues    []matching.Venue
	postSeq   int
}

const (
	testInboundPartitions = 4
	testOutcomePartitions = 4
)

// newRig gives each test a clean database and a fresh pipeline over it.
func newRig(t *testing.T) (context.Context, *rig) {
	t.Helper()

	dsn := os.Getenv("TRADING_TEST_DSN")
	if dsn == "" {
		dsn = testDSN
	}
	ctx := context.Background()
	if os.Getenv("TRADING_TEST_DSN") == "" {
		ensureTestDB(ctx, t)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no database at %s: %v", dsn, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("database unreachable (run `docker compose up -d`): %v", err)
	}
	t.Cleanup(pool.Close)

	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range all {
		// Already-applied schema is fine; a genuinely broken migration surfaces
		// immediately as a failing query rather than silently here.
		_, _ = pool.Exec(ctx, m.SQL)
	}
	if _, err := pool.Exec(ctx, `
		TRUNCATE postings, transactions, ledger_accounts, positions, account_balances,
		         reservations, fills, orders, accounts, snapshots, outbox,
		         consumer_offsets, book_snapshots RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	md := marketdata.NewCache()
	r := &rig{t: t, md: md, pool: pool}
	r.build(ctx, md, DefaultConfig())
	return ctx, r
}

// newRigWithVenues builds a rig whose matching shard runs more than one venue, so the
// smart order router has something to route between.
func newRigWithVenues(t *testing.T, venues []matching.Venue) (context.Context, *rig) {
	t.Helper()
	ctx, r := newRig(t)
	r.venues = venues
	r.build(ctx, r.md, r.eng.cfg)
	return ctx, r
}

// build wires a fresh pipeline over the rig's database and log. Called again by restart()
// to model a process coming back up against durable state it did not create.
func (r *rig) build(ctx context.Context, md *marketdata.Cache, cfg Config) {
	r.t.Helper()

	if r.log == nil {
		r.log = eventlog.NewMemLog(eventlog.Topics(testInboundPartitions, testOutcomePartitions))
	}

	r.eng = New(r.pool, md, cfg)
	r.relay = outbox.New(r.pool, r.log, 256)

	mcfg := matching.DefaultConfig()
	mcfg.CollarBps = cfg.CollarBps
	mcfg.SnapshotEvery = 1 << 30 // snapshot only when a test asks for it
	if len(r.venues) > 0 {
		mcfg.Venues = r.venues
	}
	r.match = matching.New(r.pool, r.log, mcfg)
	if err := r.match.Recover(ctx); err != nil {
		r.t.Fatalf("recover matching: %v", err)
	}

	r.consumers = nil
	for p := int32(0); p < testOutcomePartitions; p++ {
		c, err := r.eng.NewOutcomeConsumer(ctx, r.log, p, 256)
		if err != nil {
			r.t.Fatalf("outcome consumer %d: %v", p, err)
		}
		r.consumers = append(r.consumers, c)
	}
}

// restart rebuilds every stage against the same log and database, as a process restart
// would. Books come back from their snapshot and the tail of the log; consumers come back
// from their durable offsets.
func (r *rig) restart(ctx context.Context) {
	r.t.Helper()
	r.build(ctx, r.md, r.eng.cfg)
}

// drain pumps every stage until the system is quiet.
func (r *rig) drain(ctx context.Context) {
	r.t.Helper()
	if err := r.tryDrain(ctx); err != nil {
		r.t.Fatalf("drain: %v", err)
	}
}

func (r *rig) tryDrain(ctx context.Context) error {
	for round := 0; round < 500; round++ {
		moved := 0

		n, err := r.relay.Drain(ctx)
		if err != nil {
			return err
		}
		moved += n

		if n, err = r.match.PumpOnce(ctx); err != nil {
			return err
		}
		moved += n

		for _, c := range r.consumers {
			if n, err = c.PumpOnce(ctx); err != nil {
				return err
			}
			moved += n
		}
		if moved == 0 {
			return nil
		}
	}
	r.t.Fatal("pipeline did not settle")
	return nil
}

// rewindOutcomes moves every outcome consumer back to the start of its partition, so the
// whole outcome stream is delivered a second time. This is redelivery, which the log is
// entitled to do and the ledger must absorb without moving a cent.
func (r *rig) rewindOutcomes(ctx context.Context) {
	r.t.Helper()
	if _, err := r.pool.Exec(ctx,
		`DELETE FROM consumer_offsets WHERE topic = $1`, eventlog.TopicOrdersOutcomes); err != nil {
		r.t.Fatalf("rewind offsets: %v", err)
	}
	r.consumers = nil
	for p := int32(0); p < testOutcomePartitions; p++ {
		c, err := r.eng.NewOutcomeConsumer(ctx, r.log, p, 256)
		if err != nil {
			r.t.Fatalf("outcome consumer %d: %v", p, err)
		}
		r.consumers = append(r.consumers, c)
	}
}

func (r *rig) account(ctx context.Context, label string, cash money.Minor) uuid.UUID {
	r.t.Helper()
	id, err := r.eng.OpenAccount(ctx, label+"-"+uuid.NewString()[:8])
	if err != nil {
		r.t.Fatal(err)
	}
	if cash > 0 {
		if err := r.eng.Deposit(ctx, id, cash); err != nil {
			r.t.Fatal(err)
		}
	}
	return id
}

// postAt rests a maker's limit order at a named venue, through the ordinary accept path.
func (r *rig) postAt(ctx context.Context, account uuid.UUID, symbol, venue string, price money.Minor, shares int64) uuid.UUID {
	r.t.Helper()
	r.postSeq++
	v, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID:     account,
		ClientOrderID: fmt.Sprintf("post-%s-%d", venue, r.postSeq),
		Symbol:        symbol, Side: book.Sell, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(shares), LimitPrice: price, Venue: venue,
	})
	if err != nil {
		r.t.Fatalf("post at %s: %v", venue, err)
	}
	r.drain(ctx)
	if got := r.status(ctx, v.ID); got != "ACCEPTED" {
		r.t.Fatalf("post at %s did not rest: %s", venue, got)
	}
	return v.ID
}

func (r *rig) mark(symbol string, price money.Minor) {
	r.md.Publish(marketdata.Tick{Symbol: symbol, Price: price, At: time.Now()})
}

// status reads an order's settled state.
func (r *rig) status(ctx context.Context, id uuid.UUID) string {
	r.t.Helper()
	v, err := r.eng.Order(ctx, id)
	if err != nil {
		r.t.Fatalf("load order: %v", err)
	}
	return v.Status
}

func (r *rig) fills(ctx context.Context, id uuid.UUID) []FillRow {
	r.t.Helper()
	f, err := r.eng.OrderFills(ctx, id)
	if err != nil {
		r.t.Fatalf("load fills: %v", err)
	}
	return f
}

func (r *rig) balances(ctx context.Context, id uuid.UUID) Balances {
	r.t.Helper()
	b, err := r.eng.Balances(ctx, id)
	if err != nil {
		r.t.Fatalf("load balances: %v", err)
	}
	return b
}

// requireInvariants asserts the whole verification block.
func (r *rig) requireInvariants(ctx context.Context) {
	r.t.Helper()
	inv, err := r.eng.CheckInvariants(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	if !inv.OK() {
		r.t.Fatalf("invariant violation:\n%s", inv)
	}
	worst, err := r.eng.ReservationBound(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	if worst > 0 {
		r.t.Fatalf("an order consumed %d more than it reserved", worst)
	}
}

// countRows is a small helper for assertions about durable state.
func (r *rig) countRows(ctx context.Context, query string, args ...any) int64 {
	r.t.Helper()
	var n int64
	if err := r.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		r.t.Fatalf("count: %v", err)
	}
	return n
}
