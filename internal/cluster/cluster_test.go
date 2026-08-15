package cluster

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/matching"
	"github.com/ahnafia/trading-system/internal/money"
	"github.com/ahnafia/trading-system/internal/outbox"
	"github.com/ahnafia/trading-system/migrations"
)

// Its own base, for the same reason the engine tests have one: Bootstrap derives
// trading_test_p0, trading_test_p1, … and truncates them, which must not touch the
// database anyone is actually using.
const baseDSN = "postgres://trading:trading@localhost:5433/trading_test?sslmode=disable"

const partitionCount = 2

// clusterRig is a genuinely partitioned deployment: one database per partition, one relay
// per partition (each has its own outbox), one matching shard, and one outcome consumer
// per partition.
type clusterRig struct {
	t         *testing.T
	set       *Set
	log       *eventlog.MemLog
	relays    []*outbox.Relay
	match     *matching.Service
	consumers []*engine.OutcomeConsumer
	md        *marketdata.Cache
}

func newClusterRig(t *testing.T) (context.Context, *clusterRig) {
	t.Helper()
	ctx := context.Background()

	base := os.Getenv("TRADING_TEST_DSN")
	if base == "" {
		base = baseDSN
	}
	probe, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Skipf("no database at %s: %v", base, err)
	}
	if err := probe.Ping(ctx); err != nil {
		probe.Close()
		t.Skipf("database unreachable (run `docker compose up -d`): %v", err)
	}
	probe.Close()

	dsns, err := Bootstrap(ctx, base, partitionCount)
	if err != nil {
		t.Fatalf("bootstrap partitions: %v", err)
	}

	// Each partition's database gets the full schema. They are identical by design: a
	// partition is a shard of the same system, not a different system.
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, dsn := range dsns {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range all {
			_, _ = pool.Exec(ctx, m.SQL)
		}
		if _, err := pool.Exec(ctx, `
			TRUNCATE postings, transactions, ledger_accounts, positions, account_balances,
			         reservations, fills, orders, accounts, snapshots, outbox,
			         consumer_offsets, book_snapshots, partition_identity,
			         partition_leases RESTART IDENTITY CASCADE`); err != nil {
			pool.Close()
			t.Fatalf("truncate: %v", err)
		}
		pool.Close()
	}

	md := marketdata.NewCache()
	set, err := Open(ctx, md, Options{
		DSNs: dsns, Engine: engine.DefaultConfig(),
		LeaseTTL: engine.LeaseTTL, AcquireLeases: true,
	})
	if err != nil {
		t.Fatalf("open cluster: %v", err)
	}
	t.Cleanup(func() { set.Close(context.Background()) })

	r := &clusterRig{t: t, set: set, md: md}
	r.log = eventlog.NewMemLog(eventlog.Topics(4, partitionCount))

	for _, p := range set.Partitions {
		r.relays = append(r.relays, outbox.New(p.Pool, r.log, 256))
		// Outcome partition P carries exactly the accounts core partition P owns, because
		// both use the same hash over account_id with the same count.
		c, err := p.Engine.NewOutcomeConsumer(ctx, r.log, int32(p.ID), 256)
		if err != nil {
			t.Fatal(err)
		}
		r.consumers = append(r.consumers, c)
	}

	// The matching engine is its own service with its own storage; partition 0's database
	// stands in for it here.
	mcfg := matching.DefaultConfig()
	mcfg.SnapshotEvery = 1 << 30
	r.match = matching.New(set.Partitions[0].Pool, r.log, mcfg)
	if err := r.match.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, r
}

func (r *clusterRig) drain(ctx context.Context) {
	r.t.Helper()
	for round := 0; round < 500; round++ {
		moved := 0
		for _, relay := range r.relays {
			n, err := relay.Drain(ctx)
			if err != nil {
				r.t.Fatalf("relay: %v", err)
			}
			moved += n
		}
		n, err := r.match.PumpOnce(ctx)
		if err != nil {
			r.t.Fatalf("matching: %v", err)
		}
		moved += n
		for _, c := range r.consumers {
			n, err := c.PumpOnce(ctx)
			if err != nil {
				r.t.Fatalf("outcomes: %v", err)
			}
			moved += n
		}
		if moved == 0 {
			return
		}
	}
	r.t.Fatal("cluster did not settle")
}

// accountIn opens an account until one lands in the requested partition.
//
// Accounts cannot be steered to a partition — the hash decides — so a test that needs a
// counterparty in a specific partition has to keep asking. That is a property of the
// design, not an inconvenience: nothing in the system is allowed to choose where an
// account lives, because that choice is what the routing guarantee rests on.
func (r *clusterRig) accountIn(ctx context.Context, partition int, label string, cash money.Minor) uuid.UUID {
	r.t.Helper()
	for attempt := 0; attempt < 200; attempt++ {
		p := r.set.Partitions[partition]
		id, err := p.Engine.OpenAccount(ctx, fmt.Sprintf("%s-%d-%s", label, attempt, uuid.NewString()[:6]))
		if err != nil {
			r.t.Fatal(err)
		}
		if int(r.set.Router.For(id)) != partition {
			// Opened in the wrong place; it is inert (no balance, no orders) and simply
			// left behind rather than deleted, which keeps this helper free of cleanup
			// that could mask a real routing bug.
			continue
		}
		if cash > 0 {
			if err := p.Engine.Deposit(ctx, id, cash); err != nil {
				r.t.Fatal(err)
			}
		}
		return id
	}
	r.t.Fatalf("could not open an account in partition %d", partition)
	return uuid.Nil
}

// A fill between accounts in different partitions settles as two halves in two DATABASES.
// No SQL query can see both, so conservation has to be reconciled — and this is the test
// that the reconciliation actually adds up.
func TestCrossPartitionFillReconciles(t *testing.T) {
	ctx, r := newClusterRig(t)
	const sym = "ACME"
	r.md.Publish(marketdata.Tick{Symbol: sym, Price: 15000, At: time.Now()})

	maker := r.accountIn(ctx, 0, "maker", money.Minor(50_000_000_00))
	taker := r.accountIn(ctx, 1, "taker", money.Minor(5_000_000_00))

	makerEng := r.set.EngineFor(maker)
	takerEng := r.set.EngineFor(taker)
	if makerEng == takerEng {
		t.Fatal("maker and taker landed in the same partition; the test would prove nothing")
	}
	if err := makerEng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		if _, err := makerEng.Submit(ctx, engine.SubmitRequest{
			AccountID: maker, ClientOrderID: fmt.Sprintf("ask-%d", i), Symbol: sym,
			Side: book.Sell, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(20), LimitPrice: money.Minor(15000 + i*10),
		}); err != nil {
			t.Fatal(err)
		}
	}
	r.drain(ctx)

	view, err := takerEng.Submit(ctx, engine.SubmitRequest{
		AccountID: taker, ClientOrderID: "buy", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(50),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	settled, err := takerEng.Order(ctx, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "FILLED" {
		t.Fatalf("cross-partition order settled as %s", settled.Status)
	}

	// The two halves really did land in different databases.
	var makerHalves, takerHalves int64
	if err := r.set.Partitions[0].Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE fill_id IS NOT NULL`).Scan(&makerHalves); err != nil {
		t.Fatal(err)
	}
	if err := r.set.Partitions[1].Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE fill_id IS NOT NULL`).Scan(&takerHalves); err != nil {
		t.Fatal(err)
	}
	if makerHalves == 0 || takerHalves == 0 {
		t.Fatalf("expected halves in both partitions, got %d and %d", makerHalves, takerHalves)
	}
	t.Logf("%d halves in partition 0, %d in partition 1", makerHalves, takerHalves)

	// Neither partition can see the imbalance on its own. Only the reconciler can.
	for _, p := range r.set.Partitions {
		slice, err := p.Engine.ReconcileSlice(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if slice.ClearingByUnit["USD"] == 0 {
			t.Errorf("partition %d has no open clearing position; the fill was not bilateral", p.ID)
		}
	}

	g, err := r.set.Reconcile(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !g.OK() {
		t.Fatalf("cluster invariants violated:\n%s", g)
	}
	if g.InFlightFills != 0 {
		t.Errorf("expected everything settled, %d in flight", g.InFlightFills)
	}
	t.Logf("cluster verification:\n%s", g)
}

// A half that never settles is invisible to both partitions individually — each one's
// books balance perfectly. The reconciler is the only thing that can see the pair is
// broken, which is precisely why it exists.
func TestReconcilerCatchesAStrandedCrossPartitionHalf(t *testing.T) {
	ctx, r := newClusterRig(t)
	const sym = "ACME"
	r.md.Publish(marketdata.Tick{Symbol: sym, Price: 15000, At: time.Now()})

	maker := r.accountIn(ctx, 0, "maker", money.Minor(50_000_000_00))
	taker := r.accountIn(ctx, 1, "taker", money.Minor(5_000_000_00))
	makerEng, takerEng := r.set.EngineFor(maker), r.set.EngineFor(taker)

	if err := makerEng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}
	if _, err := makerEng.Submit(ctx, engine.SubmitRequest{
		AccountID: maker, ClientOrderID: "ask", Symbol: sym,
		Side: book.Sell, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(50), LimitPrice: 15000,
	}); err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)
	if _, err := takerEng.Submit(ctx, engine.SubmitRequest{
		AccountID: taker, ClientOrderID: "buy", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(20),
	}); err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	if g, err := r.set.Reconcile(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	} else if !g.OK() {
		t.Fatalf("should be sound before the injury:\n%s", g)
	}

	// Delete the maker's half, as a crash between the two settlement transactions would.
	if _, err := r.set.Partitions[0].Pool.Exec(ctx,
		`DELETE FROM transactions WHERE fill_id IS NOT NULL`); err != nil {
		t.Fatal(err)
	}

	// Each partition still looks locally fine — that is the whole point.
	for _, p := range r.set.Partitions {
		inv, err := p.Engine.CheckInvariants(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if inv.UnbalancedTxns != 0 {
			t.Errorf("partition %d: deleting a whole transaction should leave the rest balanced", p.ID)
		}
	}

	// The reconciler is not fooled.
	g, err := r.set.Reconcile(ctx, 0) // zero window: anything unpaired is stuck
	if err != nil {
		t.Fatal(err)
	}
	if len(g.OrphanedFills) == 0 {
		t.Fatalf("reconciler missed a stranded half:\n%s", g)
	}
	if g.ClearingByUnit["USD"] == 0 {
		t.Error("clearing should not net to zero with a half missing")
	}
	if g.OK() {
		t.Fatal("cluster reported sound with a stranded half-fill")
	}
	t.Logf("caught it:\n%s", g)
}

// Routing must be stable and must agree with the log's partitioner, or a core partition
// would consume outcomes for accounts it cannot write.
func TestRoutingAgreesWithTheLogPartitioner(t *testing.T) {
	router := NewRouter(4)
	for i := 0; i < 1000; i++ {
		id := uuid.New()
		if got, want := router.For(id), eventlog.PartitionFor(id.String(), 4); got != want {
			t.Fatalf("router put %s in partition %d, the log would put it in %d", id, got, want)
		}
	}
	// And it is stable: the same account always lands in the same place.
	id := uuid.New()
	first := router.For(id)
	for i := 0; i < 100; i++ {
		if router.For(id) != first {
			t.Fatal("routing is not deterministic")
		}
	}
}
