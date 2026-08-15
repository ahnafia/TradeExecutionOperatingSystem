package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/money"
)

// The fence, stated as a test.
//
// Two processes believe they own the same partition — which is exactly what a network
// partition or a consumer-group rebalance produces, and which neither Kafka nor a gateway
// can prevent. The one that lost the lease must not be able to commit, even though its
// database connection is perfectly healthy and it has no idea anything happened.
func TestFencedWriterCannotCommitAfterLosingItsLease(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	// The original owner.
	first, err := AcquireLease(ctx, r.pool, 0, LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	r.eng.SetLease(first)

	acct := r.account(ctx, "acct", money.Minor(1_000_000_00))
	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "before", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(10), LimitPrice: 14000,
	}); err != nil {
		t.Fatalf("the lease holder should be able to write: %v", err)
	}

	// A second process takes the partition. It does not ask, and the first process is
	// never told — it finds out only when it next tries to commit.
	second, err := AcquireLease(ctx, r.pool, 0, LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("takeover did not advance the epoch: %d → %d", first.Epoch(), second.Epoch())
	}

	// Every write path must refuse, not just the ones someone remembered to guard.
	_, err = r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "after", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(10), LimitPrice: 14000,
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("a fenced writer committed an order: %v", err)
	}
	if err := r.eng.Deposit(ctx, acct, money.Minor(100_00)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("a fenced writer committed a deposit: %v", err)
	}
	if _, err := r.eng.Cancel(ctx, acct); err == nil {
		t.Fatal("a fenced writer committed a cancel")
	}

	// Nothing from the fenced process reached the database.
	if n := r.countRows(ctx, `SELECT count(*) FROM orders WHERE client_order_id = 'after'`); n != 0 {
		t.Fatalf("fenced write landed anyway: %d rows", n)
	}

	// The new owner works normally.
	r.eng.SetLease(second)
	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "new-owner", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(10), LimitPrice: 14000,
	}); err != nil {
		t.Fatalf("the new lease holder should be able to write: %v", err)
	}
	r.drain(ctx)
	r.requireInvariants(ctx)
}

// Renewal must notice a takeover rather than silently extending a lease it no longer owns.
func TestRenewDetectsTakeover(t *testing.T) {
	ctx, r := newRig(t)

	first, err := AcquireLease(ctx, r.pool, 0, LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Renew(ctx); err != nil {
		t.Fatalf("renewing an owned lease should succeed: %v", err)
	}

	if _, err := AcquireLease(ctx, r.pool, 0, LeaseTTL); err != nil {
		t.Fatal(err)
	}
	if err := first.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renew should report the takeover, got %v", err)
	}
}

// Connecting a process to the wrong partition's database must fail immediately. The
// alternative is a ledger that is internally consistent everywhere and globally wrong,
// which no invariant in the system can detect.
func TestPartitionIdentityIsChecked(t *testing.T) {
	ctx, r := newRig(t)

	if err := r.eng.AssertOwnership(ctx, 0); err != nil {
		t.Fatalf("claiming an unclaimed database should succeed: %v", err)
	}
	if err := r.eng.AssertOwnership(ctx, 0); err != nil {
		t.Fatalf("reclaiming the same partition should succeed: %v", err)
	}
	if err := r.eng.AssertOwnership(ctx, 3); err == nil {
		t.Fatal("a process configured as partition 3 accepted partition 0's database")
	}
}

// An expired lease is not automatically lost — expiry only makes the partition available
// for someone else to take. Until a takeover actually happens, the holder keeps writing,
// because a self-fencing process that stopped on a slow renewal would turn a GC pause into
// an outage.
func TestExpiredButUnclaimedLeaseStillWrites(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	lease, err := AcquireLease(ctx, r.pool, 0, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	r.eng.SetLease(lease)
	acct := r.account(ctx, "acct", money.Minor(1_000_000_00))

	if _, err := r.pool.Exec(ctx,
		`UPDATE partition_leases SET expires_at = now() - interval '1 hour' WHERE partition_id = 0`); err != nil {
		t.Fatal(err)
	}

	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "still-mine", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(10), LimitPrice: 14000,
	}); err != nil {
		t.Fatalf("an unclaimed partition should still be writable by its holder: %v", err)
	}
}
