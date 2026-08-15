package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/money"
)

// reservationRow is the raw state of an order's reservation.
type reservationRow struct{ reserved, consumed, released int64 }

func (r *rig) reservation(ctx context.Context, orderID uuid.UUID) reservationRow {
	r.t.Helper()
	var row reservationRow
	err := r.pool.QueryRow(ctx,
		`SELECT reserved, consumed, released FROM reservations WHERE order_id = $1`, orderID).
		Scan(&row.reserved, &row.consumed, &row.released)
	if err != nil {
		r.t.Fatalf("load reservation: %v", err)
	}
	return row
}

// A market BUY reserves against a collar-widened price. When it fills below that, the
// difference should come back as each slice executes — not be frozen until the order
// terminates.
func TestReservationReleasesHeadroomPerFill(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(10_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(1_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}

	// Several levels, all comfortably inside the 5% collar, so the buy fills in slices at
	// prices well below the price its reservation was sized at.
	for i := 0; i < 4; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: maker, ClientOrderID: fmt.Sprintf("ask-%d", i), Symbol: sym,
			Side: book.Sell, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(10), LimitPrice: money.Minor(15000 + i*10),
		}); err != nil {
			t.Fatal(err)
		}
	}
	r.drain(ctx)

	// 40 shares available against 50 ordered: the order fills in slices and stays working,
	// so any release observed came from per-fill settlement rather than termination.
	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "buy-1", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(50), LimitPrice: 15400,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	if fills := r.fills(ctx, view.ID); len(fills) < 3 {
		t.Fatalf("expected the order to fill in slices, got %d fills", len(fills))
	}
	if got := r.status(ctx, view.ID); got != "PARTIALLY_FILLED" {
		t.Fatalf("order should still be working, status %s", got)
	}

	res := r.reservation(ctx, view.ID)
	if res.released == 0 {
		t.Fatal("no headroom released while the order is still working")
	}
	if res.consumed+res.released > res.reserved {
		t.Fatalf("reservation overdrawn: %d + %d > %d", res.consumed, res.released, res.reserved)
	}
	if bal := r.balances(ctx, taker); bal.ReservedCash >= money.Minor(res.reserved) {
		t.Fatalf("buying power did not recover: still holding %s of %d", bal.ReservedCash, res.reserved)
	}

	r.requireInvariants(ctx)
}

// A cancel request must not release the reservation. The book has not seen the request
// yet — it is still in the outbox — and an in-flight fill can still consume it. Releasing
// early is how an account overdraws against credit it was given twice.
func TestPendingCancelDoesNotReleaseTheReservation(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	taker := r.account(ctx, "taker", money.Minor(1_000_000_00))
	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "resting", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(100), LimitPrice: 14000,
	})
	if err != nil || view.Status != "ACCEPTED" {
		t.Fatalf("expected a resting order, got %s / %v", view.Status, err)
	}
	r.drain(ctx)

	before := r.balances(ctx, taker)

	status, err := r.eng.Cancel(ctx, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "PENDING_CANCEL" {
		t.Fatalf("expected PENDING_CANCEL, got %s", status)
	}

	// Deliberately NOT drained: the verdict is still in the outbox.
	if res := r.reservation(ctx, view.ID); res.released != 0 {
		t.Fatalf("reservation released on the request, before the book adjudicated: %+v", res)
	}
	if after := r.balances(ctx, taker); after.BuyingPower != before.BuyingPower {
		t.Fatalf("buying power moved on a cancel request: %s → %s",
			before.BuyingPower, after.BuyingPower)
	}

	// Only the book's verdict, arriving on the outcome stream, frees it.
	r.drain(ctx)
	if got := r.status(ctx, view.ID); got != "CANCELLED" {
		t.Fatalf("expected CANCELLED after the verdict, got %s", got)
	}
	res := r.reservation(ctx, view.ID)
	if res.consumed+res.released != res.reserved {
		t.Fatalf("reservation not fully settled after cancel: %+v", res)
	}
	if after := r.balances(ctx, taker); after.BuyingPower != after.Cash {
		t.Fatalf("buying power should be whole again: %s of %s", after.BuyingPower, after.Cash)
	}

	r.requireInvariants(ctx)
}

// The three ways a cancel and a fill can interleave, each forced deterministically.
//
// Forcing them is now a matter of WHERE the pipeline is drained, because the log is what
// decides the order the book sees things in. That is a better test than the Phase 2
// version: the thing being controlled is the actual mechanism, not a mutex standing in
// for it.
func TestCancelFillInterleavings(t *testing.T) {
	const sym = "ACME"

	setup := func(t *testing.T) (context.Context, *rig, uuid.UUID, uuid.UUID) {
		t.Helper()
		ctx, r := newRig(t)
		r.mark(sym, 15000)
		maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
		resting := r.account(ctx, "resting", money.Minor(50_000_000_00))
		if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
			t.Fatal(err)
		}
		return ctx, r, maker, resting
	}

	restBid := func(t *testing.T, ctx context.Context, r *rig, acct uuid.UUID, shares int64) uuid.UUID {
		t.Helper()
		v, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: acct, ClientOrderID: "bid", Symbol: sym,
			Side: book.Buy, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(shares), LimitPrice: 15000,
		})
		if err != nil || v.Status != "ACCEPTED" {
			t.Fatalf("resting bid not accepted: %s / %v", v.Status, err)
		}
		r.drain(ctx)
		return v.ID
	}

	cross := func(t *testing.T, ctx context.Context, r *rig, acct uuid.UUID, shares int64) {
		t.Helper()
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: acct, ClientOrderID: fmt.Sprintf("sell-%d", shares), Symbol: sym,
			Side: book.Sell, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(shares),
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("cancel reaches the book before any crossing order", func(t *testing.T) {
		ctx, r, _, resting := setup(t)
		bid := restBid(t, ctx, r, resting, 10)

		if _, err := r.eng.Cancel(ctx, bid); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)

		if got := r.status(ctx, bid); got != "CANCELLED" {
			t.Fatalf("expected CANCELLED, got %s", got)
		}
		res := r.reservation(ctx, bid)
		if res.consumed != 0 || res.consumed+res.released != res.reserved {
			t.Fatalf("a cancel before any fill should release everything: %+v", res)
		}
		r.requireInvariants(ctx)
	})

	t.Run("fill completes the order before the cancel is published", func(t *testing.T) {
		ctx, r, maker, resting := setup(t)
		bid := restBid(t, ctx, r, resting, 10)

		// Request the cancel, then let the crossing order reach the book FIRST. Both are
		// in the outbox; the crossing order was enqueued later but is drained together,
		// and the book sees the cancel first — so force the fill by draining it alone.
		cross(t, ctx, r, maker, 10)
		r.drain(ctx)

		if _, err := r.eng.Cancel(ctx, bid); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)

		if got := r.status(ctx, bid); got != "FILLED" {
			t.Fatalf("the fill won the race but the order ended %s", got)
		}
		res := r.reservation(ctx, bid)
		if res.consumed == 0 {
			t.Fatalf("a completed fill should have consumed the reservation: %+v", res)
		}
		if res.consumed+res.released != res.reserved {
			t.Fatalf("reservation not fully settled: %+v", res)
		}
		r.requireInvariants(ctx)
	})

	t.Run("partial fill lands, then the cancel takes the remainder", func(t *testing.T) {
		ctx, r, maker, resting := setup(t)
		bid := restBid(t, ctx, r, resting, 20)

		cross(t, ctx, r, maker, 8) // partial: 12 shares still resting
		r.drain(ctx)

		if got := r.status(ctx, bid); got != "PARTIALLY_FILLED" {
			t.Fatalf("expected PARTIALLY_FILLED after the partial, got %s", got)
		}

		if _, err := r.eng.Cancel(ctx, bid); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)

		if got := r.status(ctx, bid); got != "CANCELLED" {
			t.Fatalf("expected the remainder to cancel, got %s", got)
		}
		res := r.reservation(ctx, bid)
		if res.consumed == 0 || res.released == 0 {
			t.Fatalf("expected part consumed and part released: %+v", res)
		}
		if res.consumed+res.released != res.reserved {
			t.Fatalf("reservation not fully settled: %+v", res)
		}
		if bal := r.balances(ctx, resting); bal.ReservedCash != 0 {
			t.Fatalf("%s still reserved after the order terminated", bal.ReservedCash)
		}
		r.requireInvariants(ctx)
	})
}

// Under genuine concurrency the outcome is not predictable, and it does not need to be.
// What must hold either way is that the order reaches a terminal state and its reservation
// ends fully settled — never partly stranded, which would silently shrink the account's
// buying power for the rest of its life.
func TestCancelFillRaceLeavesNothingStranded(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	resting := r.account(ctx, "resting", money.Minor(50_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(100_000), 15000); err != nil {
		t.Fatal(err)
	}

	outcomes := map[string]int{}
	for round := 0; round < 40; round++ {
		bid, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: resting, ClientOrderID: fmt.Sprintf("bid-%d", round), Symbol: sym,
			Side: book.Buy, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(10), LimitPrice: 15000,
		})
		if err != nil || bid.Status != "ACCEPTED" {
			t.Fatalf("round %d: resting bid not accepted: %s / %v", round, bid.Status, err)
		}
		r.drain(ctx)

		var wg sync.WaitGroup
		wg.Add(2)
		go func(round int) {
			defer wg.Done()
			_, _ = r.eng.Submit(ctx, SubmitRequest{
				AccountID: maker, ClientOrderID: fmt.Sprintf("sell-%d", round), Symbol: sym,
				Side: book.Sell, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(10),
			})
		}(round)
		go func() {
			defer wg.Done()
			_, _ = r.eng.Cancel(ctx, bid.ID)
		}()
		wg.Wait()
		r.drain(ctx)

		status := r.status(ctx, bid.ID)
		outcomes[status]++
		if !isTerminal(status) {
			t.Fatalf("round %d: order stuck in %s", round, status)
		}
		res := r.reservation(ctx, bid.ID)
		if res.consumed+res.released != res.reserved {
			t.Fatalf("round %d (%s): reservation stranded: %+v", round, status, res)
		}
	}

	t.Logf("race outcomes: %v", outcomes)
	if bal := r.balances(ctx, resting); bal.ReservedCash != 0 {
		t.Fatalf("%s still reserved after every order terminated", bal.ReservedCash)
	}
	r.requireInvariants(ctx)
}

func TestSnapshotEncodingRoundTrips(t *testing.T) {
	original := ReplayState{
		Seq: 42, Cash: -1234, Fees: 7, Clearing: 991,
		Positions: map[string]positionState{
			"ZETA": {Qty: 5_000_000, Basis: 750_000, Realized: -12},
			"ACME": {Qty: -3, Basis: 0, Realized: 4},
			"GONE": {}, // dropped: an all-zero position carries no information
		},
	}

	decoded, err := DecodeState(original.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.Encode()) != string(original.Encode()) {
		t.Fatal("round trip is not byte-identical")
	}
	if _, ok := decoded.Positions["GONE"]; ok {
		t.Error("empty position survived encoding")
	}
	if decoded.Positions["ACME"].Realized != 4 || decoded.Positions["ZETA"].Basis != 750_000 {
		t.Errorf("values did not survive: %+v", decoded.Positions)
	}
	if _, err := DecodeState([]byte{1, 2, 3}); err == nil {
		t.Error("a truncated snapshot should not decode")
	}
}

// Recovery from a stored checkpoint plus the tail must equal a replay from genesis, byte
// for byte. Anything less means the checkpoint is lying, and every recovery after it
// inherits the lie.
func TestPersistedSnapshotPlusTailEqualsGenesisReplay(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(50_000), 15000); err != nil {
		t.Fatal(err)
	}

	trade := func(tag string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := r.eng.Submit(ctx, SubmitRequest{
				AccountID: maker, ClientOrderID: fmt.Sprintf("%s-ask-%d", tag, i), Symbol: sym,
				Side: book.Sell, Type: book.Limit, TIF: book.GTC,
				Qty: money.FromShares(25), LimitPrice: money.Minor(15000 + i),
			}); err != nil {
				t.Fatal(err)
			}
			side := book.Buy
			if i%4 == 3 {
				side = book.Sell
			}
			if _, err := r.eng.Submit(ctx, SubmitRequest{
				AccountID: taker, ClientOrderID: fmt.Sprintf("%s-t-%d", tag, i), Symbol: sym,
				Side: side, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(10),
			}); err != nil {
				var rej *Rejection
				if !asRejection(err, &rej) {
					t.Fatal(err)
				}
			}
			r.drain(ctx)
		}
	}

	trade("pre", 15)
	if _, err := r.eng.SnapshotAll(ctx); err != nil {
		t.Fatal(err)
	}
	trade("post", 10) // history accumulates past the checkpoint

	stats, err := r.eng.SnapshotStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Snapshots == 0 {
		t.Fatal("no snapshots were written")
	}
	if stats.MaxTail == 0 {
		t.Fatal("no history past the snapshot; the test would prove nothing")
	}
	t.Logf("%d snapshots, worst replay tail %d transactions", stats.Snapshots, stats.MaxTail)

	for _, acct := range []uuid.UUID{maker, taker} {
		genesis, err := r.eng.ReplayAccount(ctx, acct)
		if err != nil {
			t.Fatal(err)
		}
		recovered, err := r.eng.RecoverState(ctx, acct)
		if err != nil {
			t.Fatal(err)
		}
		if string(genesis.Encode()) != string(recovered.Encode()) {
			t.Fatalf("account %s: snapshot recovery diverges from genesis replay\ngenesis   %+v\nrecovered %+v",
				acct, genesis, recovered)
		}
		projected, err := r.eng.ProjectedState(ctx, acct)
		if err != nil {
			t.Fatal(err)
		}
		if string(projected.Encode()) != string(genesis.Encode()) {
			t.Fatalf("account %s: projection diverges from the ledger", acct)
		}
	}
	r.requireInvariants(ctx)
}

// Cost basis and realized P&L are not recorded anywhere in the ledger directly — they fall
// out of each fill's cash leg. This is the check that the ledger really is sufficient to
// rebuild everything the system will ever show a user.
func TestCostBasisIsReconstructibleFromTheLedgerAlone(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(20_000), 15000); err != nil {
		t.Fatal(err)
	}

	// Buy in at several prices, then sell most of it back, so average cost and realized
	// P&L are both non-trivial and a naive reconstruction would get them wrong.
	for i := 0; i < 10; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: maker, ClientOrderID: fmt.Sprintf("ask-%d", i), Symbol: sym,
			Side: book.Sell, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(30), LimitPrice: money.Minor(14900 + i*17),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: maker, ClientOrderID: fmt.Sprintf("bid-%d", i), Symbol: sym,
			Side: book.Buy, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(30), LimitPrice: money.Minor(14800 - i*11),
		}); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)

		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: taker, ClientOrderID: fmt.Sprintf("buy-%d", i), Symbol: sym,
			Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(20),
		}); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)

		if i%3 == 2 {
			if _, err := r.eng.Submit(ctx, SubmitRequest{
				AccountID: taker, ClientOrderID: fmt.Sprintf("sell-%d", i), Symbol: sym,
				Side: book.Sell, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(25),
			}); err != nil {
				t.Fatal(err)
			}
			r.drain(ctx)
		}
	}

	projected, err := r.eng.ProjectedState(ctx, taker)
	if err != nil {
		t.Fatal(err)
	}
	pos := projected.Positions[sym]
	if pos.Basis == 0 || pos.Realized == 0 {
		t.Fatalf("scenario left nothing interesting to check: %+v", pos)
	}

	replayed, err := r.eng.ReplayAccount(ctx, taker)
	if err != nil {
		t.Fatal(err)
	}
	if got := replayed.Positions[sym]; got != pos {
		t.Fatalf("reconstruction differs from the projection:\nledger    %+v\nprojected %+v", got, pos)
	}
	t.Logf("reconstructed qty=%s basis=%s realized=%s from postings alone",
		pos.Qty, pos.Basis, pos.Realized)
}

// A fill whose second half never settles must be caught — but only after it has had a fair
// chance to settle. Both directions matter: an alarm that fires on every in-flight fill is
// noise, and one that never fires is decoration.
func TestOrphanedHalfIsCaughtOnlyAfterTheSettleWindow(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}
	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: maker, ClientOrderID: "ask", Symbol: sym,
		Side: book.Sell, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(50), LimitPrice: 15000,
	}); err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "buy", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(20),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)
	r.requireInvariants(ctx)

	fills := r.fills(ctx, view.ID)
	if len(fills) != 1 {
		t.Fatalf("expected exactly one fill, got %d", len(fills))
	}

	// Simulate a half that never applied: drop the maker's side of the settlement.
	// Postings cascade with it, so this is exactly the state a crash between the two
	// half-fill transactions would leave behind.
	if _, err := r.pool.Exec(ctx, `
		DELETE FROM transactions WHERE fill_id = $1 AND account_id = $2`,
		fills[0].FillID, maker); err != nil {
		t.Fatal(err)
	}

	// Inside the settle window it is in flight, not a violation.
	r.eng.cfg.SettleWindow = time.Hour
	inv, err := r.eng.CheckInvariants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inv.OrphanedFillHalves != 0 {
		t.Errorf("a freshly unpaired half was reported as orphaned: %d", inv.OrphanedFillHalves)
	}
	if inv.InFlightFillHalves != 1 {
		t.Errorf("in-flight count = %d, want 1", inv.InFlightFillHalves)
	}
	if inv.UnpairedClearingUnits != 0 {
		t.Errorf("clearing should exclude half-settled fills, got %d", inv.UnpairedClearingUnits)
	}
	for sym, delta := range inv.ShareConservation {
		if delta != 0 {
			t.Errorf("share conservation for %s should exclude the half-settled fill, got %d", sym, delta)
		}
	}

	// Past the window it is stuck, and must be reported as such.
	r.eng.cfg.SettleWindow = 0
	inv, err = r.eng.CheckInvariants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inv.OrphanedFillHalves != 1 {
		t.Fatalf("a stuck half was not caught: %+v", inv)
	}
	if inv.OK() {
		t.Fatal("invariants reported OK with a stranded half-fill")
	}
}

// asRejection is errors.As specialised, so tests read without importing errors everywhere.
func asRejection(err error, target **Rejection) bool {
	for err != nil {
		if r, ok := err.(*Rejection); ok {
			*target = r
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
