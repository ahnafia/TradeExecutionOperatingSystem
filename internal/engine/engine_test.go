package engine

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/money"
)

func TestFillSettlesBothHalves(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(1_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(100_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(1000), 15000); err != nil {
		t.Fatal(err)
	}
	makerStart := r.balances(ctx, maker)

	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: maker, ClientOrderID: "ask-1", Symbol: sym,
		Side: book.Sell, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(100), LimitPrice: 15000,
	}); err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "buy-1", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(40),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Accept is durable and immediate; matching happens behind the log.
	if view.Status != "ACCEPTED" {
		t.Fatalf("expected ACCEPTED on submit, got %s", view.Status)
	}
	r.drain(ctx)

	if got := r.status(ctx, view.ID); got != "FILLED" {
		t.Fatalf("order settled as %s, want FILLED", got)
	}
	fills := r.fills(ctx, view.ID)
	if len(fills) != 1 {
		t.Fatalf("expected one fill, got %d", len(fills))
	}

	// $150.00 x 40 shares = $6,000.00, plus 5bps taker fee = $3.00
	if got, want := r.balances(ctx, taker).Cash, money.Minor(100_000_00-6_000_00-300); got != want {
		t.Errorf("taker cash = %s, want %s", got, want)
	}
	if got, want := r.balances(ctx, maker).Cash, makerStart.Cash+6_000_00; got != want {
		t.Errorf("maker cash = %s, want %s", got, want)
	}

	// Both halves must have settled: one transaction per side, sharing a fill_id.
	if n := r.countRows(ctx,
		`SELECT count(*) FROM transactions WHERE fill_id = $1`, fills[0].FillID); n != 2 {
		t.Fatalf("fill settled %d halves, want 2", n)
	}

	r.requireInvariants(ctx)
}

func TestClientOrderIDIsIdempotent(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)
	acct := r.account(ctx, "acct", money.Minor(100_000_00))

	req := SubmitRequest{
		AccountID: acct, ClientOrderID: "same", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(10), LimitPrice: 14000,
	}
	first, err := r.eng.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.eng.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("retry created a second order: %s vs %s", first.ID, second.ID)
	}
	if n := r.countRows(ctx, `SELECT count(*) FROM orders WHERE account_id = $1`, acct); n != 1 {
		t.Fatalf("expected 1 order, found %d", n)
	}
	// And crucially, only ONE publish was enqueued. A retry that enqueued a second would
	// put the same order into the book twice.
	if n := r.countRows(ctx, `SELECT count(*) FROM outbox`); n != 1 {
		t.Fatalf("expected 1 outbox row, found %d", n)
	}
}

func TestMarketOrderNeedsAReferencePrice(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	acct := r.account(ctx, "acct", money.Minor(100_000_00))

	// No tick published: a market order has no basis on which to size a reservation.
	_, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "m1", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(10),
	})
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Reason != "NO_REFERENCE_PRICE" {
		t.Fatalf("expected NO_REFERENCE_PRICE, got %v", err)
	}

	// A limit order is its own collar and is still accepted — the degradation story.
	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "l1", Symbol: sym,
		Side: book.Buy, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(10), LimitPrice: 14000,
	})
	if err != nil || view.Status != "ACCEPTED" {
		t.Fatalf("limit order should survive a market data outage: %s, %v", view.Status, err)
	}

	// A stale tick is not a price either.
	r.md.Publish(marketdata.Tick{Symbol: sym, Price: 15000, At: time.Now().Add(-time.Hour)})
	_, err = r.eng.Submit(ctx, SubmitRequest{
		AccountID: acct, ClientOrderID: "m2", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(10),
	})
	if !errors.As(err, &rej) || rej.Reason != "NO_REFERENCE_PRICE" {
		t.Fatalf("stale tick should be refused, got %v", err)
	}
}

// Concurrent buys against one account are the classic double-spend: each reads the same
// balance, each passes risk, both execute. The account row lock is what makes it
// impossible, and this is the test that would catch its removal.
func TestConcurrentBuysCannotOverdraw(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(100_000), 15000); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: maker, ClientOrderID: fmt.Sprintf("ask-%d", i), Symbol: sym,
			Side: book.Sell, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(100), LimitPrice: money.Minor(15000 + i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	r.drain(ctx)

	// Enough for roughly six 10-share buys, with 30 attempted at once.
	taker := r.account(ctx, "taker", money.Minor(9_000_00))

	var wg sync.WaitGroup
	accepted := make([]bool, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := r.eng.Submit(ctx, SubmitRequest{
				AccountID: taker, ClientOrderID: fmt.Sprintf("buy-%d", i), Symbol: sym,
				Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(10),
			})
			accepted[i] = err == nil && v.Status != "REJECTED"
		}(i)
	}
	wg.Wait()
	r.drain(ctx)

	if cash := r.balances(ctx, taker).Cash; cash < 0 {
		t.Fatalf("cash went negative: %s", cash)
	}
	n := 0
	for _, ok := range accepted {
		if ok {
			n++
		}
	}
	if n == 30 {
		t.Fatal("every order was accepted; the risk check is not binding")
	}
	t.Logf("%d/30 concurrent orders accepted, ending cash %s", n, r.balances(ctx, taker).Cash)
	r.requireInvariants(ctx)
}

// The headline property: whatever sequence of activity occurs, money and shares are
// conserved, no reservation is overdrawn, and every projection still agrees with the
// ledger that justifies it.
func TestRandomActivityConservesMoneyAndShares(t *testing.T) {
	ctx, r := newRig(t)
	symbols := []string{"ACME", "BETA"}
	refs := map[string]money.Minor{"ACME": 15000, "BETA": 4200}
	for s, p := range refs {
		r.mark(s, p)
	}

	maker := r.account(ctx, "maker", money.Minor(200_000_000_00))
	for _, s := range symbols {
		if err := r.eng.SeedShares(ctx, maker, s, money.FromShares(200_000), refs[s]); err != nil {
			t.Fatal(err)
		}
	}
	takers := make([]uuid.UUID, 4)
	for i := range takers {
		takers[i] = r.account(ctx, fmt.Sprintf("taker%d", i), money.Minor(500_000_00))
	}

	rng := rand.New(rand.NewSource(2024))
	for round := 0; round < 40; round++ {
		sym := symbols[rng.Intn(len(symbols))]
		ref := refs[sym]

		// The maker refreshes a two-sided quote.
		for lvl := 0; lvl < 3; lvl++ {
			off := money.Minor(10 * (lvl + 1))
			for _, q := range []struct {
				side  book.Side
				price money.Minor
			}{{book.Sell, ref + off}, {book.Buy, ref - off}} {
				if _, err := r.eng.Submit(ctx, SubmitRequest{
					AccountID: maker, Symbol: sym, Side: q.side,
					ClientOrderID: fmt.Sprintf("mm-%s-%d-%d-%v", sym, round, lvl, q.side),
					Type:          book.Limit, TIF: book.GTC,
					Qty: money.FromShares(int64(20 + rng.Intn(60))), LimitPrice: q.price,
				}); err != nil {
					t.Fatalf("maker quote: %v", err)
				}
			}
		}
		r.drain(ctx)

		// Takers cross, or post their own resting interest.
		for i, taker := range takers {
			side := book.Buy
			if rng.Intn(2) == 0 {
				side = book.Sell
			}
			req := SubmitRequest{
				AccountID: taker, Symbol: sym, Side: side,
				ClientOrderID: fmt.Sprintf("t%d-%s-%d", i, sym, round),
				Qty:           money.FromShares(int64(1 + rng.Intn(40))),
			}
			if rng.Intn(3) == 0 {
				req.Type, req.TIF = book.Limit, book.GTC
				req.LimitPrice = ref + money.Minor(rng.Intn(80)-40)
			} else {
				req.Type, req.TIF = book.Market, book.IOC
			}
			// Rejections are expected and are part of what is being tested.
			if _, err := r.eng.Submit(ctx, req); err != nil {
				var rej *Rejection
				if !errors.As(err, &rej) {
					t.Fatalf("round %d: %v", round, err)
				}
			}
		}
		r.drain(ctx)

		// Move the market so the collar and the reservation bound get exercised.
		refs[sym] = ref + money.Minor(rng.Intn(60)-30)
		if refs[sym] < 100 {
			refs[sym] = 100
		}
		r.mark(sym, refs[sym])
	}

	if fills := r.countRows(ctx, `SELECT count(*) FROM fills`); fills < 50 {
		t.Fatalf("only %d fills generated; the scenario is not exercising the engine", fills)
	} else {
		t.Logf("%d fills generated", fills)
	}

	r.requireInvariants(ctx)

	// And every account's projected state must equal a replay of its ledger.
	accounts, err := r.eng.Accounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range accounts {
		replayed, err := r.eng.ReplayAccount(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		projected, err := r.eng.ProjectedState(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		if string(replayed.Encode()) != string(projected.Encode()) {
			t.Fatalf("account %s diverges from its ledger:\nreplay    %+v\nprojected %+v",
				a, replayed, projected)
		}
	}
}

// A market order must not execute outside its collar even when the book offers liquidity
// there, because the reservation taken at accept time was sized on that assumption.
func TestCollarBoundsRealizedCost(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	maker := r.account(ctx, "maker", money.Minor(10_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(1_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}

	// A thin book: a little liquidity at the touch, then a cliff far above the collar.
	for _, q := range []struct {
		price money.Minor
		qty   int64
		id    string
	}{{15100, 10, "near"}, {30000, 500, "far"}} {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: maker, ClientOrderID: q.id, Symbol: sym,
			Side: book.Sell, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(q.qty), LimitPrice: q.price,
		}); err != nil {
			t.Fatal(err)
		}
	}
	r.drain(ctx)

	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "sweep", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(200),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	settled, err := r.eng.Order(ctx, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.FilledQty != money.FromShares(10) {
		t.Fatalf("collar should have clipped the sweep at 10 shares, filled %s", settled.FilledQty)
	}
	if settled.Status != "CANCELLED" {
		t.Fatalf("clipped remainder should cancel, status %s", settled.Status)
	}
	if settled.RejectReason != "COLLAR_BREACH" {
		t.Errorf("cancel reason = %q, want COLLAR_BREACH", settled.RejectReason)
	}

	r.requireInvariants(ctx)
}
