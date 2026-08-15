package engine

import (
	"fmt"
	"testing"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/matching"
	"github.com/ahnafia/trading-system/internal/money"
)

// A taker must get the best price available anywhere, not the best price at whichever
// venue happened to be asked first.
//
// The setup is deliberately adversarial to a naive router: the cheap liquidity is at the
// venue that sorts LAST alphabetically and has the WORSE latency, so anything that routes
// by name order, map order, or latency alone gets a worse fill and this test catches it.
func TestSmartOrderRouterTakesTheBestPriceAcrossVenues(t *testing.T) {
	ctx, r := newRigWithVenues(t, []matching.Venue{
		{Name: "ALPHA", LatencyMicro: 100},
		{Name: "ZULU", LatencyMicro: 900},
	})
	const sym = "ACME"
	r.mark(sym, 15000)
	r.match.EnsureBooks(sym)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}

	// Liquidity reaches each venue the way it really would: a maker posts there.
	r.postAt(ctx, maker, sym, "ALPHA", 15100, 20) // expensive, fast, sorts first
	r.postAt(ctx, maker, sym, "ZULU", 15000, 20)  // cheap, slow, sorts last

	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "sweep", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(20),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	fills := r.fills(ctx, view.ID)
	if len(fills) == 0 {
		t.Fatal("no fills; the router took nothing")
	}
	for _, f := range fills {
		if f.Price != 15000 {
			t.Errorf("router paid %s when %s was available elsewhere", f.Price, money.Minor(15000))
		}
	}
	settled, err := r.eng.Order(ctx, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.FilledQty != money.FromShares(20) {
		t.Fatalf("expected the cheap venue to fill it entirely, got %s", settled.FilledQty)
	}
	r.requireInvariants(ctx)
}

// When one venue cannot fill the whole order, the router sweeps into the next best.
func TestSmartOrderRouterSweepsAcrossVenuesInPriceOrder(t *testing.T) {
	ctx, r := newRigWithVenues(t, []matching.Venue{
		{Name: "ALPHA", LatencyMicro: 100},
		{Name: "ZULU", LatencyMicro: 900},
	})
	const sym = "ACME"
	r.mark(sym, 15000)
	r.match.EnsureBooks(sym)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}

	r.postAt(ctx, maker, sym, "ZULU", 15000, 10)  // cheapest, but only 10
	r.postAt(ctx, maker, sym, "ALPHA", 15050, 10) // next best
	r.postAt(ctx, maker, sym, "ZULU", 15200, 10)  // most expensive

	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "sweep", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(25),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	fills := r.fills(ctx, view.ID)
	var total money.Qty
	prices := map[money.Minor]money.Qty{}
	for _, f := range fills {
		total += f.Qty
		prices[f.Price] += f.Qty
	}
	if total != money.FromShares(25) {
		t.Fatalf("swept %s of 25 shares", total)
	}
	// Cheapest first, then the next: 10 @ 150.00, 10 @ 150.50, 5 @ 152.00.
	for price, want := range map[money.Minor]money.Qty{
		15000: money.FromShares(10),
		15050: money.FromShares(10),
		15200: money.FromShares(5),
	} {
		if prices[price] != want {
			t.Errorf("at %s took %s, want %s", price, prices[price], want)
		}
	}
	r.requireInvariants(ctx)
}

// Fill identities must stay unique once a symbol trades in more than one book. Each venue
// keeps its own book_seq, so an identity that ignored the venue would collide across
// venues — and the ledger would discard a real execution as a duplicate.
func TestFillIdentitiesAreUniqueAcrossVenues(t *testing.T) {
	ctx, r := newRigWithVenues(t, []matching.Venue{
		{Name: "ALPHA"}, {Name: "ZULU"},
	})
	const sym = "ACME"
	r.mark(sym, 15000)
	r.match.EnsureBooks(sym)

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(10_000), 15000); err != nil {
		t.Fatal(err)
	}

	for _, venue := range []string{"ALPHA", "ZULU"} {
		for i := 0; i < 3; i++ {
			r.postAt(ctx, maker, sym, venue, 15000, 10)
		}
	}

	for i := 0; i < 4; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: taker, ClientOrderID: fmt.Sprintf("buy-%d", i), Symbol: sym,
			Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(12),
		}); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)
	}

	// Every fill row is a distinct execution; a collision would have been silently
	// swallowed by the ON CONFLICT and shown up as a missing fill.
	fills := r.countRows(ctx, `SELECT count(*) FROM fills`)
	halves := r.countRows(ctx, `SELECT count(*) FROM transactions WHERE fill_id IS NOT NULL`)
	if fills == 0 {
		t.Fatal("no fills generated")
	}
	if halves != fills*2 {
		t.Fatalf("%d fills settled %d halves; identities collided across venues", fills, halves)
	}

	var venues int64
	if err := r.pool.QueryRow(ctx,
		`SELECT count(DISTINCT shard_id) FROM fills`).Scan(&venues); err != nil {
		t.Fatal(err)
	}
	r.requireInvariants(ctx)
}
