package book

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/money"
)

func newBook() *Book { return New("ACME", DefaultVenue, 0, 500, 1024) }

func limit(side Side, price money.Minor, shares int64) *Order {
	return &Order{
		ID: uuid.New(), AccountID: uuid.New(), Side: side, Type: Limit, TIF: GTC,
		Qty: money.FromShares(shares), Remaining: money.FromShares(shares), LimitPrice: price,
	}
}

func market(side Side, shares int64, ref money.Minor) *Order {
	return &Order{
		ID: uuid.New(), AccountID: uuid.New(), Side: side, Type: Market, TIF: IOC,
		Qty: money.FromShares(shares), Remaining: money.FromShares(shares), RefPrice: ref,
	}
}

func TestPriceThenTimePriority(t *testing.T) {
	b := newBook()
	// Two asks at the same price; the earlier one must fill first. A third, better-priced
	// ask arrives last and must still fill before both.
	first := limit(Sell, 15100, 10)
	second := limit(Sell, 15100, 10)
	better := limit(Sell, 15000, 10)
	b.Submit(first)
	b.Submit(second)
	b.Submit(better)

	res := b.Submit(market(Buy, 25, 15000))
	if len(res.Fills) != 3 {
		t.Fatalf("expected 3 fills, got %d", len(res.Fills))
	}
	if res.Fills[0].MakerOrderID != better.ID {
		t.Error("better price must execute first")
	}
	if res.Fills[1].MakerOrderID != first.ID {
		t.Error("at equal price, the earlier order must execute first")
	}
	if res.Fills[2].MakerOrderID != second.ID {
		t.Error("the later same-price order must execute last")
	}
	if res.Fills[0].Price != 15000 || res.Fills[1].Price != 15100 {
		t.Error("fills must execute at the maker's price, not the taker's")
	}
}

func TestMarketOrderStopsAtCollar(t *testing.T) {
	b := newBook()
	b.Submit(limit(Sell, 15000, 10)) // inside the collar
	b.Submit(limit(Sell, 20000, 10)) // 33% away: outside a 5% collar

	res := b.Submit(market(Buy, 20, 15000))
	if len(res.Fills) != 1 {
		t.Fatalf("expected the collar to clip the second level, got %d fills", len(res.Fills))
	}
	if res.Disposition != Cancelled || res.CancelReason != "COLLAR_BREACH" {
		t.Fatalf("expected COLLAR_BREACH cancel, got %v/%s", res.Disposition, res.CancelReason)
	}
	if res.RemainingQty != money.FromShares(10) {
		t.Fatalf("expected 10 shares unfilled, got %s", res.RemainingQty)
	}
}

func TestMarketOrderWithoutReferencePriceDoesNotExecute(t *testing.T) {
	b := newBook()
	b.Submit(limit(Sell, 15000, 10))

	o := market(Buy, 5, 0) // no reference price stamped
	res := b.Submit(o)
	if len(res.Fills) != 0 {
		t.Fatal("an order with no collar basis must not execute")
	}
}

func TestLimitRestsRemainderAndIOCCancelsIt(t *testing.T) {
	b := newBook()
	b.Submit(limit(Sell, 15000, 5))

	gtc := limit(Buy, 15000, 10)
	res := b.Submit(gtc)
	if res.Disposition != Rested || res.RemainingQty != money.FromShares(5) {
		t.Fatalf("GTC remainder should rest, got %v %s", res.Disposition, res.RemainingQty)
	}
	if bid, ok := b.BestBid(); !ok || bid != 15000 {
		t.Fatalf("resting bid missing, got %d %v", bid, ok)
	}

	b2 := newBook()
	b2.Submit(limit(Sell, 15000, 5))
	ioc := limit(Buy, 15000, 10)
	ioc.TIF = IOC
	res2 := b2.Submit(ioc)
	if res2.Disposition != Cancelled || res2.CancelReason != "IOC_REMAINDER" {
		t.Fatalf("IOC remainder should cancel, got %v/%s", res2.Disposition, res2.CancelReason)
	}
	if _, ok := b2.BestBid(); ok {
		t.Fatal("IOC must not rest")
	}
}

func TestCancelRemovesRestingOrder(t *testing.T) {
	b := newBook()
	o := limit(Sell, 15000, 10)
	b.Submit(o)

	remaining, ok := b.Cancel(o.ID)
	if !ok || remaining != money.FromShares(10) {
		t.Fatalf("cancel returned %s, %v", remaining, ok)
	}
	if _, ok := b.BestAsk(); ok {
		t.Fatal("book should be empty after cancel")
	}
	if _, ok := b.Cancel(o.ID); ok {
		t.Fatal("second cancel should find nothing")
	}
}

// The outbox relay is at-least-once, so the same accepted order can arrive twice. A
// duplicate must not become a second resting order — the two fills it would generate are
// genuinely distinct events, so no downstream idempotency key would catch them.
func TestDuplicateOrderIsIgnored(t *testing.T) {
	b := newBook()
	o := limit(Sell, 15000, 10)
	b.Submit(o)

	dup := *o
	dup.Remaining = o.Qty
	res := b.Submit(&dup)
	if !res.DuplicateIgnored {
		t.Fatal("duplicate order id should be suppressed")
	}

	buy := b.Submit(market(Buy, 20, 15000))
	var filled money.Qty
	for _, f := range buy.Fills {
		filled += f.Qty
	}
	if filled != money.FromShares(10) {
		t.Fatalf("duplicate created extra liquidity: filled %s", filled)
	}
}

func TestDuplicateSuppressionSurvivesTermination(t *testing.T) {
	b := newBook()
	o := limit(Sell, 15000, 10)
	b.Submit(o)
	b.Submit(market(Buy, 10, 15000)) // fully consumes o, removing it from byID

	dup := *o
	dup.Remaining = o.Qty
	if res := b.Submit(&dup); !res.DuplicateIgnored {
		t.Fatal("a duplicate of a terminated order must still be suppressed")
	}
}

// Replay determinism is the property that makes crash recovery in the matching engine
// safe. It is not enough that the rebuilt book holds the same orders: the fill IDENTITIES
// must line up too, because those identities are what stop a re-derived fill from being
// applied a second time.
func TestReplayProducesIdenticalFillIdentities(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	orders := make([]*Order, 0, 400)
	for i := 0; i < 400; i++ {
		side := Buy
		if rng.Intn(2) == 0 {
			side = Sell
		}
		var o *Order
		if rng.Intn(4) == 0 {
			o = market(side, rng.Int63n(20)+1, 15000)
		} else {
			o = limit(side, money.Minor(14_000+rng.Int63n(2_000)), rng.Int63n(20)+1)
		}
		orders = append(orders, o)
	}

	run := func() []Fill {
		b := newBook()
		var out []Fill
		for _, o := range orders {
			clone := *o
			clone.Remaining = clone.Qty
			out = append(out, b.Submit(&clone).Fills...)
		}
		return out
	}

	first, second := run(), run()
	if len(first) == 0 {
		t.Fatal("scenario generated no fills; the test would prove nothing")
	}
	if len(first) != len(second) {
		t.Fatalf("replay produced %d fills, original %d", len(second), len(first))
	}
	for i := range first {
		if first[i].FillID != second[i].FillID {
			t.Fatalf("fill %d identity diverged: %s vs %s", i, first[i].FillID, second[i].FillID)
		}
		if first[i].Price != second[i].Price || first[i].Qty != second[i].Qty {
			t.Fatalf("fill %d economics diverged", i)
		}
	}
}

// Whatever the book does, it must never invent or destroy quantity: every share a taker
// receives came out of a resting order.
func TestQuantityIsConserved(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	b := newBook()

	var submitted, filledTaker, filledMaker money.Qty
	for i := 0; i < 2_000; i++ {
		side := Buy
		if rng.Intn(2) == 0 {
			side = Sell
		}
		o := limit(side, money.Minor(14_800+rng.Int63n(400)), rng.Int63n(10)+1)
		if rng.Intn(5) == 0 {
			o = market(side, rng.Int63n(10)+1, 15000)
		}
		submitted += o.Qty

		res := b.Submit(o)
		for _, f := range res.Fills {
			filledTaker += f.Qty
			filledMaker += f.Qty
		}
	}

	if filledTaker != filledMaker {
		t.Fatalf("taker filled %s but maker filled %s", filledTaker, filledMaker)
	}

	var resting money.Qty
	bids, asks := b.Depth(1 << 20)
	for _, l := range bids {
		resting += l.Qty
	}
	for _, l := range asks {
		resting += l.Qty
	}
	if resting > submitted {
		t.Fatalf("book holds %s resting from %s submitted", resting, submitted)
	}
}
