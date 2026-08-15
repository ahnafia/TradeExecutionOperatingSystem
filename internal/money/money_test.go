package money

import (
	"math"
	"math/rand"
	"testing"
)

func TestNotionalKnownValues(t *testing.T) {
	cases := []struct {
		qty   Qty
		price Minor
		want  Minor
	}{
		{FromShares(100), 15000, 1_500_000}, // 100 shares @ $150.00 = $15,000.00
		{FromShares(1), 15025, 15025},
		{QtyScale / 2, 15001, 7500}, // half a share @ $150.01 → $75.005 → floor
		{QtyScale / 3, 100, 33},     // a third of a share @ $1.00 → 33.33 cents → floor
		{0, 12345, 0},
	}
	for _, c := range cases {
		got, ok := Notional(c.qty, c.price)
		if !ok || got != c.want {
			t.Errorf("Notional(%d, %d) = %d, %v; want %d", c.qty, c.price, got, ok, c.want)
		}
	}
}

// The reservation is sized with NotionalCeil and settled with Notional. If ceil ever came
// in below floor, a fill could cost more than was reserved and cash could go negative —
// so this is the arithmetic underpinning of the whole no-negative-cash claim.
func TestNotionalCeilBoundsNotional(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 200_000; i++ {
		qty := Qty(rng.Int63n(1 << 40))
		price := Minor(rng.Int63n(1 << 22))

		floor, ok1 := Notional(qty, price)
		ceil, ok2 := NotionalCeil(qty, price)
		if !ok1 || !ok2 {
			t.Fatalf("unexpected overflow at qty=%d price=%d", qty, price)
		}
		if ceil < floor {
			t.Fatalf("ceil %d < floor %d for qty=%d price=%d", ceil, floor, qty, price)
		}
		if ceil-floor > 1 {
			t.Fatalf("ceil %d and floor %d differ by more than one unit", ceil, floor)
		}
	}
}

// Splitting a quantity across several fills must never cost more than pricing it in one
// go. Floor rounding gives this for free — sum of floors <= floor of sum — and it is what
// lets a reservation sized once at accept time cover an arbitrary number of partial fills.
func TestPartialFillsNeverExceedWhole(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 20_000; i++ {
		total := Qty(rng.Int63n(1 << 32))
		price := Minor(rng.Int63n(1 << 20))

		whole, ok := Notional(total, price)
		if !ok {
			continue
		}

		var parts, sum Qty
		var partsTotal Minor
		for sum < total {
			step := Qty(rng.Int63n(int64(total-sum)) + 1)
			n, ok := Notional(step, price)
			if !ok {
				t.Fatalf("overflow splitting")
			}
			partsTotal += n
			sum += step
			parts++
		}
		if partsTotal > whole {
			t.Fatalf("split cost %d exceeds whole cost %d (%d parts)", partsTotal, whole, parts)
		}
	}
}

// Fees are billed as the delta of a rounded-up charge on cumulative notional. Charging
// each fill independently would round up once per fill and could exceed the headroom
// reserved at accept time; this checks the telescoping identity that avoids it.
func TestFeeTelescoping(t *testing.T) {
	const bps = 5
	rng := rand.New(rand.NewSource(13))

	for i := 0; i < 20_000; i++ {
		total := Minor(rng.Int63n(1 << 34))

		var cumulative, charged Minor
		for cumulative < total {
			step := Minor(rng.Int63n(int64(total-cumulative)) + 1)
			before, _ := FeeBps(cumulative, bps)
			after, _ := FeeBps(cumulative+step, bps)
			charged += after - before
			cumulative += step
		}

		want, _ := FeeBps(total, bps)
		if charged != want {
			t.Fatalf("telescoped fee %d != single charge %d on notional %d", charged, want, total)
		}
	}
}

func TestFeeRoundsUp(t *testing.T) {
	got, ok := FeeBps(1, 5) // 0.05 cents
	if !ok || got != 1 {
		t.Fatalf("FeeBps(1, 5) = %d, %v; want 1", got, ok)
	}
	if got, _ := FeeBps(0, 5); got != 0 {
		t.Fatalf("zero notional should incur no fee, got %d", got)
	}
}

func TestOverflowIsReportedNotWrapped(t *testing.T) {
	if _, ok := Notional(Qty(math.MaxInt64), Minor(math.MaxInt64)); ok {
		t.Fatal("expected overflow to be reported")
	}
	if _, ok := NotionalCeil(Qty(math.MaxInt64), Minor(math.MaxInt64)); ok {
		t.Fatal("expected overflow to be reported")
	}
	if _, ok := Add(Minor(math.MaxInt64), 1); ok {
		t.Fatal("expected Add overflow to be reported")
	}
}

func TestCollarBounds(t *testing.T) {
	up, ok := ApplyBps(15000, 500) // $150.00 + 5%
	if !ok || up != 15750 {
		t.Fatalf("ApplyBps = %d, %v; want 15750", up, ok)
	}
	down, ok := SubBps(15000, 500)
	if !ok || down != 14250 {
		t.Fatalf("SubBps = %d, %v; want 14250", down, ok)
	}
}
