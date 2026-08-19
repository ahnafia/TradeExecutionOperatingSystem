// Package money implements the arithmetic of the financial path.
//
// Two scalar types, both int64, no floats anywhere:
//
//	Minor — money in minor units (cents).
//	Qty   — quantity in units of 1e-6 shares, so 1 share is 1_000_000.
//
// Prices are Minor per WHOLE share, which is how a human quotes them. Converting a
// (Qty, price) pair into a Minor notional is therefore a scaled multiply, and it is the
// one place in this system where an overflow or a rounding slip turns into lost money.
// Every conversion here is exact 128-bit arithmetic with an explicit overflow result.
package money

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

// QtyScale is the number of Qty units in one whole share.
const QtyScale = 1_000_000

type (
	// Minor is an amount of money in minor units (cents). Signed: ledger postings are
	// signed, and a negative balance is representable so that it can be detected rather
	// than silently wrapped.
	Minor int64

	// Qty is a quantity in units of 1e-6 shares. Non-negative in orders and fills;
	// signed in positions and postings, where a share leg can be negative.
	Qty int64
)

// Shares returns q as a whole-share count, truncating any fraction. For display only.
func (q Qty) Shares() int64 { return int64(q) / QtyScale }

// String renders a quantity in shares, dropping trailing zeros of the fraction.
func (q Qty) String() string {
	neg := q < 0
	if neg {
		q = -q
	}
	s := strconv.FormatInt(int64(q)/QtyScale, 10)
	if frac := int64(q) % QtyScale; frac != 0 {
		s += strings.TrimRight(fmt.Sprintf(".%06d", frac), "0")
	}
	if neg {
		return "-" + s
	}
	return s
}

// String renders an amount as dollars and cents.
func (m Minor) String() string {
	neg := m < 0
	if neg {
		m = -m
	}
	s := fmt.Sprintf("$%d.%02d", int64(m)/100, int64(m)%100)
	if neg {
		return "-" + s
	}
	return s
}

// FromShares converts a whole-share count into Qty units.
func FromShares(n int64) Qty { return Qty(n * QtyScale) }

// Notional returns qty*price/QtyScale rounded toward zero, and whether the result fits
// in an int64.
//
// Both sides of a fill use this single value. Rounding the buyer's debit up and the
// seller's credit down would leave a residual with nowhere to go in a double-entry
// system — the transaction would not balance. Instead there is one notional, rounded
// once, and the reservation (see NotionalCeil) is sized to be an upper bound on it.
func Notional(qty Qty, price Minor) (Minor, bool) {
	n, _, ok := notional(qty, price)
	return n, ok
}

// NotionalCeil returns qty*price/QtyScale rounded away from zero. Used only to size
// reservations, never to settle a fill: it must be an upper bound on Notional so that
// realized cost can never exceed the amount reserved.
func NotionalCeil(qty Qty, price Minor) (Minor, bool) {
	n, rem, ok := notional(qty, price)
	if !ok {
		return 0, false
	}
	if rem != 0 {
		if n == maxMinor {
			return 0, false
		}
		if qty >= 0 == (price >= 0) {
			n++
		} else {
			n--
		}
	}
	return n, true
}

const maxMinor = Minor(1<<63 - 1)

// notional does the exact scaled multiply, returning the truncated quotient, the
// remainder (for the caller to round with), and whether it fit.
func notional(qty Qty, price Minor) (Minor, uint64, bool) {
	neg := false
	a, b := int64(qty), int64(price)
	if a < 0 {
		neg, a = !neg, -a
	}
	if b < 0 {
		neg, b = !neg, -b
	}
	if a < 0 || b < 0 { // -math.MinInt64 is still negative
		return 0, 0, false
	}

	hi, lo := bits.Mul64(uint64(a), uint64(b))
	if hi >= QtyScale { // quotient would exceed 64 bits; bits.Div64 would panic
		return 0, 0, false
	}
	quo, rem := bits.Div64(hi, lo, QtyScale)
	if quo > uint64(maxMinor) {
		return 0, 0, false
	}

	out := Minor(quo)
	if neg {
		out = -out
	}
	return out, rem, true
}

// FeeBps returns bps basis points of notional, rounded away from zero.
//
// Rounding up is deliberate: the fee is charged to the account and covered by reserved
// headroom, so rounding it up can never overdraw a reservation, whereas rounding down
// would let a long tail of sub-cent fees leak value out of the fee account.
func FeeBps(notionalAmt Minor, bps int64) (Minor, bool) {
	if bps == 0 {
		return 0, true
	}
	neg := false
	n := int64(notionalAmt)
	if n < 0 {
		neg, n = true, -n
	}
	if n < 0 || bps < 0 {
		return 0, false
	}

	hi, lo := bits.Mul64(uint64(n), uint64(bps))
	if hi >= 10_000 {
		return 0, false
	}
	quo, rem := bits.Div64(hi, lo, 10_000)
	if rem != 0 {
		quo++
	}
	if quo > uint64(maxMinor) {
		return 0, false
	}

	out := Minor(quo)
	if neg {
		out = -out
	}
	return out, true
}

// ApplyBps returns amount scaled by (1 + bps/10_000), rounded away from zero. Used to
// widen a reference price into a collar bound.
func ApplyBps(amount Minor, bps int64) (Minor, bool) {
	delta, ok := FeeBps(amount, bps)
	if !ok {
		return 0, false
	}
	sum := amount + delta
	if (delta > 0 && sum < amount) || (delta < 0 && sum > amount) {
		return 0, false
	}
	return sum, true
}

// SubBps returns amount scaled by (1 - bps/10_000), rounded toward zero.
func SubBps(amount Minor, bps int64) (Minor, bool) {
	delta, ok := FeeBps(amount, bps)
	if !ok {
		return 0, false
	}
	return amount - delta, true
}

// Add reports the sum and whether it overflowed.
func Add(a, b Minor) (Minor, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
}

// ParseMinor reads a decimal amount ("150.25", "$1,900") into minor units.
//
// It never touches a float. A float parse of "150.25" is already wrong before any
// arithmetic happens, and in a ledger that error compounds silently — so the decimal
// point is handled by splitting the string, not by binary floating point.
func ParseMinor(s string) (Minor, error) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.ReplaceAll(s, ",", ""), "$"))
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" {
		whole = "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad amount %q", s)
	}
	if len(frac) > 2 {
		return 0, fmt.Errorf("amount %q has more than two decimal places", s)
	}
	frac = (frac + "00")[:2]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad amount %q", s)
	}
	out := Minor(w*100 + f)
	if neg {
		out = -out
	}
	return out, nil
}

// ParseQty reads a share count into 1e-6 share units.
func ParseQty(s string) (Qty, error) {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" {
		whole = "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad quantity %q", s)
	}
	if len(frac) > 6 {
		return 0, fmt.Errorf("quantity %q is finer than 1e-6 shares", s)
	}
	frac = (frac + "000000")[:6]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad quantity %q", s)
	}
	out := Qty(w*QtyScale + f)
	if neg {
		out = -out
	}
	return out, nil
}
