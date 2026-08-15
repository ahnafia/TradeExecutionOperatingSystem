package engine

import "github.com/ahnafia/trading-system/internal/money"

// positionState is the average-cost accounting for one symbol in one account.
//
// It exists as a standalone value with no database in it for one reason: the write path
// and the replay path must produce identical numbers, and the only durable way to
// guarantee that is for both to call the same code. A second implementation inside the
// replayer would agree with this one right up until someone changed one of them.
type positionState struct {
	Qty      money.Qty
	Basis    money.Minor
	Realized money.Minor
}

// applyBuy adds shares at a cost. `cost` is the full cash outlay including fees, which is
// exactly the negation of the transaction's cash leg — so a replayer can recover it from
// the ledger without needing the fill's price or size recorded separately.
func (p *positionState) applyBuy(qty money.Qty, cost money.Minor) {
	p.Qty += qty
	p.Basis += cost
}

// applySell removes shares and realizes P&L against average cost. `proceeds` is the net
// cash received after fees, which is the transaction's cash leg.
//
// A full close takes the entire remaining basis rather than a rounded proportion. Without
// that case, integer division leaves a few cents of basis stranded on a position with
// zero quantity — invisible per trade, permanent, and it makes realized P&L wrong by a
// growing amount for any account that trades in and out repeatedly.
func (p *positionState) applySell(qty money.Qty, proceeds money.Minor) {
	var costOut money.Minor
	switch {
	case p.Qty <= 0:
		costOut = 0
	case qty >= p.Qty:
		costOut = p.Basis
	default:
		costOut = money.Minor(int64(p.Basis) * int64(qty) / int64(p.Qty))
	}
	p.Realized += proceeds - costOut
	p.Qty -= qty
	p.Basis -= costOut
}

// apply routes a signed share delta and its cash leg to the right side.
func (p *positionState) apply(deltaQty money.Qty, cashLeg money.Minor) {
	if deltaQty >= 0 {
		p.applyBuy(deltaQty, -cashLeg)
		return
	}
	p.applySell(-deltaQty, cashLeg)
}
