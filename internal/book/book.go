// Package book implements a price-time-priority limit order book.
//
// The book is deliberately pure: its only inputs are the orders handed to it, and it
// calls no clock, no random source, and nothing over a network. Replaying the same
// orders in the same sequence rebuilds an identical book and emits an identical sequence
// of fill identities. That property is what makes crash recovery in the matching engine
// safe, so it is a test (see book_replay_test.go), not an aspiration.
//
// A consequence worth stating: the collar bound for a market order comes from RefPrice,
// stamped on the order when the trading core accepted it. The book never asks market
// data for a price. If it did, replay would depend on what the simulator happened to be
// doing at replay time, and determinism would be gone.
package book

import (
	"sort"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/ids"
	"github.com/ahnafia/trading-system/internal/money"
)

type Side uint8

const (
	Buy Side = iota
	Sell
)

func (s Side) String() string {
	if s == Buy {
		return "BUY"
	}
	return "SELL"
}

func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

type Type uint8

const (
	Market Type = iota
	Limit
)

func (t Type) String() string {
	if t == Market {
		return "MARKET"
	}
	return "LIMIT"
}

type TIF uint8

const (
	IOC TIF = iota
	GTC
)

func (t TIF) String() string {
	if t == IOC {
		return "IOC"
	}
	return "GTC"
}

// Order is the book's view of an order. The trading core owns the authoritative record;
// this is the subset the book needs to match.
type Order struct {
	ID         uuid.UUID
	AccountID  uuid.UUID
	Side       Side
	Type       Type
	TIF        TIF
	Qty        money.Qty // original size
	Remaining  money.Qty
	LimitPrice money.Minor // meaningful iff Type == Limit
	RefPrice   money.Minor // collar basis, stamped at accept; meaningful iff Type == Market
	Seq        uint64      // arrival order, assigned by the book; breaks price ties
}

// Fill is one execution between a taker and a resting maker.
type Fill struct {
	FillID       uuid.UUID
	ShardID      int
	Symbol       string
	Venue        string
	BookSeq      uint64
	Price        money.Minor
	Qty          money.Qty
	TakerOrderID uuid.UUID
	TakerAccount uuid.UUID
	TakerSide    Side
	MakerOrderID uuid.UUID
	MakerAccount uuid.UUID
}

// Disposition is what happened to the incoming order's unfilled remainder.
type Disposition uint8

const (
	Rested    Disposition = iota // remainder is resting in the book
	Cancelled                    // remainder was cancelled (IOC, market, or collar breach)
	Complete                     // fully filled, nothing left over
)

func (d Disposition) String() string {
	switch d {
	case Rested:
		return "RESTED"
	case Cancelled:
		return "CANCELLED"
	default:
		return "COMPLETE"
	}
}

// Result is the outcome of submitting one order.
type Result struct {
	Fills            []Fill
	Disposition      Disposition
	RemainingQty     money.Qty
	CancelReason     string
	DuplicateIgnored bool // order_id was already seen; nothing was done
}

type level struct {
	price  money.Minor
	orders []*Order // FIFO: index 0 is the oldest, and therefore first in priority
}

// Book holds one symbol's resting orders. A Book is not safe for concurrent use; the
// engine gives each symbol a single writer, which is the whole point of sharding by
// symbol.
type Book struct {
	Symbol    string
	Venue     string
	ShardID   int
	CollarBps int64

	bids []*level // sorted by price DESCENDING: best bid first
	asks []*level // sorted by price ASCENDING:  best ask first

	byID    map[uuid.UUID]*Order
	arrival uint64 // monotonic arrival counter, the time in "price-time"
	bookSeq uint64 // monotonic fill counter; the basis of every fill identity

	// Bounded dedup for order ids the book has already accepted. The outbox relay is
	// at-least-once, so a republished orders.accepted must not insert a second resting
	// order. Live orders are covered by byID; this ring covers ones that have already
	// terminated. It must be larger than the maximum relay lag, which is monitored.
	dedupRing  []uuid.UUID
	dedupSet   map[uuid.UUID]struct{}
	dedupNext  int
	dedupLimit int
}

// DefaultVenue is the venue a single-venue deployment uses.
const DefaultVenue = "PRIMARY"

// New returns an empty book. dedupWindow is the number of terminated order ids retained
// for duplicate suppression; see the note on Book.dedupRing.
func New(symbol, venue string, shardID int, collarBps int64, dedupWindow int) *Book {
	if dedupWindow < 1 {
		dedupWindow = 1
	}
	if venue == "" {
		venue = DefaultVenue
	}
	return &Book{
		Symbol:     symbol,
		Venue:      venue,
		ShardID:    shardID,
		CollarBps:  collarBps,
		byID:       make(map[uuid.UUID]*Order),
		dedupRing:  make([]uuid.UUID, dedupWindow),
		dedupSet:   make(map[uuid.UUID]struct{}, dedupWindow),
		dedupLimit: dedupWindow,
	}
}

// BookSeq is the number of fills this book has generated. Part of its snapshot state:
// restoring it is what makes fill identities line up after a rebuild.
func (b *Book) BookSeq() uint64 { return b.bookSeq }

// SetBookSeq restores the fill counter during recovery.
func (b *Book) SetBookSeq(n uint64) { b.bookSeq = n }

// Arrival is the number of orders the book has admitted; restored during recovery so
// time priority continues where it left off.
func (b *Book) Arrival() uint64 { return b.arrival }

// SetArrival restores the arrival counter during recovery.
func (b *Book) SetArrival(n uint64) { b.arrival = n }

// Submit matches an order against the book and rests or cancels the remainder.
//
// The incoming order is the taker; resting orders are makers and execute at THEIR price,
// which is what gives a resting order its price improvement and is the reason a maker
// posts in the first place.
func (b *Book) Submit(o *Order) Result {
	if b.seen(o.ID) {
		return Result{Disposition: Cancelled, DuplicateIgnored: true, CancelReason: "DUPLICATE"}
	}
	b.remember(o.ID)

	b.arrival++
	o.Seq = b.arrival
	if o.Remaining == 0 {
		o.Remaining = o.Qty
	}

	res := Result{}
	opposite := b.side(o.Side.Opposite())

	for o.Remaining > 0 && len(*opposite) > 0 {
		best := (*opposite)[0]
		if !b.crosses(o, best.price) {
			break
		}

		maker := best.orders[0]
		qty := o.Remaining
		if maker.Remaining < qty {
			qty = maker.Remaining
		}

		b.bookSeq++
		fill := Fill{
			FillID:       ids.FillID(b.ShardID, b.Venue, b.Symbol, b.bookSeq),
			ShardID:      b.ShardID,
			Symbol:       b.Symbol,
			Venue:        b.Venue,
			BookSeq:      b.bookSeq,
			Price:        best.price, // maker's price
			Qty:          qty,
			TakerOrderID: o.ID,
			TakerAccount: o.AccountID,
			TakerSide:    o.Side,
			MakerOrderID: maker.ID,
			MakerAccount: maker.AccountID,
		}
		res.Fills = append(res.Fills, fill)

		o.Remaining -= qty
		maker.Remaining -= qty
		if maker.Remaining == 0 {
			delete(b.byID, maker.ID)
			best.orders = best.orders[1:]
			if len(best.orders) == 0 {
				*opposite = (*opposite)[1:]
			}
		}
	}

	res.RemainingQty = o.Remaining
	switch {
	case o.Remaining == 0:
		res.Disposition = Complete
	case o.Type == Limit && o.TIF == GTC:
		b.rest(o)
		res.Disposition = Rested
	default:
		res.Disposition = Cancelled
		res.CancelReason = b.cancelReason(o)
	}
	return res
}

// cancelReason distinguishes "the book ran out of liquidity inside the collar" from
// "this order was never going to rest". Both cancel the remainder, but only the first
// means a client's market order was clipped by a risk control, and that is a number
// worth graphing.
func (b *Book) cancelReason(o *Order) string {
	if o.Type != Market {
		return "IOC_REMAINDER"
	}
	opposite := b.side(o.Side.Opposite())
	if len(*opposite) > 0 {
		// There IS liquidity left; the only reason we stopped is the collar.
		return "COLLAR_BREACH"
	}
	return "BOOK_EXHAUSTED"
}

// crosses reports whether an incoming order may execute at price p.
//
// For a limit order the limit price is the bound. For a market order the bound is the
// collar around the reference price stamped at accept time — this is what makes the
// reservation taken by the trading core a true upper bound on the cost of the fill, and
// therefore what stops a thin book from driving an account's cash negative.
func (b *Book) crosses(o *Order, p money.Minor) bool {
	if o.Type == Limit {
		if o.Side == Buy {
			return p <= o.LimitPrice
		}
		return p >= o.LimitPrice
	}

	if o.RefPrice <= 0 {
		return false // no reference price means no collar; refuse to execute
	}
	if o.Side == Buy {
		bound, ok := money.ApplyBps(o.RefPrice, b.CollarBps)
		return ok && p <= bound
	}
	bound, ok := money.SubBps(o.RefPrice, b.CollarBps)
	return ok && p >= bound
}

// Cancel removes a resting order, returning the quantity that was still unfilled.
func (b *Book) Cancel(id uuid.UUID) (money.Qty, bool) {
	o, ok := b.byID[id]
	if !ok {
		return 0, false
	}
	side := b.side(o.Side)
	for li, lv := range *side {
		if lv.price != b.restingPrice(o) {
			continue
		}
		for oi, cand := range lv.orders {
			if cand.ID != id {
				continue
			}
			lv.orders = append(lv.orders[:oi], lv.orders[oi+1:]...)
			if len(lv.orders) == 0 {
				*side = append((*side)[:li], (*side)[li+1:]...)
			} else {
				(*side)[li] = lv
			}
			delete(b.byID, id)
			return o.Remaining, true
		}
	}
	return 0, false
}

func (b *Book) restingPrice(o *Order) money.Minor { return o.LimitPrice }

// Restore inserts an order into the book without matching it. Used only when rebuilding
// from durable state, where the order is known to have already rested.
func (b *Book) Restore(o *Order) {
	if o.Remaining <= 0 || o.Type != Limit {
		return
	}
	b.remember(o.ID)
	b.rest(o)
	if o.Seq > b.arrival {
		b.arrival = o.Seq
	}
}

func (b *Book) rest(o *Order) {
	side := b.side(o.Side)
	price := o.LimitPrice

	idx := sort.Search(len(*side), func(i int) bool {
		if o.Side == Buy {
			return (*side)[i].price <= price // bids descend
		}
		return (*side)[i].price >= price // asks ascend
	})

	if idx < len(*side) && (*side)[idx].price == price {
		(*side)[idx].orders = append((*side)[idx].orders, o)
	} else {
		lv := &level{price: price, orders: []*Order{o}}
		*side = append(*side, nil)
		copy((*side)[idx+1:], (*side)[idx:])
		(*side)[idx] = lv
	}
	b.byID[o.ID] = o
}

func (b *Book) side(s Side) *[]*level {
	if s == Buy {
		return &b.bids
	}
	return &b.asks
}

func (b *Book) seen(id uuid.UUID) bool {
	if _, live := b.byID[id]; live {
		return true
	}
	_, ok := b.dedupSet[id]
	return ok
}

func (b *Book) remember(id uuid.UUID) {
	if _, ok := b.dedupSet[id]; ok {
		return
	}
	if old := b.dedupRing[b.dedupNext]; old != uuid.Nil {
		delete(b.dedupSet, old)
	}
	b.dedupRing[b.dedupNext] = id
	b.dedupSet[id] = struct{}{}
	b.dedupNext = (b.dedupNext + 1) % b.dedupLimit
}

// BestBid returns the highest resting bid price.
func (b *Book) BestBid() (money.Minor, bool) {
	if len(b.bids) == 0 {
		return 0, false
	}
	return b.bids[0].price, true
}

// BestAsk returns the lowest resting ask price.
func (b *Book) BestAsk() (money.Minor, bool) {
	if len(b.asks) == 0 {
		return 0, false
	}
	return b.asks[0].price, true
}

// DepthLevel is one price level of the book, for display and tests.
type DepthLevel struct {
	Price money.Minor
	Qty   money.Qty
}

// Depth returns up to n levels per side, best first.
func (b *Book) Depth(n int) (bids, asks []DepthLevel) {
	collect := func(src []*level) []DepthLevel {
		out := make([]DepthLevel, 0, n)
		for i := 0; i < len(src) && i < n; i++ {
			var q money.Qty
			for _, o := range src[i].orders {
				q += o.Remaining
			}
			out = append(out, DepthLevel{Price: src[i].price, Qty: q})
		}
		return out
	}
	return collect(b.bids), collect(b.asks)
}

// --- snapshots -------------------------------------------------------------

// Snapshot is a book's restorable state.
//
// The counters travel with the orders and are not optional. book_seq is the basis of every
// fill identity: a shard that restored its resting orders but restarted its counter would
// mint identities colliding with fills already settled, and the ledger's dedup — which
// exists to protect against double-application — would instead silently discard real
// executions. arrival preserves time priority, so a restored order does not jump the queue
// ahead of one that was behind it before the restart.
type Snapshot struct {
	BookSeq uint64  `json:"book_seq"`
	Arrival uint64  `json:"arrival"`
	Orders  []Order `json:"orders"`
}

// Snapshot captures the resting orders in priority order.
func (b *Book) Snapshot() Snapshot {
	s := Snapshot{BookSeq: b.bookSeq, Arrival: b.arrival}
	for _, side := range [][]*level{b.bids, b.asks} {
		for _, lv := range side {
			for _, o := range lv.orders {
				s.Orders = append(s.Orders, *o)
			}
		}
	}
	sort.Slice(s.Orders, func(i, j int) bool { return s.Orders[i].Seq < s.Orders[j].Seq })
	return s
}

// LoadSnapshot replaces the book's contents with a snapshot.
//
// Orders are restored in arrival order so that price-time priority is reconstructed
// exactly: resting an order is a FIFO append within its price level, so replaying the
// appends in their original sequence rebuilds the original queue.
func (b *Book) LoadSnapshot(s Snapshot) {
	b.bids, b.asks = nil, nil
	b.byID = make(map[uuid.UUID]*Order, len(s.Orders))
	b.dedupRing = make([]uuid.UUID, b.dedupLimit)
	b.dedupSet = make(map[uuid.UUID]struct{}, b.dedupLimit)
	b.dedupNext = 0

	ordered := make([]Order, len(s.Orders))
	copy(ordered, s.Orders)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Seq < ordered[j].Seq })

	for i := range ordered {
		o := ordered[i]
		if o.Remaining <= 0 {
			continue
		}
		b.remember(o.ID)
		b.rest(&o)
	}
	b.bookSeq = s.BookSeq
	b.arrival = s.Arrival
}
