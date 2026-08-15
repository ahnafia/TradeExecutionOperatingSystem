package matching

import (
	"sort"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/money"
)

// Venue is one place a symbol can trade.
//
// Latency is synthetic and exists to break price ties. Real smart order routers weigh
// far more than this — fees, fill probability, information leakage, rebates — but the
// shape of the decision is the same, and the shape is what matters here: price first,
// then something that distinguishes equal prices, deterministically.
type Venue struct {
	Name         string
	LatencyMicro int64
}

// DefaultVenues is a single-venue deployment.
func DefaultVenues() []Venue { return []Venue{{Name: book.DefaultVenue}} }

// routePlan is one step of a routed order: take some quantity at one venue.
type routePlan struct {
	venue string
	qty   money.Qty
}

// route decides how to split an order across venues.
//
// The algorithm is the obvious one — repeatedly take the best price available anywhere —
// and it is the correctness of the ordering that matters, not its sophistication. Two
// properties are load-bearing:
//
//   - It is DETERMINISTIC. Venues are considered in a fixed order and ties break on a
//     fixed key, so a replay of the same inbound stream produces the same split. A router
//     that consulted wall-clock latency, or iterated a map, would make the matching engine
//     non-replayable — and replayability is what makes its crash recovery safe.
//
//   - It respects the SAME collar the reservation was sized against. Routing across venues
//     must not become a way to execute outside the band the trading core priced, because
//     the reservation bounding realized cost assumed that band.
//
// Returns the ordered plan. Quantities not routable inside the collar are simply not in
// the plan, and the caller cancels or rests the remainder as the order's TIF dictates.
func (s *Service) route(o *book.Order, books map[string]*book.Book) []routePlan {
	remaining := o.Remaining
	if remaining <= 0 {
		return nil
	}

	// Fixed iteration order: sorted by name, so the plan cannot depend on map ordering.
	names := make([]string, 0, len(books))
	for name := range books {
		names = append(names, name)
	}
	sort.Strings(names)

	// Snapshot the depth each venue is offering, so planning does not mutate any book.
	type quote struct {
		venue   string
		price   money.Minor
		qty     money.Qty
		latency int64
	}
	var offers []quote
	for _, name := range names {
		b := books[name]
		bids, asks := b.Depth(1 << 20)
		levels := asks
		if o.Side == book.Sell {
			levels = bids
		}
		for _, lv := range levels {
			offers = append(offers, quote{
				venue: name, price: lv.Price, qty: lv.Qty,
				latency: s.venueLatency(name),
			})
		}
	}

	// Best price first; equal prices go to the faster venue; equal again breaks on name so
	// the result is a total order with no ambiguity left.
	sort.SliceStable(offers, func(i, j int) bool {
		a, b := offers[i], offers[j]
		if a.price != b.price {
			if o.Side == book.Buy {
				return a.price < b.price // buying: cheapest first
			}
			return a.price > b.price // selling: richest first
		}
		if a.latency != b.latency {
			return a.latency < b.latency
		}
		return a.venue < b.venue
	})

	// Accumulate per venue rather than emitting a step per price level: the venue's own
	// book will walk its levels itself, and one instruction per venue keeps the plan small
	// and the resulting fills identical to what that venue would have produced alone.
	planned := map[string]money.Qty{}
	var order []string
	for _, off := range offers {
		if remaining <= 0 {
			break
		}
		if !crossable(o, off.price, s.cfg.CollarBps) {
			continue
		}
		take := off.qty
		if take > remaining {
			take = remaining
		}
		if take <= 0 {
			continue
		}
		if _, seen := planned[off.venue]; !seen {
			order = append(order, off.venue)
		}
		planned[off.venue] += take
		remaining -= take
	}

	out := make([]routePlan, 0, len(order))
	for _, v := range order {
		out = append(out, routePlan{venue: v, qty: planned[v]})
	}
	return out
}

// crossable mirrors the book's own crossing rule, so the router never plans an execution
// the book would refuse. Duplicating the rule is deliberate: the book must keep enforcing
// it independently, because the router is an optimisation and the book is the guarantee.
func crossable(o *book.Order, price money.Minor, collarBps int64) bool {
	if o.Type == book.Limit {
		if o.Side == book.Buy {
			return price <= o.LimitPrice
		}
		return price >= o.LimitPrice
	}
	if o.RefPrice <= 0 {
		return false
	}
	if o.Side == book.Buy {
		bound, ok := money.ApplyBps(o.RefPrice, collarBps)
		return ok && price <= bound
	}
	bound, ok := money.SubBps(o.RefPrice, collarBps)
	return ok && price >= bound
}

func (s *Service) venueLatency(name string) int64 {
	for _, v := range s.cfg.Venues {
		if v.Name == name {
			return v.LatencyMicro
		}
	}
	return 0
}

// primaryVenue is where an unrouted remainder rests. The first configured venue, so the
// choice is explicit configuration rather than whichever map key came out first.
func (s *Service) primaryVenue() string {
	if len(s.cfg.Venues) > 0 {
		return s.cfg.Venues[0].Name
	}
	return book.DefaultVenue
}
