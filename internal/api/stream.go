package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/events"
)

// The live fill feed — seam contract #4.
//
// It works by tailing orders.outcomes directly, NOT by hooking the engine. That choice is
// what makes it survive more than one instance: any process can read the log, so a client
// connected to instance B still sees fills that instance A settled. A notification hook
// inside the settlement path would have been fewer lines and would have quietly stopped
// working the moment the service scaled past one.
//
// Readers start at the CURRENT end of each partition. A client subscribing wants to know
// what happens next, not to be replayed the whole day; history is what GET /api/orders is
// for.
type hub struct {
	log eventlog.Log

	mu   sync.RWMutex
	subs map[uuid.UUID]map[int64]chan streamEvent
	next int64
}

// streamEvent is one thing that happened to one account.
type streamEvent struct {
	Type      string `json:"type"` // fill | order
	OrderID   string `json:"order_id"`
	Symbol    string `json:"symbol,omitempty"`
	Venue     string `json:"venue,omitempty"`
	Side      string `json:"side,omitempty"`
	Qty       string `json:"qty,omitempty"`
	Price     string `json:"price,omitempty"`
	Status    string `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Timestamp string `json:"at"`
}

func newHub(log eventlog.Log) *hub {
	return &hub{log: log, subs: map[uuid.UUID]map[int64]chan streamEvent{}}
}

// run tails every outcome partition and fans records out to subscribers.
func (h *hub) run(ctx context.Context) {
	n := h.log.Partitions(eventlog.TopicOrdersOutcomes)
	for p := int32(0); p < n; p++ {
		go h.tail(ctx, p)
	}
}

func (h *hub) tail(ctx context.Context, partition int32) {
	// Start at the end: seek past whatever already exists.
	from := h.endOf(ctx, partition)
	reader, err := h.log.Reader(eventlog.TopicOrdersOutcomes, partition, from)
	if err != nil {
		slog.Error("fill stream reader", "partition", partition, "err", err)
		return
	}
	defer reader.Close()

	for {
		if ctx.Err() != nil {
			return
		}
		recs, err := reader.Fetch(ctx, 256)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("fill stream fetch", "partition", partition, "err", err)
			}
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if len(recs) == 0 {
			time.Sleep(120 * time.Millisecond)
			continue
		}
		for _, rec := range recs {
			h.dispatch(rec)
		}
	}
}

// endOf finds the current tail of a partition by draining what is already there.
func (h *hub) endOf(ctx context.Context, partition int32) int64 {
	reader, err := h.log.Reader(eventlog.TopicOrdersOutcomes, partition, 0)
	if err != nil {
		return 0
	}
	defer reader.Close()

	var last int64
	for {
		recs, err := reader.Fetch(ctx, 1024)
		if err != nil || len(recs) == 0 {
			return last
		}
		last = recs[len(recs)-1].Offset + 1
	}
}

// dispatch turns one outcome record into a client-facing event.
func (h *hub) dispatch(rec eventlog.Record) {
	env, err := events.Decode(rec.Value)
	if err != nil {
		return // the core's consumer is the authority on unreadable records; it will stop
	}

	var (
		account uuid.UUID
		ev      streamEvent
	)
	ev.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)

	switch env.Type {
	case events.TypeFillHalf:
		m, err := events.Into[events.FillHalf](env)
		if err != nil {
			return
		}
		side := "sell"
		if m.Buying {
			side = "buy"
		}
		account = m.AccountID
		ev.Type, ev.OrderID, ev.Symbol, ev.Venue = "fill", m.OrderID.String(), m.Symbol, m.Venue
		ev.Side = side
		ev.Qty = qtyString(m.Qty)
		ev.Price = priceString(m.Price)

	case events.TypeDisposition:
		m, err := events.Into[events.Disposition](env)
		if err != nil {
			return
		}
		account = m.AccountID
		ev.Type, ev.OrderID, ev.Symbol = "order", m.OrderID.String(), m.Symbol
		ev.Status, ev.Reason = m.Disposition, m.Reason

	case events.TypeCancelOutcome:
		m, err := events.Into[events.CancelOutcome](env)
		if err != nil {
			return
		}
		account = m.AccountID
		ev.Type, ev.OrderID, ev.Symbol = "order", m.OrderID.String(), m.Symbol
		ev.Status, ev.Reason = "CANCELLED", m.Reason
		if !m.Cancelled {
			ev.Status = "CANCEL_REJECTED"
		}

	default:
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.subs[account] {
		select {
		case ch <- ev:
		default:
			// A subscriber that cannot keep up is dropped rather than blocking the tailer.
			// One slow browser must not stall the feed for everyone else, and the client
			// can reconcile by refetching its orders.
		}
	}
}

func (h *hub) subscribe(account uuid.UUID) (<-chan streamEvent, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.next++
	id := h.next
	ch := make(chan streamEvent, 64)
	if h.subs[account] == nil {
		h.subs[account] = map[int64]chan streamEvent{}
	}
	h.subs[account][id] = ch

	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if m := h.subs[account]; m != nil {
			delete(m, id)
			if len(m) == 0 {
				delete(h.subs, account)
			}
		}
		close(ch)
	}
}

// handleFills is the SSE endpoint.
func (s *Server) handleFills(w http.ResponseWriter, r *http.Request) {
	account, ok := accountOf(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "NO_ACCOUNT", "send X-Account-Id")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "NO_STREAMING", "server cannot stream")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies that buffer will hold a stream open and deliver nothing; this asks nginx not
	// to, and is harmless where it means nothing.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ch, unsubscribe := s.hub.subscribe(account)
	defer unsubscribe()

	// A heartbeat keeps intermediaries from reaping an idle connection, and tells the
	// client the difference between "quiet market" and "dead socket".
	beat := time.NewTicker(20 * time.Second)
	defer beat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-beat.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case ev := <-ch:
			b, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
			flusher.Flush()
		}
	}
}

var _ = context.Background
