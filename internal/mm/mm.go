// Package mm is a simulated market maker.
//
// It is deliberately an ordinary client. It holds an ordinary account, submits ordinary
// limit orders through the same entry point as anybody else, and gets no privileged path
// into the book. Two things follow, and both matter more than they look:
//
//  1. Replay stays deterministic. The price simulator is random, but it influences the
//     system only by causing these orders, and orders are durable. Rebuilding a book from
//     the order log reproduces it exactly, which would be false if quotes were generated
//     inside the matching engine at replay time.
//
//  2. There is never a stubbed fill path to tear out. When real accounts are admitted
//     later, they trade against these quotes in the same book, through the same code.
//
// The cost is that quoting consumes real trading-core write capacity. That is why this
// quotes on price movement rather than on a timer, and rests wide rather than churning
// cancel/replace.
package mm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/money"
)

// Config tunes the quoting strategy.
type Config struct {
	SpreadBps  int64     // half-spread of the innermost quote around the reference price
	LevelStep  int64     // additional basis points between successive levels
	Levels     int       // quote levels per side
	Size       money.Qty // size per level
	RequoteBps int64     // reprice only after the reference moves this far
	Interval   time.Duration
}

// DefaultConfig is a calm two-sided quote.
func DefaultConfig() Config {
	return Config{
		SpreadBps:  20,
		LevelStep:  15,
		Levels:     3,
		Size:       money.FromShares(50),
		RequoteBps: 10,
		Interval:   250 * time.Millisecond,
	}
}

// MarketMaker quotes both sides of a set of symbols.
type MarketMaker struct {
	eng     *engine.Engine
	md      *marketdata.Cache
	account uuid.UUID
	symbols []string
	cfg     Config

	mu        sync.Mutex
	seq       int
	resting   map[string][]uuid.UUID
	lastQuote map[string]money.Minor
}

// New builds a market maker for the given account and symbols.
func New(eng *engine.Engine, md *marketdata.Cache, account uuid.UUID, symbols []string, cfg Config) *MarketMaker {
	return &MarketMaker{
		eng: eng, md: md, account: account, symbols: symbols, cfg: cfg,
		resting:   make(map[string][]uuid.UUID),
		lastQuote: make(map[string]money.Minor),
	}
}

// Run quotes until stop closes.
func (m *MarketMaker) Run(ctx context.Context, stop <-chan struct{}) {
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			for _, sym := range m.symbols {
				_ = m.QuoteOnce(ctx, sym)
			}
		}
	}
}

// QuoteOnce refreshes one symbol's quotes if the reference price has moved enough.
// Returns the number of orders placed.
func (m *MarketMaker) QuoteOnce(ctx context.Context, symbol string) int {
	ref, ok := m.md.Ref(symbol, 0)
	if !ok || ref <= 0 {
		return 0
	}

	m.mu.Lock()
	last, quoted := m.lastQuote[symbol]
	m.mu.Unlock()

	if quoted && !m.moved(last, ref) {
		return 0
	}

	m.cancelResting(ctx, symbol)

	placed := 0
	for i := 0; i < m.cfg.Levels; i++ {
		offset := m.cfg.SpreadBps + int64(i)*m.cfg.LevelStep
		bid, okB := money.SubBps(ref, offset)
		ask, okA := money.ApplyBps(ref, offset)
		if okB && bid > 0 {
			if m.place(ctx, symbol, book.Buy, bid) {
				placed++
			}
		}
		if okA && ask > 0 {
			if m.place(ctx, symbol, book.Sell, ask) {
				placed++
			}
		}
	}

	m.mu.Lock()
	m.lastQuote[symbol] = ref
	m.mu.Unlock()
	return placed
}

func (m *MarketMaker) moved(last, ref money.Minor) bool {
	delta := ref - last
	if delta < 0 {
		delta = -delta
	}
	threshold, ok := money.FeeBps(last, m.cfg.RequoteBps)
	return ok && delta >= threshold
}

func (m *MarketMaker) place(ctx context.Context, symbol string, side book.Side, price money.Minor) bool {
	m.mu.Lock()
	m.seq++
	clientID := fmt.Sprintf("mm-%s-%s-%d", symbol, side, m.seq)
	m.mu.Unlock()

	view, err := m.eng.Submit(ctx, engine.SubmitRequest{
		AccountID:     m.account,
		ClientOrderID: clientID,
		Symbol:        symbol,
		Side:          side,
		Type:          book.Limit,
		TIF:           book.GTC,
		Qty:           m.cfg.Size,
		LimitPrice:    price,
	})
	// A rejected quote is normal: the maker runs out of inventory or buying power like
	// any other participant. It is not an error condition for the engine.
	if err != nil || view.Status == "REJECTED" {
		return false
	}

	if view.Status == "ACCEPTED" || view.Status == "PARTIALLY_FILLED" {
		m.mu.Lock()
		m.resting[symbol] = append(m.resting[symbol], view.ID)
		m.mu.Unlock()
	}
	return true
}

func (m *MarketMaker) cancelResting(ctx context.Context, symbol string) {
	m.mu.Lock()
	ids := m.resting[symbol]
	m.resting[symbol] = nil
	m.mu.Unlock()

	for _, id := range ids {
		_, _ = m.eng.Cancel(ctx, id)
	}
}

// Account returns the maker's account id.
func (m *MarketMaker) Account() uuid.UUID { return m.account }
