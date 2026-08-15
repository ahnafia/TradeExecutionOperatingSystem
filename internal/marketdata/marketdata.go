// Package marketdata is a price simulator behind a conflating cache.
//
// Conflation is the whole design: the cache keeps only the latest tick per symbol and
// drops superseded ones. A superseded tick carries no information, so dropping it is
// correct rather than lossy, and it means tick volume never reaches the transactional
// path — the trading core does a point read for a reference price and nothing more.
//
// Note where the simulator sits relative to replay. It is the only non-deterministic
// component in the system, and it is deliberately OUTSIDE the replay boundary: it
// influences the engine only by moving prices that cause market makers to submit
// orders, and those orders are durable. Replaying the order log rebuilds the books
// exactly, even though re-running this simulator would not reproduce the same prices.
package marketdata

import (
	"math/rand"
	"sync"
	"time"

	"github.com/ahnafia/trading-system/internal/money"
)

// Tick is the latest known price for a symbol.
type Tick struct {
	Symbol string
	Price  money.Minor
	At     time.Time
}

// Cache holds the latest tick per symbol. Safe for concurrent use.
type Cache struct {
	mu    sync.RWMutex
	ticks map[string]Tick
	now   func() time.Time
}

// NewCache returns an empty conflating cache.
func NewCache() *Cache {
	return &Cache{ticks: make(map[string]Tick), now: time.Now}
}

// Publish records a tick, overwriting any older one for the same symbol.
func (c *Cache) Publish(t Tick) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ticks[t.Symbol] = t
}

// Ref returns the reference price for a symbol, and false if there is none or it is
// older than maxAge.
//
// A market order cannot be accepted without this: the reservation that keeps cash from
// going negative is sized from it. A limit order can, because its limit price is its own
// collar. That asymmetry is the whole degradation story when market data is down.
func (c *Cache) Ref(symbol string, maxAge time.Duration) (money.Minor, bool) {
	c.mu.RLock()
	t, ok := c.ticks[symbol]
	c.mu.RUnlock()
	if !ok || t.Price <= 0 {
		return 0, false
	}
	if maxAge > 0 && c.now().Sub(t.At) > maxAge {
		return 0, false
	}
	return t.Price, true
}

// Snapshot returns every current tick, for the CLI.
func (c *Cache) Snapshot() []Tick {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Tick, 0, len(c.ticks))
	for _, t := range c.ticks {
		out = append(out, t)
	}
	return out
}

// Simulator drives a random walk per symbol into a Cache.
type Simulator struct {
	cache    *Cache
	interval time.Duration
	volBps   int64
	rng      *rand.Rand

	mu     sync.Mutex
	prices map[string]money.Minor
}

// NewSimulator seeds a walk at the given starting prices. volBps is the standard
// deviation of each step in basis points.
func NewSimulator(cache *Cache, seed map[string]money.Minor, interval time.Duration, volBps int64, rngSeed int64) *Simulator {
	prices := make(map[string]money.Minor, len(seed))
	for k, v := range seed {
		prices[k] = v
	}
	s := &Simulator{
		cache:    cache,
		interval: interval,
		volBps:   volBps,
		rng:      rand.New(rand.NewSource(rngSeed)),
		prices:   prices,
	}
	s.publishAll(time.Now())
	return s
}

// Run steps the walk until ctx-like stop channel closes.
func (s *Simulator) Run(stop <-chan struct{}) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			s.step(now)
		}
	}
}

func (s *Simulator) step(now time.Time) {
	s.mu.Lock()
	for sym, p := range s.prices {
		drift := money.Minor(s.rng.NormFloat64() * float64(s.volBps) / 10_000 * float64(p))
		next := p + drift
		if next < 1 {
			next = 1
		}
		s.prices[sym] = next
	}
	s.mu.Unlock()
	s.publishAll(now)
}

func (s *Simulator) publishAll(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sym, p := range s.prices {
		s.cache.Publish(Tick{Symbol: sym, Price: p, At: now})
	}
}

// Price returns the simulator's current price for a symbol.
func (s *Simulator) Price(symbol string) (money.Minor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.prices[symbol]
	return p, ok
}

// Symbols returns the simulated symbols.
func (s *Simulator) Symbols() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.prices))
	for k := range s.prices {
		out = append(out, k)
	}
	return out
}
