package engine

import (
	"time"

	"github.com/ahnafia/trading-system/internal/money"
)

// Observer receives instrumentation events.
//
// It is an interface rather than a direct Prometheus dependency so the engine stays
// ignorant of how it is monitored. That is not purity for its own sake: the engine is the
// thing that gets split across six services in later phases, and a package that imports a
// metrics registry drags that registry into every one of them.
type Observer interface {
	ObserveSubmit(status string, d time.Duration)
	ObserveFill(qty money.Qty, notional money.Minor)
	ObserveRejection(reason string)
	ObserveCancel(status string)
}

// NopObserver discards everything. The default, so nothing has to be wired up for the
// engine to run.
type NopObserver struct{}

func (NopObserver) ObserveSubmit(string, time.Duration) {}
func (NopObserver) ObserveFill(money.Qty, money.Minor)  {}
func (NopObserver) ObserveRejection(string)             {}
func (NopObserver) ObserveCancel(string)                {}

// Observe attaches an observer. Not safe to call while the engine is serving.
func (e *Engine) Observe(o Observer) {
	if o == nil {
		o = NopObserver{}
	}
	e.obs = o
}
