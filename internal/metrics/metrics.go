// Package metrics exposes the engine's behaviour and its invariants to Prometheus.
//
// The invariant gauges are the point. RED metrics tell you the system is serving; the
// invariant gauges tell you it is serving *correctly*, and during a chaos run they are the
// entire story — the screen showing them pinned at zero while partitions are being killed
// is what the project is for.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/money"
)

// Metrics is the registry and every collector in it.
type Metrics struct {
	reg *prometheus.Registry

	submitDuration *prometheus.HistogramVec
	ordersTotal    *prometheus.CounterVec
	rejections     *prometheus.CounterVec
	cancels        *prometheus.CounterVec
	fillsTotal     prometheus.Counter
	fillQty        prometheus.Counter
	fillNotional   prometheus.Counter

	invariant       *prometheus.GaugeVec
	inFlight        prometheus.Gauge
	shareDelta      *prometheus.GaugeVec
	invariantOK     prometheus.Gauge
	checkDuration   prometheus.Histogram
	snapshotTail    prometheus.Gauge
	snapshotCount   prometheus.Gauge
	reservationOver prometheus.Gauge
}

// New builds a registry with all collectors registered.
func New() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}

	m.submitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "trading_submit_duration_seconds",
		Help: "End-to-end order submission latency, by resulting status.",
		// Bucketed around the documented budget: ~4ms p50, ~25ms p99.
		Buckets: []float64{.001, .002, .004, .008, .016, .025, .05, .1, .25, .5, 1},
	}, []string{"status"})

	m.ordersTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trading_orders_total",
		Help: "Orders submitted, by resulting status.",
	}, []string{"status"})

	m.rejections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trading_rejections_total",
		Help: "Rejected orders by reason. COLLAR_BREACH and NO_REFERENCE_PRICE are risk controls firing, not faults.",
	}, []string{"reason"})

	m.cancels = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trading_cancels_total",
		Help: "Cancel adjudications by final order status. A cancel that finds the order FILLED lost the race, which is a normal outcome.",
	}, []string{"status"})

	m.fillsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trading_fills_total", Help: "Fills generated.",
	})
	m.fillQty = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trading_filled_shares_total", Help: "Shares filled.",
	})
	m.fillNotional = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trading_filled_notional_minor_total", Help: "Filled notional in minor units.",
	})

	// One gauge with a `check` label rather than a dozen names: the dashboard panel is
	// "every series on this metric must be zero", which stays true when a check is added.
	m.invariant = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "trading_invariant_violations",
		Help: "Invariant check results. EVERY series must be zero.",
	}, []string{"check"})

	// Deliberately NOT on the violations metric. A fill is two transactions with no
	// transaction spanning them, so halves in flight are the normal state of a busy
	// system. Graph it next to the violations panel as saturation, not as breakage.
	m.inFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trading_fill_halves_in_flight",
		Help: "Fill halves still settling inside the settle window. Expected to be non-zero under load; a rising trend is backpressure.",
	})

	m.shareDelta = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "trading_share_conservation_delta",
		Help: "Per symbol: shares held across all accounts minus shares issued. Must be zero.",
	}, []string{"symbol"})

	m.invariantOK = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trading_invariants_ok",
		Help: "1 when every invariant holds, 0 otherwise.",
	})

	m.checkDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "trading_invariant_check_duration_seconds",
		Help:    "Time to run the full verification pass.",
		Buckets: prometheus.DefBuckets,
	})

	m.snapshotTail = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trading_snapshot_max_tail",
		Help: "Worst-case number of transactions any account must replay past its latest snapshot. This is what snapshots exist to bound.",
	})
	m.snapshotCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trading_snapshots", Help: "Snapshot rows retained.",
	})
	m.reservationOver = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trading_reservation_worst_overdraw_minor",
		Help: "Worst (consumed - reserved) over all orders. Above zero means a fill cost more than was reserved for it.",
	})

	m.reg.MustRegister(
		m.submitDuration, m.ordersTotal, m.rejections, m.cancels,
		m.fillsTotal, m.fillQty, m.fillNotional,
		m.invariant, m.inFlight, m.shareDelta, m.invariantOK, m.checkDuration,
		m.snapshotTail, m.snapshotCount, m.reservationOver,
	)
	return m
}

// Handler serves the Prometheus exposition endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// --- engine.Observer -------------------------------------------------------

func (m *Metrics) ObserveSubmit(status string, d time.Duration) {
	if status == "" {
		status = "UNKNOWN"
	}
	m.submitDuration.WithLabelValues(status).Observe(d.Seconds())
	m.ordersTotal.WithLabelValues(status).Inc()
}

func (m *Metrics) ObserveFill(qty money.Qty, notional money.Minor) {
	m.fillsTotal.Inc()
	m.fillQty.Add(float64(qty) / money.QtyScale)
	m.fillNotional.Add(float64(notional))
}

func (m *Metrics) ObserveRejection(reason string) {
	if reason == "" {
		reason = "UNKNOWN"
	}
	m.rejections.WithLabelValues(reason).Inc()
}

func (m *Metrics) ObserveCancel(status string) {
	if status == "" {
		status = "UNKNOWN"
	}
	m.cancels.WithLabelValues(status).Inc()
}

// --- invariant refresh -----------------------------------------------------

// Refresh runs the verification pass and publishes the result.
func (m *Metrics) Refresh(ctx context.Context, e *engine.Engine) error {
	started := time.Now()
	inv, err := e.CheckInvariants(ctx)
	m.checkDuration.Observe(time.Since(started).Seconds())
	if err != nil {
		m.invariantOK.Set(0)
		return err
	}

	for check, v := range map[string]int64{
		"unbalanced_txns":          inv.UnbalancedTxns,
		"orphaned_fill_halves":     inv.OrphanedFillHalves,
		"unpaired_clearing_units":  inv.UnpairedClearingUnits,
		"cash_conservation_delta":  inv.CashConservationDelta,
		"position_ledger_mismatch": inv.PositionLedgerMismatch,
		"balance_ledger_mismatch":  inv.BalanceLedgerMismatch,
		"negative_cash_accounts":   inv.NegativeCashAccounts,
		"overdrawn_reservations":   inv.OverdrawnReservations,
	} {
		m.invariant.WithLabelValues(check).Set(float64(v))
	}

	m.inFlight.Set(float64(inv.InFlightFillHalves))

	m.shareDelta.Reset() // a symbol that stops trading should stop reporting, not go stale
	for symbol, delta := range inv.ShareConservation {
		m.shareDelta.WithLabelValues(symbol).Set(float64(delta))
	}

	worst, err := e.ReservationBound(ctx)
	if err != nil {
		return err
	}
	m.reservationOver.Set(float64(worst))

	stats, err := e.SnapshotStats(ctx)
	if err != nil {
		return err
	}
	m.snapshotTail.Set(float64(stats.MaxTail))
	m.snapshotCount.Set(float64(stats.Snapshots))

	ok := inv.OK() && worst <= 0
	if ok {
		m.invariantOK.Set(1)
	} else {
		m.invariantOK.Set(0)
	}
	return nil
}

// RefreshLoop republishes the invariant gauges until ctx is cancelled.
func (m *Metrics) RefreshLoop(ctx context.Context, e *engine.Engine, every time.Duration, onErr func(error)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := m.Refresh(ctx, e); err != nil && onErr != nil {
			onErr(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
