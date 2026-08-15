package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/money"
)

// QueriesConfig configures a Queries connection.
type QueriesConfig struct {
	// DSN is the ClickHouse connection string. Empty uses DefaultDSN.
	DSN string
}

// Queries answers aggregate reads against the tables Ingester writes.
//
// Every result here is a float or a ratio, never a stored value: internal/money's int64
// minor-units and 1e-6-share convention is what the fills and order_events columns store
// (schema.sql), but a VWAP, a notional sum, or a rate is inherently a computed ratio, and
// forcing that back into fixed-point would just relocate the rounding this package doesn't
// need to be exact about — these numbers back a dashboard, not a ledger entry. See the
// package doc for why a ledger entry never reads from here regardless.
type Queries struct {
	conn driver.Conn
}

// NewQueries connects to ClickHouse and verifies it is reachable before returning.
//
// As with NewIngester, failing fast here is the point: a caller wiring up a dashboard or
// an API endpoint decides once, at startup, whether analytics is available, rather than
// having every query call learn it the hard way.
func NewQueries(cfg QueriesConfig) (*Queries, error) {
	conn, err := connect(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("analytics: new queries: %w", err)
	}
	return &Queries{conn: conn}, nil
}

// Close closes the underlying ClickHouse connection.
func (q *Queries) Close() error { return q.conn.Close() }

// VWAPPoint is one symbol's volume-weighted average execution price over a queried range.
type VWAPPoint struct {
	Symbol string
	// VWAP is in Minor units (cents) per whole share — a float because it is a ratio of
	// summed int64 columns, not a stored value (package doc).
	VWAP float64
	// Shares is the total quantity traded, in whole shares (money.Qty / money.QtyScale).
	Shares float64
}

// VWAP computes volume-weighted average price per symbol over [from, to).
//
// It reads FINAL and filters side = 'TAKER'. FINAL forces ClickHouse to resolve
// ReplacingMergeTree duplicates before aggregating rather than after, which matters here
// specifically because a duplicated row would otherwise double-count both the numerator
// and denominator identically and never show up as an obviously wrong ratio. The TAKER
// filter is what keeps one execution counted once: every fill produces two rows in fills,
// one per side of the trade (schema.sql), and summing across both would double both price
// weight and volume for no benefit — TAKER and MAKER report the same execution at the same
// price and quantity, so either side alone reconstructs the trade.
func (q *Queries) VWAP(ctx context.Context, from, to time.Time) ([]VWAPPoint, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT symbol,
		       sum(toFloat64(price) * toFloat64(qty)) / sum(toFloat64(qty)) AS vwap,
		       sum(toFloat64(qty)) / ? AS shares
		  FROM fills FINAL
		 WHERE ts >= ? AND ts < ? AND side = 'TAKER'
		 GROUP BY symbol
		 ORDER BY symbol`,
		float64(money.QtyScale), from, to)
	if err != nil {
		return nil, fmt.Errorf("analytics: vwap query: %w", err)
	}
	defer rows.Close()

	var out []VWAPPoint
	for rows.Next() {
		var p VWAPPoint
		if err := rows.Scan(&p.Symbol, &p.VWAP, &p.Shares); err != nil {
			return nil, fmt.Errorf("analytics: vwap scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analytics: vwap rows: %w", err)
	}
	return out, nil
}

// AccountNotional is one account's traded notional over a queried range.
type AccountNotional struct {
	AccountID uuid.UUID
	// Notional is in Minor units (cents) — a float for the reason VWAP's is: it is a
	// computed sum over int64 columns, not a stored value.
	Notional float64
	Fills    uint64
}

// TopAccountsByNotional ranks accounts by traded notional over [from, to), descending,
// limited to the top n.
//
// Unlike VWAP this deliberately does NOT filter to TAKER: traded notional is "how much did
// this account trade," and an account's maker fills are exactly as much its own trading
// activity as its taker fills are. Filtering to TAKER here would undercount every
// market-maker account, which is the opposite of what this query is for.
func (q *Queries) TopAccountsByNotional(ctx context.Context, from, to time.Time, n int) ([]AccountNotional, error) {
	if n <= 0 {
		n = 20
	}
	rows, err := q.conn.Query(ctx, `
		SELECT account_id,
		       sum(toFloat64(price) * toFloat64(qty)) / ? AS notional,
		       count() AS fills
		  FROM fills FINAL
		 WHERE ts >= ? AND ts < ?
		 GROUP BY account_id
		 ORDER BY notional DESC
		 LIMIT ?`,
		float64(money.QtyScale), from, to, n)
	if err != nil {
		return nil, fmt.Errorf("analytics: top accounts query: %w", err)
	}
	defer rows.Close()

	var out []AccountNotional
	for rows.Next() {
		var a AccountNotional
		if err := rows.Scan(&a.AccountID, &a.Notional, &a.Fills); err != nil {
			return nil, fmt.Errorf("analytics: top accounts scan: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analytics: top accounts rows: %w", err)
	}
	return out, nil
}

// VolumeBucket is one time bucket's fill count and traded volume for a symbol.
type VolumeBucket struct {
	Bucket time.Time
	Fills  uint64
	// Shares is total quantity traded in that bucket, in whole shares.
	Shares float64
}

// VolumeSeries buckets fill count and traded volume for one symbol into fixed-width
// intervals over [from, to).
//
// Like VWAP this filters to side = 'TAKER' so each execution is counted once rather than
// twice; a "fills per minute" series built from both sides would silently be a "fill-halves
// per minute" series worth double the true execution count.
func (q *Queries) VolumeSeries(ctx context.Context, symbol string, from, to time.Time, bucket time.Duration) ([]VolumeBucket, error) {
	if bucket <= 0 {
		bucket = time.Minute
	}
	seconds := int64(bucket / time.Second)
	if seconds < 1 {
		seconds = 1
	}

	// The bucket width is interpolated directly into the query rather than bound as a
	// parameter because ClickHouse's INTERVAL syntax does not accept a bound parameter in
	// that position. It is safe to interpolate here because it is an int64 this package
	// itself computed above, never a caller-supplied string.
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(ts, INTERVAL %d SECOND) AS bucket,
		       count() AS fills,
		       sum(toFloat64(qty)) / ? AS shares
		  FROM fills FINAL
		 WHERE symbol = ? AND ts >= ? AND ts < ? AND side = 'TAKER'
		 GROUP BY bucket
		 ORDER BY bucket`, seconds)

	rows, err := q.conn.Query(ctx, query, float64(money.QtyScale), symbol, from, to)
	if err != nil {
		return nil, fmt.Errorf("analytics: volume series query: %w", err)
	}
	defer rows.Close()

	var out []VolumeBucket
	for rows.Next() {
		var b VolumeBucket
		if err := rows.Scan(&b.Bucket, &b.Fills, &b.Shares); err != nil {
			return nil, fmt.Errorf("analytics: volume series scan: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analytics: volume series rows: %w", err)
	}
	return out, nil
}

// RejectionRate is one symbol's share of orders that ended cancelled or rejected rather
// than complete, over a queried range.
type RejectionRate struct {
	Symbol string
	// TotalOrders is the count of distinct orders with at least one recorded outcome for
	// this symbol in the range — the denominator.
	TotalOrders uint64
	// Cancelled is orders whose disposition ended CANCELLED (covers collar breaches,
	// exhausted IOC remainders, and explicit cancels that won their race — see Reasons).
	Cancelled uint64
	// Rate is Cancelled / TotalOrders, 0 when TotalOrders is 0.
	Rate float64
}

// RejectionRate computes, per symbol, the fraction of orders whose final disposition was
// CANCELLED rather than COMPLETE, over [from, to).
//
// The rate's numerator is drawn from ORDER_DISPOSITION rows (disposition = 'CANCELLED'),
// which is the book's own terminal verdict on an order's remainder (§5.3-adjacent: every
// order gets exactly one such verdict). CANCEL_OUTCOME rows are not double-counted into
// the numerator — a successful explicit cancel (CancelOutcome.Cancelled = true) is always
// followed by the disposition that already counts it, and a lost race
// (Cancelled = false) is by definition NOT a cancellation (§5.6) — but a symbol's
// CANCEL_OUTCOME reasons remain queryable in order_events directly for a caller that wants
// the breakdown between "cancel requested and won" and "cancel requested and lost to a
// fill."
func (q *Queries) RejectionRate(ctx context.Context, from, to time.Time) ([]RejectionRate, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT symbol,
		       countDistinct(order_id) AS total_orders,
		       countDistinctIf(order_id, disposition = 'CANCELLED') AS cancelled
		  FROM order_events FINAL
		 WHERE kind = 'DISPOSITION' AND ts >= ? AND ts < ?
		 GROUP BY symbol
		 ORDER BY symbol`,
		from, to)
	if err != nil {
		return nil, fmt.Errorf("analytics: rejection rate query: %w", err)
	}
	defer rows.Close()

	var out []RejectionRate
	for rows.Next() {
		var r RejectionRate
		if err := rows.Scan(&r.Symbol, &r.TotalOrders, &r.Cancelled); err != nil {
			return nil, fmt.Errorf("analytics: rejection rate scan: %w", err)
		}
		if r.TotalOrders > 0 {
			r.Rate = float64(r.Cancelled) / float64(r.TotalOrders)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analytics: rejection rate rows: %w", err)
	}
	return out, nil
}
