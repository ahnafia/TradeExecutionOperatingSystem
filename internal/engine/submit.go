package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/events"
	"github.com/ahnafia/trading-system/internal/money"
)

// SubmitRequest is a client order.
type SubmitRequest struct {
	AccountID     uuid.UUID
	ClientOrderID string
	Symbol        string
	Side          book.Side
	Type          book.Type
	TIF           book.TIF
	Qty           money.Qty
	LimitPrice    money.Minor

	// Venue is where a resting remainder should post. Empty means the primary venue.
	// It never affects where the order TAKES from — the router always sweeps for the best
	// price — only where it rests if it does not fully execute.
	Venue string
}

// OrderView is the acknowledged state of an order.
//
// Note what is NOT here any more: fills. In Phase 2 Submit matched the order and returned
// its executions, because the book was in the same process. It is not, and pretending
// otherwise would mean blocking the client until a round trip through the log completed —
// converting a 4ms acknowledgement into a 40ms one and coupling accept availability to the
// matching engine's. ACCEPTED means durable and certain to reach a terminal state; what
// that state is arrives later, on the outcome stream.
type OrderView struct {
	ID             uuid.UUID
	AccountID      uuid.UUID
	ClientOrderID  string
	Symbol         string
	Side           book.Side
	Type           book.Type
	TIF            book.TIF
	Qty            money.Qty
	LimitPrice     money.Minor
	RefPrice       money.Minor
	Status         string
	RejectReason   string
	FilledQty      money.Qty
	FilledNotional money.Minor
	Venue          string
}

// AvgPrice returns the volume-weighted average fill price, or 0 if unfilled.
func (o OrderView) AvgPrice() money.Minor {
	if o.FilledQty == 0 {
		return 0
	}
	return money.Minor(int64(o.FilledNotional) * money.QtyScale / int64(o.FilledQty))
}

// Submit accepts an order: one durable transaction, then done.
//
// The transaction does five things that must all happen or none of them: it takes the
// account's lock, checks risk, reserves funds, records the order, and enqueues the
// intent to publish it. That last one is the outbox, and it is what replaces "and then
// call the matching engine". There is no RPC here whose failure could leave an order
// reserved but unrouted, because there is no RPC.
func (e *Engine) Submit(ctx context.Context, req SubmitRequest) (view OrderView, err error) {
	started := time.Now()
	defer func() {
		status := view.Status
		if err != nil {
			var rej *Rejection
			if errors.As(err, &rej) {
				status = "REJECTED"
				e.obs.ObserveRejection(rej.Reason)
			} else {
				status = "ERROR"
			}
		} else if status == "REJECTED" {
			e.obs.ObserveRejection(view.RejectReason)
		}
		e.obs.ObserveSubmit(status, time.Since(started))
	}()

	if req.Qty <= 0 {
		return OrderView{}, reject("INVALID_QTY", "qty must be positive")
	}
	if req.Type == book.Limit && req.LimitPrice <= 0 {
		return OrderView{}, reject("INVALID_PRICE", "limit order needs a positive limit price")
	}
	if req.ClientOrderID == "" {
		return OrderView{}, reject("INVALID_CLIENT_ORDER_ID", "client_order_id is required")
	}

	// A market order's reservation is sized from a reference price, so without one there
	// is no safe amount to reserve and the order cannot be accepted. A limit order is its
	// own collar and needs no reference — the entire degradation story when market data
	// is unavailable.
	var ref money.Minor
	if req.Type == book.Market {
		var ok bool
		ref, ok = e.md.Ref(req.Symbol, e.cfg.MaxRefStaleness)
		if !ok {
			// A market order is priced against a reference the reservation is sized from,
			// so without one there is no safe amount to hold and the order cannot be
			// accepted. Say what to do instead — a caller told only that a value is
			// missing has to go read the source to find out that a limit order works.
			return OrderView{}, reject("NO_REFERENCE_PRICE",
				"no recent price for %s, so a market order cannot be collared or sized; "+
					"use a limit order, which prices itself", req.Symbol)
		}
	}
	return e.accept(ctx, req, ref)
}

// accept is the one synchronous durable write on the hot path.
func (e *Engine) accept(ctx context.Context, req SubmitRequest, ref money.Minor) (OrderView, error) {
	view := OrderView{
		ID:            uuid.New(),
		AccountID:     req.AccountID,
		ClientOrderID: req.ClientOrderID,
		Symbol:        req.Symbol,
		Side:          req.Side,
		Type:          req.Type,
		TIF:           req.TIF,
		Qty:           req.Qty,
		LimitPrice:    req.LimitPrice,
		RefPrice:      ref,
		Venue:         req.Venue,
	}

	err := e.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := lockAccount(ctx, tx, req.AccountID); err != nil {
			return err
		}

		// Client idempotency: a retried submit returns the original order rather than
		// creating a second one, and does not enqueue a second publish.
		existing, found, err := loadOrderByClientID(ctx, tx, req.AccountID, req.ClientOrderID)
		if err != nil {
			return err
		}
		if found {
			view = existing
			return nil
		}

		res, err := e.sizeReservation(req, ref)
		if err != nil {
			return err
		}

		if err := e.riskCheck(ctx, tx, req, res); err != nil {
			var rej *Rejection
			if errors.As(err, &rej) {
				// A rejected order never reaches a book, so nothing is enqueued. It is
				// recorded anyway: "we said no, and here is why" is part of the audit
				// trail, and a client retrying the same client_order_id must get the same
				// answer rather than a second evaluation against a moved market.
				view.Status, view.RejectReason = "REJECTED", rej.Reason
				return insertOrder(ctx, tx, view, nil)
			}
			return err
		}

		view.Status = "ACCEPTED"
		if err := insertOrder(ctx, tx, view, &res); err != nil {
			return err
		}
		return enqueueAccepted(ctx, tx, view)
	})
	if err != nil {
		return OrderView{}, err
	}
	return view, nil
}

// enqueueAccepted writes the outbox row that will become an inbound record for the book.
//
// Keyed by SYMBOL, because the ordering that matters on the way in is per book. Two orders
// from one account for different symbols have no defined relative arrival, and do not need
// one; two orders for the same symbol do, and get it.
func enqueueAccepted(ctx context.Context, tx pgx.Tx, v OrderView) error {
	payload, err := events.Encode(events.TypeOrderAccepted, events.OrderAccepted{
		OrderID:    v.ID,
		AccountID:  v.AccountID,
		Symbol:     v.Symbol,
		Side:       v.Side.String(),
		Type:       v.Type.String(),
		TIF:        v.TIF.String(),
		Qty:        int64(v.Qty),
		LimitPrice: int64(v.LimitPrice),
		RefPrice:   int64(v.RefPrice),
		Venue:      v.Venue,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		eventlog.TopicOrdersInbound, v.Symbol, payload)
	if err != nil {
		return fmt.Errorf("enqueue accepted order: %w", err)
	}
	return nil
}

// reservation is the amount set aside at accept time.
//
// unitPrice and feeBps record the TERMS the reservation was taken under, not just the
// total. Releasing headroom incrementally has to reconstruct what was owed at an earlier
// fill level, and recomputing that from live config would be wrong the moment the collar
// or fee schedule changes while an order is working.
type reservation struct {
	kind      string // CASH or SHARES
	symbol    string
	amount    int64 // minor units for CASH, qty units for SHARES
	unitPrice int64 // per-share price the cash reservation was sized at; 0 for SHARES
	feeBps    int64
}

// sizeReservation computes an upper bound on what the order can possibly cost.
//
// For a BUY the bound is the collar-widened reference price (market) or the limit price
// (limit), plus fee headroom, rounded UP. The book will not execute outside the same
// collar, so realized cost is bounded above by this number — which is what makes "cash
// never goes negative" a property of the design rather than a hope about the market.
func (e *Engine) sizeReservation(req SubmitRequest, ref money.Minor) (reservation, error) {
	if req.Side == book.Sell {
		return reservation{kind: "SHARES", symbol: req.Symbol, amount: int64(req.Qty)}, nil
	}

	bound := req.LimitPrice
	if req.Type == book.Market {
		var ok bool
		bound, ok = money.ApplyBps(ref, e.cfg.CollarBps)
		if !ok {
			return reservation{}, reject("OVERFLOW", "collar bound does not fit")
		}
	}

	notional, ok := money.NotionalCeil(req.Qty, bound)
	if !ok {
		return reservation{}, reject("OVERFLOW", "order notional does not fit")
	}
	// Reserve at the taker rate even for a limit order that may end up a maker: the
	// reservation must bound the worst case, and over-reserving only costs buying power.
	fee, ok := money.FeeBps(notional, e.cfg.TakerFeeBps)
	if !ok {
		return reservation{}, reject("OVERFLOW", "fee does not fit")
	}
	total, ok := money.Add(notional, fee)
	if !ok {
		return reservation{}, reject("OVERFLOW", "reservation does not fit")
	}
	return reservation{
		kind:      "CASH",
		amount:    int64(total),
		unitPrice: int64(bound),
		feeBps:    e.cfg.TakerFeeBps,
	}, nil
}

// riskCheck runs inside the accept transaction, under the account row lock.
//
// This is why risk is a package and not a service. Two concurrent BUYs that each read the
// same balance and each independently "pass" would both execute and overdraw the account.
// Holding the lock across the check AND the reservation is what makes that impossible,
// and a network hop in the middle would give the property away for nothing.
func (e *Engine) riskCheck(ctx context.Context, tx pgx.Tx, req SubmitRequest, res reservation) error {
	if res.kind == "CASH" {
		b, err := balances(ctx, tx, req.AccountID)
		if err != nil {
			return err
		}
		if b.BuyingPower < money.Minor(res.amount) {
			return reject("INSUFFICIENT_BUYING_POWER",
				"need %d, have %d", res.amount, b.BuyingPower)
		}
		return nil
	}

	var held, reserved int64
	err := tx.QueryRow(ctx, `
		SELECT coalesce((SELECT qty FROM positions WHERE account_id=$1 AND symbol=$2), 0),
		       coalesce((SELECT sum(reserved - consumed - released) FROM reservations
		                  WHERE account_id=$1 AND kind='SHARES' AND symbol=$2), 0)`,
		req.AccountID, req.Symbol).Scan(&held, &reserved)
	if err != nil {
		return fmt.Errorf("share availability: %w", err)
	}
	if held-reserved < res.amount {
		return reject("INSUFFICIENT_SHARES", "need %d, have %d available", res.amount, held-reserved)
	}
	return nil
}

func insertOrder(ctx context.Context, tx pgx.Tx, v OrderView, res *reservation) error {
	var limitArg, refArg any
	if v.Type == book.Limit {
		limitArg = int64(v.LimitPrice)
	}
	if v.RefPrice > 0 {
		refArg = int64(v.RefPrice)
	}
	var rejectArg any
	if v.RejectReason != "" {
		rejectArg = v.RejectReason
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO orders (id, account_id, symbol, side, type, tif, qty, limit_price,
		                    ref_price, status, reject_reason, client_order_id, venue)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		v.ID, v.AccountID, v.Symbol, v.Side.String(), v.Type.String(), v.TIF.String(),
		int64(v.Qty), limitArg, refArg, v.Status, rejectArg, v.ClientOrderID,
		nullIfEmpty(v.Venue)); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	if res == nil {
		return nil
	}
	var symArg, priceArg any
	if res.symbol != "" {
		symArg = res.symbol
	}
	if res.unitPrice > 0 {
		priceArg = res.unitPrice
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO reservations (order_id, account_id, kind, symbol, reserved, unit_price, fee_bps)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		v.ID, v.AccountID, res.kind, symArg, res.amount, priceArg, res.feeBps); err != nil {
		return fmt.Errorf("insert reservation: %w", err)
	}
	return nil
}

func loadOrderByClientID(ctx context.Context, q querier, account uuid.UUID, clientID string) (OrderView, bool, error) {
	var (
		v                    OrderView
		side, typ, tif       string
		limitPrice, refPrice *int64
		rejectReason         *string
	)
	err := q.QueryRow(ctx, `
		SELECT id, account_id, symbol, side, type, tif, qty, limit_price, ref_price,
		       status, reject_reason, filled_qty, filled_notional, client_order_id
		  FROM orders WHERE account_id = $1 AND client_order_id = $2`,
		account, clientID).Scan(&v.ID, &v.AccountID, &v.Symbol, &side, &typ, &tif,
		&v.Qty, &limitPrice, &refPrice, &v.Status, &rejectReason,
		&v.FilledQty, &v.FilledNotional, &v.ClientOrderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderView{}, false, nil
	}
	if err != nil {
		return OrderView{}, false, fmt.Errorf("load order: %w", err)
	}

	v.Side, v.Type, v.TIF = parseSide(side), parseType(typ), parseTIF(tif)
	if limitPrice != nil {
		v.LimitPrice = money.Minor(*limitPrice)
	}
	if refPrice != nil {
		v.RefPrice = money.Minor(*refPrice)
	}
	if rejectReason != nil {
		v.RejectReason = *rejectReason
	}
	return v, true, nil
}

// Order reads an order's current state.
func (e *Engine) Order(ctx context.Context, id uuid.UUID) (OrderView, error) {
	var (
		v                    OrderView
		side, typ, tif       string
		limitPrice, refPrice *int64
		rejectReason         *string
	)
	err := e.pool.QueryRow(ctx, `
		SELECT id, account_id, symbol, side, type, tif, qty, limit_price, ref_price,
		       status, reject_reason, filled_qty, filled_notional, client_order_id
		  FROM orders WHERE id = $1`, id).
		Scan(&v.ID, &v.AccountID, &v.Symbol, &side, &typ, &tif, &v.Qty, &limitPrice,
			&refPrice, &v.Status, &rejectReason, &v.FilledQty, &v.FilledNotional, &v.ClientOrderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderView{}, reject("NO_SUCH_ORDER", "%s", id)
	}
	if err != nil {
		return OrderView{}, err
	}
	v.Side, v.Type, v.TIF = parseSide(side), parseType(typ), parseTIF(tif)
	if limitPrice != nil {
		v.LimitPrice = money.Minor(*limitPrice)
	}
	if refPrice != nil {
		v.RefPrice = money.Minor(*refPrice)
	}
	if rejectReason != nil {
		v.RejectReason = *rejectReason
	}
	return v, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func parseSide(s string) book.Side {
	if s == "SELL" {
		return book.Sell
	}
	return book.Buy
}

func parseType(s string) book.Type {
	if s == "LIMIT" {
		return book.Limit
	}
	return book.Market
}

func parseTIF(s string) book.TIF {
	if s == "GTC" {
		return book.GTC
	}
	return book.IOC
}

// FillRow is one execution an order participated in.
type FillRow struct {
	FillID  uuid.UUID
	Symbol  string
	BookSeq int64
	Price   money.Minor
	Qty     money.Qty
	AsTaker bool
}

// OrderFills lists the executions an order participated in, oldest first.
//
// The core learns about fills from the outcome stream rather than from a return value, so
// this reads what settled rather than what was predicted — which is the honest thing for a
// client to be shown once matching happens somewhere else.
func (e *Engine) OrderFills(ctx context.Context, orderID uuid.UUID) ([]FillRow, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT fill_id, symbol, book_seq, price, qty, taker_order_id = $1
		  FROM fills
		 WHERE taker_order_id = $1 OR maker_order_id = $1
		 ORDER BY shard_id, symbol, book_seq`, orderID)
	if err != nil {
		return nil, fmt.Errorf("order fills: %w", err)
	}
	defer rows.Close()

	var out []FillRow
	for rows.Next() {
		var f FillRow
		if err := rows.Scan(&f.FillID, &f.Symbol, &f.BookSeq, &f.Price, &f.Qty, &f.AsTaker); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
