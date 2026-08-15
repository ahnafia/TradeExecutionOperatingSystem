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

// ConsumerGroup names the core's position in the outcome stream.
const ConsumerGroup = "trading-core"

// OutcomeConsumer applies the matching engine's decisions to the ledger.
//
// This is the second half of the exactly-once story. The log delivers at least once —
// the relay can republish, the matching engine can regenerate fills after a crash — and
// this turns that into an effect that happens once, by advancing the consumer offset in
// the SAME transaction as the state change it authorizes. There is no moment where one
// has advanced and the other has not, so there is no recovery case to reason about
// separately: whatever committed, committed together.
type OutcomeConsumer struct {
	eng       *Engine
	log       eventlog.Log
	partition int32
	reader    eventlog.Reader
	next      int64
	batch     int
}

// NewOutcomeConsumer positions a consumer at its durable offset for one partition.
func (e *Engine) NewOutcomeConsumer(ctx context.Context, log eventlog.Log, partition int32, batch int) (*OutcomeConsumer, error) {
	if batch <= 0 {
		batch = 256
	}
	next, err := e.loadOffset(ctx, eventlog.TopicOrdersOutcomes, partition)
	if err != nil {
		return nil, err
	}
	reader, err := log.Reader(eventlog.TopicOrdersOutcomes, partition, next)
	if err != nil {
		return nil, err
	}
	return &OutcomeConsumer{eng: e, log: log, partition: partition, reader: reader, next: next, batch: batch}, nil
}

func (e *Engine) loadOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	var next int64
	err := e.pool.QueryRow(ctx, `
		SELECT next_offset FROM consumer_offsets
		 WHERE consumer_group = $1 AND topic = $2 AND partition = $3`,
		ConsumerGroup, topic, partition).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load offset: %w", err)
	}
	return next, nil
}

// PumpOnce applies whatever is currently available, returning how many records it handled.
func (c *OutcomeConsumer) PumpOnce(ctx context.Context) (int, error) {
	recs, err := c.reader.Fetch(ctx, c.batch)
	if err != nil {
		return 0, err
	}
	for i, rec := range recs {
		if err := c.eng.applyOutcome(ctx, c.partition, rec); err != nil {
			return i, err
		}
		c.next = rec.Offset + 1
	}
	return len(recs), nil
}

// Run applies outcomes until the context is cancelled.
func (c *OutcomeConsumer) Run(ctx context.Context, idle time.Duration, onErr func(error)) {
	for {
		n, err := c.PumpOnce(ctx)
		if err != nil && ctx.Err() == nil && onErr != nil {
			onErr(err)
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
	}
}

// Close releases the reader.
func (c *OutcomeConsumer) Close() error { return c.reader.Close() }

// Offset reports the next offset this consumer will read.
func (c *OutcomeConsumer) Offset() int64 { return c.next }

// applyOutcome handles one record in one transaction, offset included.
func (e *Engine) applyOutcome(ctx context.Context, partition int32, rec eventlog.Record) error {
	env, err := events.Decode(rec.Value)
	if err != nil {
		// Advancing past a record we cannot read would drop a fill and leave the ledger
		// short with no trace of why. Stop instead, loudly.
		return fmt.Errorf("outcomes partition %d offset %d: %w", partition, rec.Offset, err)
	}

	switch env.Type {
	case events.TypeFillHalf:
		msg, err := events.Into[events.FillHalf](env)
		if err != nil {
			return err
		}
		return e.applyFillHalf(ctx, partition, rec.Offset, msg)

	case events.TypeDisposition:
		msg, err := events.Into[events.Disposition](env)
		if err != nil {
			return err
		}
		return e.applyDisposition(ctx, partition, rec.Offset, msg)

	case events.TypeCancelOutcome:
		msg, err := events.Into[events.CancelOutcome](env)
		if err != nil {
			return err
		}
		return e.applyCancelOutcome(ctx, partition, rec.Offset, msg)

	default:
		return fmt.Errorf("outcomes partition %d offset %d: unexpected type %q",
			partition, rec.Offset, env.Type)
	}
}

// venueOrDefault fills in the venue for records produced before venues existed. Replaying
// an old log must not fail on a field that was not in the schema when it was written.
func venueOrDefault(v string) string {
	if v == "" {
		return book.DefaultVenue
	}
	return v
}

// commitOffset advances the consumer position. Always called inside the same transaction
// as the state change it authorizes — that co-location is the entire point.
func commitOffset(ctx context.Context, tx pgx.Tx, topic string, partition int32, next int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (consumer_group, topic, partition)
		DO UPDATE SET next_offset = EXCLUDED.next_offset, updated_at = now()`,
		ConsumerGroup, topic, partition, next)
	if err != nil {
		return fmt.Errorf("commit offset: %w", err)
	}
	return nil
}

// applyFillHalf settles one side of one execution.
func (e *Engine) applyFillHalf(ctx context.Context, partition int32, offset int64, msg events.FillHalf) error {
	notional, ok := money.Notional(money.Qty(msg.Qty), money.Minor(msg.Price))
	if !ok {
		return fmt.Errorf("fill %s: notional overflow", msg.FillID)
	}

	err := e.inTx(ctx, func(tx pgx.Tx) error {
		// The offset advances whatever happens below — including when the event turns out
		// to be a duplicate. A duplicate that did not advance the offset would be
		// redelivered forever.
		if err := commitOffset(ctx, tx, eventlog.TopicOrdersOutcomes, partition, offset+1); err != nil {
			return err
		}

		seq, err := lockAccount(ctx, tx, msg.AccountID)
		if err != nil {
			return err
		}

		// One row per fill, inserted by whichever half arrives first. It is what the
		// reconciler pairs the two halves against.
		if _, err := tx.Exec(ctx, `
			INSERT INTO fills (fill_id, shard_id, venue, symbol, book_seq, price, qty,
			                   taker_order_id, maker_order_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (fill_id) DO NOTHING`,
			msg.FillID, msg.ShardID, venueOrDefault(msg.Venue), msg.Symbol,
			int64(msg.BookSeq), msg.Price, msg.Qty,
			msg.TakerOrderID, msg.MakerOrderID); err != nil {
			return fmt.Errorf("record fill: %w", err)
		}

		// Fees are charged on the order's CUMULATIVE notional and billed as the delta.
		// Rounding each fill's fee up independently would let a many-fill order's fees
		// exceed the headroom reserved at accept time; telescoping keeps the total equal
		// to a single rounded-up charge on the whole.
		var priorQty, priorNotional int64
		if err := tx.QueryRow(ctx,
			`SELECT filled_qty, filled_notional FROM orders WHERE id = $1 FOR UPDATE`,
			msg.OrderID).Scan(&priorQty, &priorNotional); err != nil {
			return fmt.Errorf("load order for fill: %w", err)
		}
		feeBps := e.cfg.MakerFeeBps
		if msg.Side == "TAKER" {
			feeBps = e.cfg.TakerFeeBps
		}
		feeBefore, ok1 := money.FeeBps(money.Minor(priorNotional), feeBps)
		feeAfter, ok2 := money.FeeBps(money.Minor(priorNotional)+notional, feeBps)
		if !ok1 || !ok2 {
			return fmt.Errorf("fill %s: fee overflow", msg.FillID)
		}
		fee := feeAfter - feeBefore

		legs := halfFillLegs(msg.Symbol, money.Qty(msg.Qty), notional, fee, msg.Buying)
		orderID, fillID := msg.OrderID, msg.FillID
		err = writeTxn(ctx, tx, txnSpec{
			account: msg.AccountID, seq: seq, kind: "FILL_HALF", eventID: msg.EventID,
			orderID: &orderID, fillID: &fillID, legs: legs,
		})
		if errors.Is(err, ErrDuplicateEvent) {
			// Already applied. The offset committed above still advances, which is how a
			// redelivered record stops being redelivered.
			return nil
		}
		if err != nil {
			return err
		}

		if err := applyPositionDelta(ctx, tx, msg.AccountID, msg.Symbol,
			money.Qty(msg.Qty), notional, fee, msg.Buying); err != nil {
			return err
		}
		if err := settleReservation(ctx, tx, msg.OrderID, money.Qty(priorQty),
			money.Qty(msg.Qty), notional, fee, msg.Buying); err != nil {
			return err
		}
		return advanceOrderFill(ctx, tx, msg.OrderID, money.Qty(msg.Qty), notional)
	})
	if err != nil {
		return fmt.Errorf("apply %s half of fill %s: %w", msg.Side, msg.FillID, err)
	}
	e.obs.ObserveFill(money.Qty(msg.Qty), notional)
	return nil
}

// applyDisposition settles what the book did with an order's unfilled remainder.
//
// It arrives after every fill for that order, because both are keyed by the same account
// and the matching engine produced them in that order. That is what makes it safe to
// decide a final status here: everything that could still change filled_qty has already
// been applied.
func (e *Engine) applyDisposition(ctx context.Context, partition int32, offset int64, msg events.Disposition) error {
	return e.inTx(ctx, func(tx pgx.Tx) error {
		if err := commitOffset(ctx, tx, eventlog.TopicOrdersOutcomes, partition, offset+1); err != nil {
			return err
		}
		if _, err := lockAccount(ctx, tx, msg.AccountID); err != nil {
			return err
		}

		var status string
		switch msg.Disposition {
		case "COMPLETE":
			status = "FILLED"
		case "RESTED":
			status = "ACCEPTED"
		default:
			status = "CANCELLED"
		}

		var reasonArg any
		if msg.Reason != "" {
			reasonArg = msg.Reason
		}

		// RESTED with partial fills is PARTIALLY_FILLED, which the CASE below derives
		// from filled_qty rather than trusting the message — the fills are the authority
		// on how much filled, and they are already applied.
		if _, err := tx.Exec(ctx, `
			UPDATE orders
			   SET status = CASE
			         WHEN $2 = 'ACCEPTED' AND filled_qty > 0 THEN 'PARTIALLY_FILLED'::order_status
			         ELSE $2::order_status END,
			       reject_reason = coalesce(reject_reason, $3),
			       version = version + 1
			 WHERE id = $1
			   AND status NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED','PENDING_CANCEL')`,
			msg.OrderID, status, reasonArg); err != nil {
			return fmt.Errorf("apply disposition: %w", err)
		}
		return releaseIfTerminal(ctx, tx, msg.OrderID)
	})
}

// applyCancelOutcome settles the book's verdict on a cancel request, and only now
// releases the reservation.
func (e *Engine) applyCancelOutcome(ctx context.Context, partition int32, offset int64, msg events.CancelOutcome) error {
	err := e.inTx(ctx, func(tx pgx.Tx) error {
		if err := commitOffset(ctx, tx, eventlog.TopicOrdersOutcomes, partition, offset+1); err != nil {
			return err
		}
		if _, err := lockAccount(ctx, tx, msg.AccountID); err != nil {
			return err
		}

		if msg.Cancelled {
			// FILLED is terminal and is never overwritten: an order that completed before
			// the book saw the cancel stays filled, and the client is told the cancel lost.
			if _, err := tx.Exec(ctx, `
				UPDATE orders
				   SET status = 'CANCELLED',
				       reject_reason = coalesce(reject_reason, $2),
				       version = version + 1
				 WHERE id = $1 AND status NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED')`,
				msg.OrderID, msg.Reason); err != nil {
				return err
			}
		} else {
			// The book had nothing to remove. Settle the order out of PENDING_CANCEL to
			// whatever its fills say it is; without this it would hold its reservation
			// forever waiting for a verdict that has already been given.
			if _, err := tx.Exec(ctx, `
				UPDATE orders
				   SET status = CASE WHEN filled_qty >= qty THEN 'FILLED'::order_status
				                     ELSE 'CANCELLED'::order_status END,
				       reject_reason = coalesce(reject_reason, $2),
				       version = version + 1
				 WHERE id = $1 AND status = 'PENDING_CANCEL'`,
				msg.OrderID, msg.Reason); err != nil {
				return err
			}
		}
		return releaseIfTerminal(ctx, tx, msg.OrderID)
	})
	if err != nil {
		return err
	}
	e.obs.ObserveCancel(msg.Reason)
	return nil
}

// --- ledger mechanics shared by the paths above ----------------------------

// halfFillLegs builds one side of a fill as a self-balancing double entry.
//
// It balances in each unit of account separately: cents in one dimension, share
// millionths in the other. Summing the two together would be adding quantities with
// different meanings, so the deferred trigger groups by unit rather than by transaction
// alone.
//
//	BUY   cash: −(notional+fee)  fees: +fee   clearing: +notional   → 0
//	      shrs: +qty                          clearing: −qty        → 0
//	SELL  cash: +(notional−fee)  fees: +fee   clearing: −notional   → 0
//	      shrs: −qty                          clearing: +qty        → 0
//
// The clearing legs of the two halves are equal and opposite, so summing every clearing
// account across every partition gives zero once both halves have settled.
func halfFillLegs(symbol string, qty money.Qty, notional, fee money.Minor, buying bool) []leg {
	sign := int64(1)
	if !buying {
		sign = -1
	}
	cash := -sign*int64(notional) - int64(fee)
	return []leg{
		{kind: "CASH", amount: cash},
		{kind: "FEES", amount: int64(fee)},
		{kind: "CLEARING", amount: sign * int64(notional)},
		{kind: "POSITION", symbol: symbol, amount: sign * int64(qty)},
		{kind: "CLEARING", symbol: symbol, amount: -sign * int64(qty)},
	}
}

func applyPositionDelta(ctx context.Context, tx pgx.Tx, account uuid.UUID, symbol string,
	qty money.Qty, notional, fee money.Minor, buying bool) error {

	var p positionState
	err := tx.QueryRow(ctx, `
		INSERT INTO positions (account_id, symbol) VALUES ($1, $2)
		ON CONFLICT (account_id, symbol) DO UPDATE SET symbol = EXCLUDED.symbol
		RETURNING qty, cost_basis, realized_pnl`, account, symbol).Scan(&p.Qty, &p.Basis, &p.Realized)
	if err != nil {
		return fmt.Errorf("load position: %w", err)
	}

	// The same arithmetic the replayer runs, called from the same place, so the two
	// cannot drift. `cost` and `proceeds` are exactly the transaction's cash leg, which is
	// what makes this reconstructible from the ledger alone.
	if buying {
		p.applyBuy(qty, notional+fee)
	} else {
		p.applySell(qty, notional-fee)
	}

	_, err = tx.Exec(ctx, `
		UPDATE positions SET qty = $3, cost_basis = $4, realized_pnl = $5
		WHERE account_id = $1 AND symbol = $2`,
		account, symbol, int64(p.Qty), int64(p.Basis), int64(p.Realized))
	return err
}

// settleReservation consumes what a fill actually cost and releases the headroom that
// slice of the order no longer needs.
//
// The release is the difference between the reservation owed at the old fill level and at
// the new one — using the price and fee rate stored on the reservation, not current
// config, which may have changed since the order was accepted.
func settleReservation(ctx context.Context, tx pgx.Tx, orderID uuid.UUID,
	priorQty, fillQty money.Qty, notional, fee money.Minor, buying bool) error {

	var (
		reserved, consumed, released, feeBps int64
		unitPrice                            *int64
	)
	err := tx.QueryRow(ctx, `
		SELECT reserved, consumed, released, fee_bps, unit_price
		  FROM reservations WHERE order_id = $1 FOR UPDATE`, orderID).
		Scan(&reserved, &consumed, &released, &feeBps, &unitPrice)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("no reservation for order %s", orderID)
	}
	if err != nil {
		return fmt.Errorf("load reservation for %s: %w", orderID, err)
	}

	cost := int64(fillQty)
	var release int64
	if buying {
		cost = int64(notional) + int64(fee)

		if unitPrice != nil {
			owed := func(q money.Qty) int64 {
				n, ok := money.NotionalCeil(q, money.Minor(*unitPrice))
				if !ok {
					return 0
				}
				f, ok := money.FeeBps(n, feeBps)
				if !ok {
					return int64(n)
				}
				return int64(n) + int64(f)
			}
			release = owed(priorQty+fillQty) - owed(priorQty) - cost
		}
	}

	// Clamp. Ceiling rounding at the two cumulative levels can differ from the rounding of
	// the slice by a single minor unit, and the remaining headroom is swept at terminal
	// anyway — so a slightly conservative release is free, while an over-release would
	// trip the row's CHECK and fail the fill.
	headroom := reserved - consumed - released - cost
	if release > headroom {
		release = headroom
	}
	if release < 0 {
		release = 0
	}

	// The CHECK on this row is what turns "we under-reserved" from a silent overdraft into
	// a loud, immediate failure at the moment it happens.
	_, err = tx.Exec(ctx,
		`UPDATE reservations SET consumed = consumed + $2, released = released + $3 WHERE order_id = $1`,
		orderID, cost, release)
	if err != nil {
		return fmt.Errorf("settle reservation for %s: %w", orderID, err)
	}
	return nil
}

func advanceOrderFill(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, qty money.Qty, notional money.Minor) error {
	// A partial fill must not clear a pending cancel. Completing the order does: FILLED is
	// terminal, and it means the fill won the race against the cancel outright.
	_, err := tx.Exec(ctx, `
		UPDATE orders
		   SET filled_qty = filled_qty + $2,
		       filled_notional = filled_notional + $3,
		       status = CASE WHEN filled_qty + $2 >= qty     THEN 'FILLED'::order_status
		                     WHEN status = 'PENDING_CANCEL'  THEN 'PENDING_CANCEL'::order_status
		                     ELSE 'PARTIALLY_FILLED'::order_status END,
		       version = version + 1
		 WHERE id = $1`, orderID, int64(qty), int64(notional))
	if err != nil {
		return fmt.Errorf("advance order: %w", err)
	}
	return releaseIfTerminal(ctx, tx, orderID)
}

// releaseIfTerminal frees the unspent part of a reservation once an order can no longer
// consume it. Buying power is derived from the reservations table, so this is the only
// thing that gives the money back.
func releaseIfTerminal(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		UPDATE reservations r
		   SET released = r.reserved - r.consumed
		  FROM orders o
		 WHERE r.order_id = o.id AND o.id = $1
		   AND o.status IN ('FILLED','CANCELLED','REJECTED','EXPIRED')
		   AND r.consumed + r.released <> r.reserved`, orderID)
	return err
}
