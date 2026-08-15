package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/events"
)

// RequestCancel records the client's intent and enqueues it for the book.
//
// Two things happen in one transaction and neither can happen without the other: the order
// moves to PENDING_CANCEL, and an outbox row is written that will become the book's copy of
// the request.
//
// It deliberately does NOT release the reservation. The book has not seen this request
// yet, and an in-flight fill can still consume it; releasing here would hand the buying
// power back while the money is still committed, and the account could then overdraw
// against credit it was given twice. PENDING_CANCEL names exactly that interval — the
// order is neither live nor terminal, and saying so is more honest than either lying to
// the client or blocking until the book answers.
//
// Phase 2 needed a recovery sweep for cancels stranded between the request and the
// verdict. It no longer does: the outbox row is durable, so a crash anywhere after this
// commits still ends with the request published and answered. Putting the request in the
// same transaction as the state change is what removed a whole recovery path.
func (e *Engine) RequestCancel(ctx context.Context, orderID uuid.UUID) (string, error) {
	var status string
	err := e.inTx(ctx, func(tx pgx.Tx) error {
		var (
			account uuid.UUID
			symbol  string
		)
		err := tx.QueryRow(ctx,
			`SELECT account_id, symbol, status::text FROM orders WHERE id = $1`,
			orderID).Scan(&account, &symbol, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return reject("NO_SUCH_ORDER", "%s", orderID)
		}
		if err != nil {
			return err
		}
		if _, err := lockAccount(ctx, tx, account); err != nil {
			return err
		}

		// Re-read under the lock: a fill may have landed between the two statements.
		if err := tx.QueryRow(ctx,
			`SELECT status::text FROM orders WHERE id = $1`, orderID).Scan(&status); err != nil {
			return err
		}
		if isTerminal(status) {
			return nil // the race was already lost, and that is an answer
		}
		if status == "PENDING_CANCEL" {
			return nil // already asked; asking twice would just produce a second verdict
		}

		if _, err := tx.Exec(ctx,
			`UPDATE orders SET status = 'PENDING_CANCEL', version = version + 1 WHERE id = $1`,
			orderID); err != nil {
			return err
		}
		status = "PENDING_CANCEL"

		payload, err := events.Encode(events.TypeCancelRequested, events.CancelRequested{
			OrderID:   orderID,
			AccountID: account,
			Symbol:    symbol,
		})
		if err != nil {
			return err
		}
		// Keyed by SYMBOL and carried on the same topic as orders, which is what lets the
		// book linearize this cancel against the orders that might cross it. On two
		// topics there would be no defined order between them, and replay could resolve
		// the race differently than the original run did.
		if _, err := tx.Exec(ctx,
			`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
			eventlog.TopicOrdersInbound, symbol, payload); err != nil {
			return fmt.Errorf("enqueue cancel request: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

// Cancel is RequestCancel. The name is kept because that is what a client is doing; what
// it returns is the order's status right now, which for a live order is PENDING_CANCEL.
// The verdict arrives on the outcome stream.
func (e *Engine) Cancel(ctx context.Context, orderID uuid.UUID) (string, error) {
	return e.RequestCancel(ctx, orderID)
}

// PendingCancels returns orders awaiting the book's verdict, for the status page.
func (e *Engine) PendingCancels(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := e.pool.Query(ctx,
		`SELECT id FROM orders WHERE status = 'PENDING_CANCEL' ORDER BY seq_no`)
	if err != nil {
		return nil, fmt.Errorf("pending cancels: %w", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func isTerminal(status string) bool {
	switch status {
	case "FILLED", "CANCELLED", "REJECTED", "EXPIRED":
		return true
	}
	return false
}
