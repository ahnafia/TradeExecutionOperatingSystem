package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Invariants is the continuous verification block. Every field must be zero.
//
// Two of these deserve their labels read carefully, because in a single-database Phase 1
// deployment they cannot fail for arithmetic reasons rather than because the system is
// correct:
//
//   - UnbalancedTxns is enforced by a deferred trigger at COMMIT, so a non-zero value
//     here means the trigger is missing or was disabled, not that a fill went wrong.
//   - CashConservationDelta sums postings that a per-transaction balance constraint has
//     already forced to zero. It is a corruption check.
//
// The ones that genuinely detect a bug in this code are the projection comparisons
// (positions and balances vs. the ledger that justifies them), OrphanedFillHalves, and
// the clearing pairing — because those compare things written by different statements,
// and in the partitioned system, by different processes against different databases.
type Invariants struct {
	UnbalancedTxns         int64
	InFlightFillHalves     int64 // informational: settling right now, NOT a violation
	OrphanedFillHalves     int64
	UnpairedClearingUnits  int64
	CashConservationDelta  int64
	ShareConservation      map[string]int64 // symbol → (held − seeded); all must be 0
	PositionLedgerMismatch int64
	BalanceLedgerMismatch  int64
	NegativeCashAccounts   int64
	OverdrawnReservations  int64
}

// OK reports whether every invariant holds.
func (i Invariants) OK() bool {
	if i.UnbalancedTxns != 0 || i.OrphanedFillHalves != 0 || i.UnpairedClearingUnits != 0 ||
		i.CashConservationDelta != 0 || i.PositionLedgerMismatch != 0 ||
		i.BalanceLedgerMismatch != 0 || i.NegativeCashAccounts != 0 ||
		i.OverdrawnReservations != 0 {
		return false
	}
	for _, v := range i.ShareConservation {
		if v != 0 {
			return false
		}
	}
	return true
}

// String renders the verification block that every chaos run ends with.
func (i Invariants) String() string {
	s := fmt.Sprintf(""+
		"Unbalanced txns:           %d\n"+
		"In-flight fill halves:     %d  (settling now; not a violation)\n"+
		"Orphaned fill halves:      %d\n"+
		"Unpaired clearing units:   %d\n"+
		"Cash conservation delta:   %d\n"+
		"Position/ledger mismatch:  %d\n"+
		"Balance/ledger mismatch:   %d\n"+
		"Negative cash accounts:    %d\n"+
		"Overdrawn reservations:    %d\n",
		i.UnbalancedTxns, i.InFlightFillHalves, i.OrphanedFillHalves, i.UnpairedClearingUnits,
		i.CashConservationDelta, i.PositionLedgerMismatch, i.BalanceLedgerMismatch,
		i.NegativeCashAccounts, i.OverdrawnReservations)

	syms := make([]string, 0, len(i.ShareConservation))
	for k := range i.ShareConservation {
		syms = append(syms, k)
	}
	sort.Strings(syms)
	for _, sym := range syms {
		s += fmt.Sprintf("Share conservation %-8s %d\n", sym+":", i.ShareConservation[sym])
	}
	return s
}

// CheckInvariants runs the full verification pass.
func (e *Engine) CheckInvariants(ctx context.Context) (Invariants, error) {
	var inv Invariants
	inv.ShareConservation = map[string]int64{}

	scalars := []struct {
		dst   *int64
		name  string
		query string
	}{
		{&inv.UnbalancedTxns, "unbalanced txns", `
			SELECT count(*) FROM (
			  SELECT p.txn_id, la.unit
			    FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
			   GROUP BY p.txn_id, la.unit
			  HAVING sum(p.amount) <> 0) x`},

		// Every fill must apply exactly twice: once in the taker's partition and once in
		// the maker's. One half means money is stranded in a clearing account.
		//
		// The age filter is not slack, it is the guarantee being stated correctly. A fill
		// is two transactions in two partitions with no transaction spanning them, so an
		// unpaired half is the NORMAL state for the moment between them. Alarming on that
		// would fire continuously under load and teach whoever is on call to ignore the
		// one signal that matters. What is not normal is a half that stays unpaired, and
		// that is what this counts.
		{&inv.OrphanedFillHalves, "orphaned fill halves", `
			SELECT count(*) FROM fills f
			LEFT JOIN (SELECT fill_id, count(*) AS c FROM transactions
			            WHERE fill_id IS NOT NULL GROUP BY fill_id) t ON t.fill_id = f.fill_id
			WHERE coalesce(t.c, 0) <> 2
			  AND f.created_at < now() - make_interval(secs => $1)`},

		// Informational twin of the above: halves still inside the settle window. Expected
		// to be non-zero on a busy system, and reported so that a rising value is visible
		// as backpressure before it becomes an orphan.
		{&inv.InFlightFillHalves, "in-flight fill halves", `
			SELECT count(*) FROM fills f
			LEFT JOIN (SELECT fill_id, count(*) AS c FROM transactions
			            WHERE fill_id IS NOT NULL GROUP BY fill_id) t ON t.fill_id = f.fill_id
			WHERE coalesce(t.c, 0) <> 2
			  AND f.created_at >= now() - make_interval(secs => $1)`},

		// The two halves' clearing legs are equal and opposite, so every unit of account
		// nets to zero across all accounts — over fills that have finished settling.
		// Half-settled fills are excluded for the same reason as above: their imbalance is
		// the open clearing position, which is accounted for, not lost.
		{&inv.UnpairedClearingUnits, "unpaired clearing", `
			SELECT count(*) FROM (
			  SELECT la.unit FROM postings p
			    JOIN ledger_accounts la ON la.id = p.ledger_acct
			    JOIN transactions t ON t.id = p.txn_id
			   WHERE la.kind = 'CLEARING' AND (t.fill_id IS NULL OR t.fill_id IN (
			           SELECT fill_id FROM transactions WHERE fill_id IS NOT NULL
			            GROUP BY fill_id HAVING count(*) = 2))
			   GROUP BY la.unit HAVING sum(p.amount) <> 0) x`},

		// Money in the system equals money deposited into it.
		{&inv.CashConservationDelta, "cash conservation", `
			SELECT coalesce(sum(CASE WHEN la.kind IN ('CASH','FEES','CLEARING') THEN p.amount
			                         WHEN la.kind = 'EXTERNAL' THEN p.amount
			                         ELSE 0 END), 0)
			  FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
			 WHERE la.unit = 'USD'`},

		// Projection vs. the postings that justify it. These are written by different
		// statements in the same transaction, so a mismatch is a real application bug.
		// Compared over the UNION of both sides' keys, so a position with no ledger
		// account and a ledger account with no position are both caught. The comparison
		// is restricted to POSITION accounts: an account's CLEARING and EXTERNAL share
		// legs are share-denominated too, and folding them in here would compare a
		// position against a counterparty leg that was never meant to equal it.
		{&inv.PositionLedgerMismatch, "position/ledger", `
			SELECT count(*) FROM (
			  SELECT coalesce(pos.qty, 0) AS projected, coalesce(led.total, 0) AS ledger
			    FROM (SELECT account_id, symbol FROM positions
			          UNION
			          SELECT account_id, symbol FROM ledger_accounts WHERE kind = 'POSITION') k
			    LEFT JOIN positions pos
			           ON pos.account_id = k.account_id AND pos.symbol = k.symbol
			    LEFT JOIN (SELECT la.account_id, la.symbol, sum(p.amount) AS total
			                 FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
			                WHERE la.kind = 'POSITION'
			                GROUP BY 1, 2) led
			           ON led.account_id = k.account_id AND led.symbol = k.symbol) x
			 WHERE projected <> ledger`},

		{&inv.BalanceLedgerMismatch, "balance/ledger", `
			SELECT count(*) FROM (
			  SELECT b.account_id
			    FROM account_balances b
			    LEFT JOIN ledger_accounts la ON la.account_id = b.account_id AND la.kind = 'CASH'
			    LEFT JOIN postings p ON p.ledger_acct = la.id
			   GROUP BY b.account_id, b.cash
			  HAVING b.cash <> coalesce(sum(p.amount), 0)) x`},

		{&inv.NegativeCashAccounts, "negative cash", `
			SELECT count(*) FROM account_balances WHERE cash < 0`},

		{&inv.OverdrawnReservations, "overdrawn reservations", `
			SELECT count(*) FROM reservations WHERE consumed + released > reserved OR consumed < 0`},
	}

	settleSecs := e.cfg.SettleWindow.Seconds()
	for _, s := range scalars {
		var err error
		if strings.Contains(s.query, "$1") {
			err = e.pool.QueryRow(ctx, s.query, settleSecs).Scan(s.dst)
		} else {
			err = e.pool.QueryRow(ctx, s.query).Scan(s.dst)
		}
		if err != nil {
			return inv, fmt.Errorf("invariant %q: %w", s.name, err)
		}
	}

	// Shares are conserved per symbol: what accounts hold equals what was seeded in.
	// Held is summed from the LEDGER rather than the positions projection, so the
	// half-settled filter applies here too: this aggregates across accounts, which is
	// exactly where the window between a fill's two transactions becomes visible.
	// (Projection versus ledger is checked separately, per account, where no such window
	// exists because both move in one transaction.)
	rows, err := e.pool.Query(ctx, `
		SELECT u.sym, coalesce(h.held, 0) - coalesce(s.seeded, 0)
		  FROM (SELECT DISTINCT symbol AS sym FROM ledger_accounts WHERE symbol IS NOT NULL) u
		  LEFT JOIN (SELECT la.symbol, sum(p.amount) AS held
		               FROM postings p
		               JOIN ledger_accounts la ON la.id = p.ledger_acct
		               JOIN transactions t ON t.id = p.txn_id
		              WHERE la.kind = 'POSITION'
		                AND (t.fill_id IS NULL OR t.fill_id IN (
		                       SELECT fill_id FROM transactions WHERE fill_id IS NOT NULL
		                        GROUP BY fill_id HAVING count(*) = 2))
		              GROUP BY la.symbol) h ON h.symbol = u.sym
		  LEFT JOIN (SELECT la.symbol, -sum(p.amount) AS seeded
		               FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
		              WHERE la.kind = 'EXTERNAL' AND la.symbol IS NOT NULL
		              GROUP BY la.symbol) s ON s.symbol = u.sym`)
	if err != nil {
		return inv, fmt.Errorf("invariant %q: %w", "share conservation", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sym string
		var delta int64
		if err := rows.Scan(&sym, &delta); err != nil {
			return inv, err
		}
		inv.ShareConservation[sym] = delta
	}
	return inv, rows.Err()
}

// ReservationBound reports the worst case over every order of (consumed − reserved).
//
// This is the "realized cost never exceeds what was reserved" property, measured rather
// than assumed. A positive result means the collar failed to bound an execution, which
// is the precise failure that lets cash go negative.
func (e *Engine) ReservationBound(ctx context.Context) (int64, error) {
	var worst int64
	err := e.pool.QueryRow(ctx,
		`SELECT coalesce(max(consumed - reserved), 0) FROM reservations`).Scan(&worst)
	return worst, err
}

// ReconcileSlice is one partition's contribution to the cluster-wide invariants.
//
// It exists because after Phase 4 the terms of a conservation check live in different
// databases, so the check cannot be a query. Each partition reports its aggregates and the
// reconciler adds them up. The shapes here are chosen so that addition is all the
// reconciler has to do — anything requiring a join across partitions would have to be
// restructured into something that does not.
type ReconcileSlice struct {
	// ClearingByUnit is this partition's clearing balance per unit of account. Summed
	// across partitions it is zero once every fill has both halves settled.
	ClearingByUnit map[string]int64

	// SharesBySymbol is every share-denominated posting in this partition, per symbol:
	// positions, clearing legs, and issuance together. Summed across partitions it is
	// zero, because shares are only ever moved between accounts or in from EXTERNAL.
	SharesBySymbol map[string]int64

	// CashDelta is every currency-denominated posting here. Summed across partitions it
	// is zero: money enters only through EXTERNAL, which is itself a posting.
	CashDelta int64

	// FillHalves counts how many halves of each fill settled in this partition — almost
	// always one, since the counterparty is usually elsewhere.
	FillHalves    map[uuid.UUID]int
	FillFirstSeen map[uuid.UUID]time.Time
}

// ReconcileSlice gathers this partition's contribution to the cluster verification block.
func (e *Engine) ReconcileSlice(ctx context.Context) (ReconcileSlice, error) {
	out := ReconcileSlice{
		ClearingByUnit: map[string]int64{},
		SharesBySymbol: map[string]int64{},
		FillHalves:     map[uuid.UUID]int{},
		FillFirstSeen:  map[uuid.UUID]time.Time{},
	}

	clearing, err := e.pool.Query(ctx, `
		SELECT la.unit, sum(p.amount)
		  FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
		 WHERE la.kind = 'CLEARING'
		 GROUP BY la.unit`)
	if err != nil {
		return out, fmt.Errorf("clearing slice: %w", err)
	}
	for clearing.Next() {
		var unit string
		var amt int64
		if err := clearing.Scan(&unit, &amt); err != nil {
			clearing.Close()
			return out, err
		}
		out.ClearingByUnit[unit] = amt
	}
	clearing.Close()
	if err := clearing.Err(); err != nil {
		return out, err
	}

	shares, err := e.pool.Query(ctx, `
		SELECT la.unit, sum(p.amount)
		  FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
		 WHERE la.unit <> 'USD'
		 GROUP BY la.unit`)
	if err != nil {
		return out, fmt.Errorf("share slice: %w", err)
	}
	for shares.Next() {
		var sym string
		var qty int64
		if err := shares.Scan(&sym, &qty); err != nil {
			shares.Close()
			return out, err
		}
		out.SharesBySymbol[sym] = qty
	}
	shares.Close()
	if err := shares.Err(); err != nil {
		return out, err
	}

	if err := e.pool.QueryRow(ctx, `
		SELECT coalesce(sum(p.amount), 0)
		  FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
		 WHERE la.unit = 'USD'`).Scan(&out.CashDelta); err != nil {
		return out, fmt.Errorf("cash slice: %w", err)
	}

	fills, err := e.pool.Query(ctx, `
		SELECT fill_id, count(*), min(created_at)
		  FROM transactions WHERE fill_id IS NOT NULL GROUP BY fill_id`)
	if err != nil {
		return out, fmt.Errorf("fill slice: %w", err)
	}
	defer fills.Close()
	for fills.Next() {
		var id uuid.UUID
		var n int
		var first time.Time
		if err := fills.Scan(&id, &n, &first); err != nil {
			return out, err
		}
		out.FillHalves[id] = n
		out.FillFirstSeen[id] = first
	}
	return out, fills.Err()
}

// --- harness support -------------------------------------------------------
//
// These exist for the chaos runner's EXTERNAL oracle. They read what the system believes
// so the harness can compare it against what the harness itself recorded submitting. The
// comparison is only meaningful because the two records are independent.

// TerminalOrders returns every order that has reached a terminal state.
func (e *Engine) TerminalOrders(ctx context.Context) (map[uuid.UUID]string, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT id, status::text FROM orders
		 WHERE status IN ('FILLED','CANCELLED','REJECTED','EXPIRED')`)
	if err != nil {
		return nil, fmt.Errorf("terminal orders: %w", err)
	}
	defer rows.Close()
	out := map[uuid.UUID]string{}
	for rows.Next() {
		var id uuid.UUID
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			return nil, err
		}
		out[id] = status
	}
	return out, rows.Err()
}

// WorkingOrders returns orders that have not yet reached a terminal state.
func (e *Engine) WorkingOrders(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT id FROM orders
		 WHERE status IN ('ACCEPTED','PARTIALLY_FILLED') ORDER BY seq_no`)
	if err != nil {
		return nil, fmt.Errorf("working orders: %w", err)
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

// DuplicateClientOrderIDs finds any client_order_id that produced more than one order.
//
// The unique constraint makes this impossible, which is the point: the check exists to
// prove the constraint is still there and still doing its job, not because the query is
// expected to find something.
func (e *Engine) DuplicateClientOrderIDs(ctx context.Context) ([]string, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT client_order_id FROM orders
		 GROUP BY account_id, client_order_id HAVING count(*) > 1`)
	if err != nil {
		return nil, fmt.Errorf("duplicate client order ids: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// CountFills is the number of distinct executions settled.
func (e *Engine) CountFills(ctx context.Context) (int64, error) {
	var n int64
	err := e.pool.QueryRow(ctx, `SELECT count(*) FROM fills`).Scan(&n)
	return n, err
}
