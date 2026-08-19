package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/ids"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/money"
)

// Config carries every tunable. None of these are constants in the code: the same binary
// runs the small local playground and the benchmark cluster, and a `const` here is a
// migration later (seam contract #5).
type Config struct {
	ShardID         int
	CollarBps       int64 // market-order execution band around the reference price
	TakerFeeBps     int64
	MakerFeeBps     int64
	DedupWindow     int           // terminated order ids each book retains
	MaxRefStaleness time.Duration // a reference price older than this is not a price
	// SettleWindow is how long a fill's two halves may legitimately be unpaired. Beyond
	// it, an unpaired half is stuck rather than in flight.
	SettleWindow time.Duration
}

// DefaultConfig is the playground profile.
func DefaultConfig() Config {
	return Config{
		ShardID:         0,
		CollarBps:       500, // 5%
		TakerFeeBps:     5,
		MakerFeeBps:     0,
		DedupWindow:     1 << 20,
		MaxRefStaleness: 5 * time.Second,
		SettleWindow:    5 * time.Second,
	}
}

// Engine is the trading core: accounts, cash, reservations, orders, positions, and the
// double-entry ledger. It is sharded by account_id.
//
// As of Phase 3 it does NOT contain a book. Matching lives in its own service, sharded by
// symbol, with an event log between them. What the core keeps is everything whose
// correctness is per-account, which is exactly the set of things one account's row lock
// can serialize.
type Engine struct {
	cfg   Config
	pool  *pgxpool.Pool
	md    *marketdata.Cache
	obs   Observer
	lease *Lease // nil means unfenced; see SetLease
}

// New wires an engine. Books are rebuilt from durable state by Recover.
func New(pool *pgxpool.Pool, md *marketdata.Cache, cfg Config) *Engine {
	return &Engine{cfg: cfg, pool: pool, md: md, obs: NopObserver{}}
}

// Config returns the engine's configuration.
func (e *Engine) Config() Config { return e.cfg }

// MarkPrice returns the current conflated mark for a symbol, ignoring staleness. For
// display and for harness code deciding where to post; never for sizing a reservation,
// which must go through the staleness check.
func (e *Engine) MarkPrice(symbol string) (money.Minor, bool) { return e.md.Ref(symbol, 0) }

var (
	// ErrDuplicateEvent means this event_id has already been applied. Not a failure:
	// it is the mechanism that turns Kafka's at-least-once into exactly-once.
	ErrDuplicateEvent = errors.New("duplicate event")
	// ErrRejected carries a business rejection (insufficient funds, no reference price).
	ErrRejected = errors.New("rejected")
)

// Rejection is a business rejection with a machine-readable reason.
type Rejection struct {
	Reason string
	Detail string
}

func (r *Rejection) Error() string {
	if r.Detail == "" {
		return r.Reason
	}
	return r.Reason + ": " + r.Detail
}
func (r *Rejection) Unwrap() error { return ErrRejected }

func reject(reason, format string, args ...any) *Rejection {
	return &Rejection{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// ---------------------------------------------------------------------------
// accounts
// ---------------------------------------------------------------------------

// OpenAccount creates an account. The engine knows nothing about it but an opaque UUID:
// no email, no name that means anything, no identity (seam contract #1). `label` exists
// so the CLI can print something human, and is never interpreted.
func (e *Engine) OpenAccount(ctx context.Context, label string) (uuid.UUID, error) {
	id := uuid.New()
	_, err := e.pool.Exec(ctx, `INSERT INTO accounts (id, label) VALUES ($1, $2)`, id, label)
	if err != nil {
		return uuid.Nil, fmt.Errorf("open account: %w", err)
	}
	return id, nil
}

// AccountRef resolves a label or uuid string to an account id, so the CLI can take either.
func (e *Engine) AccountRef(ctx context.Context, ref string) (uuid.UUID, error) {
	if id, err := uuid.Parse(ref); err == nil {
		return id, nil
	}
	var id uuid.UUID
	err := e.pool.QueryRow(ctx, `SELECT id FROM accounts WHERE label = $1`, ref).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("no account %q: %w", ref, err)
	}
	return id, nil
}

// Deposit credits cash from outside the system.
//
// This is the only way money enters, which is what makes total-cash-versus-total-deposits
// a meaningful invariant rather than an identity.
func (e *Engine) Deposit(ctx context.Context, account uuid.UUID, amount money.Minor) error {
	if amount <= 0 {
		return reject("INVALID_AMOUNT", "deposit must be positive")
	}
	return e.inTx(ctx, func(tx pgx.Tx) error {
		seq, err := lockAccount(ctx, tx, account)
		if err != nil {
			return err
		}
		return writeTxn(ctx, tx, txnSpec{
			account: account,
			seq:     seq,
			kind:    "DEPOSIT",
			eventID: ids.LedgerEventID(account, seq),
			legs:    []leg{{kind: "CASH", amount: int64(amount)}, {kind: "EXTERNAL", amount: -int64(amount)}},
		})
	})
}

// SeedShares gives an account an opening position by BUYING it from outside the system at
// the stated price. Used to give market makers inventory to quote with.
//
// It is a purchase, not a grant, and that is a Phase 2 correction rather than a stylistic
// choice. A grant moves shares in without moving cash, so the position's cost basis exists
// nowhere in the ledger — it is a number the write path invented and stored, and no replay
// could ever reconstruct it. Modelling it as a purchase puts the basis in the cash leg,
// where every other position's basis lives, and makes the whole projection recoverable
// from postings alone.
//
// The account must therefore have the cash. That is not an inconvenience of the model; it
// is the model being honest about where value came from.
func (e *Engine) SeedShares(ctx context.Context, account uuid.UUID, symbol string, qty money.Qty, price money.Minor) error {
	if qty <= 0 {
		return reject("INVALID_AMOUNT", "seed qty must be positive")
	}
	basis, ok := money.Notional(qty, price)
	if !ok {
		return reject("OVERFLOW", "seed notional does not fit")
	}

	return e.inTx(ctx, func(tx pgx.Tx) error {
		seq, err := lockAccount(ctx, tx, account)
		if err != nil {
			return err
		}

		bal, err := balances(ctx, tx, account)
		if err != nil {
			return err
		}
		if bal.BuyingPower < basis {
			return reject("INSUFFICIENT_BUYING_POWER",
				"seeding %s %s at %s costs %s, buying power is %s",
				qty, symbol, price, basis, bal.BuyingPower)
		}

		if err := writeTxn(ctx, tx, txnSpec{
			account: account,
			seq:     seq,
			kind:    "DEPOSIT",
			eventID: ids.LedgerEventID(account, seq),
			legs: []leg{
				{kind: "CASH", amount: -int64(basis)},
				{kind: "EXTERNAL", amount: int64(basis)},
				{kind: "POSITION", symbol: symbol, amount: int64(qty)},
				{kind: "EXTERNAL", symbol: symbol, amount: -int64(qty)},
			},
		}); err != nil {
			return err
		}

		var p positionState
		if err := tx.QueryRow(ctx, `
			INSERT INTO positions (account_id, symbol) VALUES ($1, $2)
			ON CONFLICT (account_id, symbol) DO UPDATE SET symbol = EXCLUDED.symbol
			RETURNING qty, cost_basis, realized_pnl`,
			account, symbol).Scan(&p.Qty, &p.Basis, &p.Realized); err != nil {
			return err
		}
		p.applyBuy(qty, basis)

		_, err = tx.Exec(ctx, `
			UPDATE positions SET qty = $3, cost_basis = $4, realized_pnl = $5
			 WHERE account_id = $1 AND symbol = $2`,
			account, symbol, int64(p.Qty), int64(p.Basis), int64(p.Realized))
		return err
	})
}

// ---------------------------------------------------------------------------
// balances
// ---------------------------------------------------------------------------

// Balances is an account's cash view. Buying power is derived, never stored, so it
// cannot drift from the reservations that justify it.
type Balances struct {
	Cash         money.Minor
	ReservedCash money.Minor
	BuyingPower  money.Minor
	Fees         money.Minor
}

// Balances reads an account's cash position from the ledger.
func (e *Engine) Balances(ctx context.Context, account uuid.UUID) (Balances, error) {
	return balances(ctx, e.pool, account)
}

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func balances(ctx context.Context, q querier, account uuid.UUID) (Balances, error) {
	var b Balances
	err := q.QueryRow(ctx, `
		SELECT coalesce(cash, 0), coalesce(fees, 0)
		FROM account_balances WHERE account_id = $1`, account).Scan(&b.Cash, &b.Fees)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil // no postings yet
	}
	if err != nil {
		return b, fmt.Errorf("cash balance: %w", err)
	}
	err = q.QueryRow(ctx, `
		SELECT coalesce(sum(reserved - consumed - released), 0)
		FROM reservations WHERE account_id = $1 AND kind = 'CASH'`, account).Scan(&b.ReservedCash)
	if err != nil {
		return b, fmt.Errorf("reserved cash: %w", err)
	}
	b.BuyingPower = b.Cash - b.ReservedCash
	return b, nil
}

// Position is one symbol's holding, with P&L.
type Position struct {
	Symbol        string
	Qty           money.Qty
	CostBasis     money.Minor
	RealizedPnL   money.Minor
	ReservedQty   money.Qty
	MarkPrice     money.Minor
	UnrealizedPnL money.Minor
	HasMark       bool
}

// Positions reads an account's holdings.
//
// Unrealized P&L is computed here, at read time, from the conflated mark price. It is
// never stored and never updated by a tick: that is why a million ticks a second creates
// no work for the transactional path (ARCHITECTURE.md §6.1).
func (e *Engine) Positions(ctx context.Context, account uuid.UUID) ([]Position, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT p.symbol, p.qty, p.cost_basis, p.realized_pnl,
		       coalesce((SELECT sum(r.reserved - r.consumed - r.released)
		                   FROM reservations r
		                  WHERE r.account_id = p.account_id AND r.kind = 'SHARES'
		                    AND r.symbol = p.symbol), 0)
		FROM positions p WHERE p.account_id = $1 ORDER BY p.symbol`, account)
	if err != nil {
		return nil, fmt.Errorf("positions: %w", err)
	}
	defer rows.Close()

	var out []Position
	for rows.Next() {
		var p Position
		if err := rows.Scan(&p.Symbol, &p.Qty, &p.CostBasis, &p.RealizedPnL, &p.ReservedQty); err != nil {
			return nil, err
		}
		if mark, ok := e.md.Ref(p.Symbol, 0); ok {
			p.MarkPrice, p.HasMark = mark, true
			if mv, ok := money.Notional(p.Qty, mark); ok {
				p.UnrealizedPnL = mv - p.CostBasis
			}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// transaction plumbing
// ---------------------------------------------------------------------------

// inTx runs fn in a transaction, fenced by the partition lease.
//
// The epoch assertion happens FIRST and inside the same transaction as the write. That
// co-location is the whole mechanism: a process that lost its partition cannot commit,
// because the assertion and the write either commit together or not at all. Checking
// ownership before beginning the transaction would leave a window; checking it in a
// different transaction would leave a larger one.
func (e *Engine) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if e.lease != nil {
		if err := e.lease.assert(ctx, tx); err != nil {
			return err
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// lockAccount takes the account row lock and allocates the next per-account sequence
// number.
//
// One statement, three jobs (ARCHITECTURE.md §2.5c): it serializes concurrent writers
// for this account, it orders their effects, and it produces the replay watermark. The
// lock is held for the rest of the transaction, so concurrency in this system is across
// accounts and never within one.
func lockAccount(ctx context.Context, tx pgx.Tx, account uuid.UUID) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, `SELECT next_seq FROM accounts WHERE id = $1 FOR UPDATE`, account).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, reject("NO_SUCH_ACCOUNT", "%s", account)
	}
	if err != nil {
		return 0, fmt.Errorf("lock account: %w", err)
	}
	return seq, nil
}

func bumpSeq(ctx context.Context, tx pgx.Tx, account uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE accounts SET next_seq = next_seq + 1 WHERE id = $1`, account)
	return err
}

// leg is one side of a double-entry posting, addressed by ledger account kind.
type leg struct {
	kind   string
	symbol string // empty means the cash unit
	amount int64
}

type txnSpec struct {
	account uuid.UUID
	seq     int64
	kind    string
	eventID uuid.UUID
	orderID *uuid.UUID
	fillID  *uuid.UUID
	legs    []leg
}

// writeTxn inserts a transaction and its postings, and advances the account clock.
//
// If the event has already been applied it returns ErrDuplicateEvent and writes nothing.
// That is the entire exactly-once story on the apply path: the caller commits its
// consumer offset in this same transaction, so a redelivery finds the event present,
// skips it, and still advances.
func writeTxn(ctx context.Context, tx pgx.Tx, s txnSpec) error {
	var txnID int64
	err := tx.QueryRow(ctx, `
		INSERT INTO transactions (account_id, account_seq, kind, event_id, order_id, fill_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING id`,
		s.account, s.seq, s.kind, s.eventID, s.orderID, s.fillID).Scan(&txnID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrDuplicateEvent
	}
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}

	var dCash, dFees int64
	for _, l := range s.legs {
		if l.amount == 0 {
			continue // a zero posting carries no information and clutters the ledger
		}
		acct, err := ledgerAccount(ctx, tx, s.account, l.kind, l.symbol)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO postings (txn_id, ledger_acct, amount) VALUES ($1, $2, $3)`,
			txnID, acct, l.amount); err != nil {
			return fmt.Errorf("insert posting: %w", err)
		}
		switch l.kind {
		case "CASH":
			dCash += l.amount
		case "FEES":
			dFees += l.amount
		}
	}

	// The balance projection moves in the same transaction as the postings it summarizes.
	if dCash != 0 || dFees != 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO account_balances (account_id, cash, fees) VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE
			SET cash = account_balances.cash + EXCLUDED.cash,
			    fees = account_balances.fees + EXCLUDED.fees`,
			s.account, dCash, dFees); err != nil {
			return fmt.Errorf("update balance projection: %w", err)
		}
	}
	return bumpSeq(ctx, tx, s.account)
}

// ledgerAccount resolves (account, kind, symbol) to a ledger account id, creating it on
// first use. `unit` is what the deferred balance trigger groups by: a transaction that
// moves both cash and shares must balance in each unit separately, since summing cents
// and share-millionths together would be arithmetic without meaning.
func ledgerAccount(ctx context.Context, tx pgx.Tx, account uuid.UUID, kind, symbol string) (int64, error) {
	unit := "USD"
	var symArg any
	if symbol != "" {
		unit = symbol
		symArg = symbol
	}

	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO ledger_accounts (account_id, kind, symbol, unit)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_id, kind, coalesce(symbol, '')) DO UPDATE SET unit = ledger_accounts.unit
		RETURNING id`, account, kind, symArg, unit).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ledger account %s/%s: %w", kind, symbol, err)
	}
	return id, nil
}

// Pool exposes the connection pool for callers that legitimately need it outside the
// engine's own operations — applying migrations at boot, and answering a readiness probe.
// Nothing in the trading path should reach for this.
func (e *Engine) Pool() *pgxpool.Pool { return e.pool }
