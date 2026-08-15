package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/money"
)

// snapshotVersion is the on-disk format tag. Bump it and old snapshots are ignored rather
// than misread, which is the only safe way to change a format that recovery depends on.
const snapshotVersion = 1

// ReplayState is an account's complete financial state, rebuilt from the ledger.
//
// Phase 2 extends this beyond quantities to cost basis and realized P&L. That matters
// more than it looks: it is the difference between "the ledger justifies your share
// count" and "the ledger justifies every number the system will ever show you". Both are
// recoverable because a fill's cash leg carries its full economics — outlay including
// fees on a buy, net proceeds on a sell — so nothing about the fill needs to be stored
// outside the double entry to reconstruct the accounting.
type ReplayState struct {
	Seq       int64
	Cash      money.Minor
	Fees      money.Minor
	Clearing  money.Minor
	Positions map[string]positionState
}

func newReplayState() ReplayState {
	return ReplayState{Positions: map[string]positionState{}}
}

func (s ReplayState) clone() ReplayState {
	out := ReplayState{Seq: s.Seq, Cash: s.Cash, Fees: s.Fees, Clearing: s.Clearing}
	out.Positions = make(map[string]positionState, len(s.Positions))
	for k, v := range s.Positions {
		out.Positions[k] = v
	}
	return out
}

// Encode serialises the state canonically, so two states can be compared as bytes and a
// snapshot can be stored and reloaded without ambiguity.
//
// Canonical means: a version tag, fixed field order, symbols sorted, fixed-width
// big-endian integers, and empty positions dropped. This is deliberately not jsonb —
// jsonb normalises key order and numeric representation, so a byte comparison over it
// proves nothing. It is also not protobuf yet: generated types arrive in Phase 3, where
// .proto files are needed for Kafka envelopes anyway, and snapshots adopt them then
// rather than pulling a codegen toolchain in early for one struct.
func (s ReplayState) Encode() []byte {
	var buf bytes.Buffer
	write := func(v int64) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v))
		buf.Write(b[:])
	}

	write(snapshotVersion)
	write(s.Seq)
	write(int64(s.Cash))
	write(int64(s.Fees))
	write(int64(s.Clearing))

	syms := make([]string, 0, len(s.Positions))
	for k, v := range s.Positions {
		if v.Qty != 0 || v.Basis != 0 || v.Realized != 0 {
			syms = append(syms, k)
		}
	}
	sort.Strings(syms)
	write(int64(len(syms)))

	for _, sym := range syms {
		p := s.Positions[sym]
		write(int64(len(sym)))
		buf.WriteString(sym)
		write(int64(p.Qty))
		write(int64(p.Basis))
		write(int64(p.Realized))
	}
	return buf.Bytes()
}

// DecodeState reverses Encode.
func DecodeState(b []byte) (ReplayState, error) {
	s := newReplayState()
	r := bytes.NewReader(b)
	read := func() (int64, error) {
		var v uint64
		err := binary.Read(r, binary.BigEndian, &v)
		return int64(v), err
	}

	version, err := read()
	if err != nil {
		return s, fmt.Errorf("snapshot truncated: %w", err)
	}
	if version != snapshotVersion {
		return s, fmt.Errorf("snapshot version %d, expected %d", version, snapshotVersion)
	}

	for _, dst := range []*int64{&s.Seq, (*int64)(&s.Cash), (*int64)(&s.Fees), (*int64)(&s.Clearing)} {
		v, err := read()
		if err != nil {
			return s, fmt.Errorf("snapshot truncated: %w", err)
		}
		*dst = v
	}

	n, err := read()
	if err != nil {
		return s, fmt.Errorf("snapshot truncated: %w", err)
	}
	for i := int64(0); i < n; i++ {
		nameLen, err := read()
		if err != nil || nameLen < 0 || nameLen > 64 {
			return s, fmt.Errorf("snapshot has a bad symbol length at entry %d", i)
		}
		name := make([]byte, nameLen)
		if _, err := r.Read(name); err != nil {
			return s, fmt.Errorf("snapshot truncated in symbol %d: %w", i, err)
		}
		var p positionState
		for _, dst := range []*int64{(*int64)(&p.Qty), (*int64)(&p.Basis), (*int64)(&p.Realized)} {
			v, err := read()
			if err != nil {
				return s, fmt.Errorf("snapshot truncated: %w", err)
			}
			*dst = v
		}
		s.Positions[string(name)] = p
	}
	return s, nil
}

// ReplayAccount rebuilds an account's state from genesis.
func (e *Engine) ReplayAccount(ctx context.Context, account uuid.UUID) (ReplayState, error) {
	return e.ReplayAccountRange(ctx, account, 0, 0, newReplayState())
}

// ReplayAccountFrom continues a replay from a snapshot taken at fromSeq.
func (e *Engine) ReplayAccountFrom(ctx context.Context, account uuid.UUID, fromSeq int64, from ReplayState) (ReplayState, error) {
	return e.ReplayAccountRange(ctx, account, fromSeq, 0, from)
}

// txnLegs accumulates one transaction's postings so they can be interpreted together.
// A fill's meaning is spread across its legs — the share leg says what moved, the cash
// leg says what it cost — so postings cannot be applied one at a time.
type txnLegs struct {
	seq       int64
	cash      money.Minor
	fees      money.Minor
	clearing  money.Minor
	positions map[string]money.Qty
}

// ReplayAccountRange replays (fromSeq, toSeq]. A toSeq of zero means "to the end".
//
// account_seq — not the postings' bigserial id — is the watermark. Sequence values are
// allocated before commit, so a bigserial ordering can interleave differently from commit
// order and silently drop a transaction that committed late. account_seq is allocated
// under the account row lock, so it is gap-free and commit-ordered by construction.
func (e *Engine) ReplayAccountRange(ctx context.Context, account uuid.UUID, fromSeq, toSeq int64, from ReplayState) (ReplayState, error) {
	state := from.clone()
	if state.Positions == nil {
		state.Positions = map[string]positionState{}
	}

	rows, err := e.pool.Query(ctx, `
		SELECT t.account_seq, la.kind::text, coalesce(la.symbol, ''), p.amount
		  FROM transactions t
		  JOIN postings p ON p.txn_id = t.id
		  JOIN ledger_accounts la ON la.id = p.ledger_acct
		 WHERE t.account_id = $1 AND t.account_seq > $2
		   AND ($3 = 0 OR t.account_seq <= $3)
		 ORDER BY t.account_seq, p.id`, account, fromSeq, toSeq)
	if err != nil {
		return state, fmt.Errorf("replay: %w", err)
	}
	defer rows.Close()

	var current *txnLegs
	flush := func() {
		if current == nil {
			return
		}
		state.Seq = current.seq
		state.Cash += current.cash
		state.Fees += current.fees
		state.Clearing += current.clearing
		// One symbol per transaction in this system; the loop is defensive, not general.
		for sym, delta := range current.positions {
			p := state.Positions[sym]
			p.apply(delta, current.cash)
			state.Positions[sym] = p
		}
		current = nil
	}

	for rows.Next() {
		var (
			seq    int64
			kind   string
			symbol string
			amount int64
		)
		if err := rows.Scan(&seq, &kind, &symbol, &amount); err != nil {
			return state, err
		}
		if current != nil && current.seq != seq {
			flush()
		}
		if current == nil {
			current = &txnLegs{seq: seq, positions: map[string]money.Qty{}}
		}

		switch kind {
		case "CASH":
			current.cash += money.Minor(amount)
		case "FEES":
			current.fees += money.Minor(amount)
		case "CLEARING":
			if symbol == "" {
				current.clearing += money.Minor(amount)
			}
		case "POSITION":
			current.positions[symbol] += money.Qty(amount)
		}
	}
	if err := rows.Err(); err != nil {
		return state, err
	}
	flush()
	return state, nil
}

// ProjectedState reads the same account's state from the projections the write path
// maintains. Comparing it to a replay is how "the ledger is the truth and everything else
// is derived" stops being a slogan and becomes a test.
func (e *Engine) ProjectedState(ctx context.Context, account uuid.UUID) (ReplayState, error) {
	state := newReplayState()

	b, err := balances(ctx, e.pool, account)
	if err != nil {
		return state, err
	}
	state.Cash, state.Fees = b.Cash, b.Fees

	if err := e.pool.QueryRow(ctx, `
		SELECT coalesce(sum(p.amount), 0)
		  FROM postings p JOIN ledger_accounts la ON la.id = p.ledger_acct
		 WHERE la.account_id = $1 AND la.kind = 'CLEARING' AND la.symbol IS NULL`,
		account).Scan(&state.Clearing); err != nil {
		return state, fmt.Errorf("clearing: %w", err)
	}

	if err := e.pool.QueryRow(ctx,
		`SELECT coalesce(max(account_seq), 0) FROM transactions WHERE account_id = $1`,
		account).Scan(&state.Seq); err != nil {
		return state, err
	}

	rows, err := e.pool.Query(ctx,
		`SELECT symbol, qty, cost_basis, realized_pnl FROM positions WHERE account_id = $1`, account)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	for rows.Next() {
		var sym string
		var p positionState
		if err := rows.Scan(&sym, &p.Qty, &p.Basis, &p.Realized); err != nil {
			return state, err
		}
		state.Positions[sym] = p
	}
	return state, rows.Err()
}

// Accounts lists every account id, for whole-system replay checks.
func (e *Engine) Accounts(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := e.pool.Query(ctx, `SELECT id FROM accounts ORDER BY created_at, id`)
	if err != nil {
		return nil, err
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
