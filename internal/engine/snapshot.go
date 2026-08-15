package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// snapshotsKept is how many checkpoints per account survive pruning. More than one so a
// corrupt or truncated latest snapshot has a predecessor to fall back to; few enough that
// the table stays bounded.
const snapshotsKept = 3

// LoadSnapshot returns the most recent checkpoint for an account.
func (e *Engine) LoadSnapshot(ctx context.Context, account uuid.UUID) (ReplayState, bool, error) {
	var raw []byte
	err := e.pool.QueryRow(ctx, `
		SELECT state FROM snapshots WHERE account_id = $1
		 ORDER BY account_seq DESC LIMIT 1`, account).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return newReplayState(), false, nil
	}
	if err != nil {
		return newReplayState(), false, fmt.Errorf("load snapshot: %w", err)
	}

	state, err := DecodeState(raw)
	if err != nil {
		return newReplayState(), false, fmt.Errorf("decode snapshot for %s: %w", account, err)
	}
	return state, true, nil
}

// RecoverState rebuilds an account from its latest snapshot plus the tail of the ledger.
//
// This is the bounded-recovery path, and it is required to agree exactly with a replay
// from genesis. If it ever does not, the snapshot is lying, and every recovery after it
// inherits the lie — which is why the equivalence is a property test rather than a
// comment.
func (e *Engine) RecoverState(ctx context.Context, account uuid.UUID) (ReplayState, error) {
	snapshot, found, err := e.LoadSnapshot(ctx, account)
	if err != nil {
		return newReplayState(), err
	}
	if !found {
		return e.ReplayAccount(ctx, account)
	}
	return e.ReplayAccountFrom(ctx, account, snapshot.Seq, snapshot)
}

// Snapshot checkpoints an account at its current sequence and prunes old checkpoints.
//
// The checkpoint is taken from a recovery, not from the projection tables. Snapshotting
// the projection would checkpoint whatever the write path currently believes; snapshotting
// a reconstruction checkpoints what the ledger says, so a projection bug cannot be baked
// into the recovery path and then read back as truth.
func (e *Engine) Snapshot(ctx context.Context, account uuid.UUID) (ReplayState, error) {
	state, err := e.RecoverState(ctx, account)
	if err != nil {
		return state, err
	}
	if state.Seq == 0 {
		return state, nil // nothing has happened to this account yet
	}

	if _, err := e.pool.Exec(ctx, `
		INSERT INTO snapshots (account_id, account_seq, state) VALUES ($1, $2, $3)
		ON CONFLICT (account_id, account_seq) DO NOTHING`,
		account, state.Seq, state.Encode()); err != nil {
		return state, fmt.Errorf("write snapshot: %w", err)
	}

	if _, err := e.pool.Exec(ctx, `
		DELETE FROM snapshots
		 WHERE account_id = $1
		   AND account_seq NOT IN (
		     SELECT account_seq FROM snapshots WHERE account_id = $1
		      ORDER BY account_seq DESC LIMIT $2)`,
		account, snapshotsKept); err != nil {
		return state, fmt.Errorf("prune snapshots: %w", err)
	}
	return state, nil
}

// SnapshotAll checkpoints every account, returning how many were written.
func (e *Engine) SnapshotAll(ctx context.Context) (int, error) {
	accounts, err := e.Accounts(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, a := range accounts {
		state, err := e.Snapshot(ctx, a)
		if err != nil {
			return n, err
		}
		if state.Seq > 0 {
			n++
		}
	}
	return n, nil
}

// SnapshotStats describes the checkpoint table, for the CLI and metrics.
type SnapshotStats struct {
	Accounts  int
	Snapshots int
	MaxTail   int64 // most transactions any account must replay past its latest snapshot
}

// SnapshotStats reports how bounded recovery currently is. MaxTail is the number that
// matters: it is the worst-case replay length, and it is the thing snapshots exist to cap.
func (e *Engine) SnapshotStats(ctx context.Context) (SnapshotStats, error) {
	var s SnapshotStats
	err := e.pool.QueryRow(ctx, `
		SELECT (SELECT count(DISTINCT account_id) FROM snapshots),
		       (SELECT count(*) FROM snapshots),
		       coalesce((
		         SELECT max(t.max_seq - coalesce(s.snap_seq, 0))
		           FROM (SELECT account_id, max(account_seq) AS max_seq
		                   FROM transactions GROUP BY account_id) t
		           LEFT JOIN (SELECT account_id, max(account_seq) AS snap_seq
		                        FROM snapshots GROUP BY account_id) s
		             ON s.account_id = t.account_id), 0)`).
		Scan(&s.Accounts, &s.Snapshots, &s.MaxTail)
	if err != nil {
		return s, fmt.Errorf("snapshot stats: %w", err)
	}
	return s, nil
}
