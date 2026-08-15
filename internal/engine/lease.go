package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrLeaseLost means this process no longer owns the partition it was writing to. Every
// transaction that discovers it aborts; the process must stop writing and re-acquire.
var ErrLeaseLost = errors.New("partition lease lost")

// Lease is a process's claim on a partition.
//
// The claim is enforced where it matters: inside the write transaction, against the same
// database the write is going to. A stalled process that comes back to life after its
// lease expired will find its epoch stale on the very next statement it tries to commit,
// and will fail before it can touch a balance. Nothing outside Postgres has to be
// consulted, and nothing outside Postgres has to be trusted.
type Lease struct {
	pool        *pgxpool.Pool
	partitionID int
	ownerID     uuid.UUID
	epoch       int64
	ttl         time.Duration

	// OnLost is called when a renewal discovers the lease was taken. The process should
	// stop accepting work; its in-flight transactions will abort on their own.
	OnLost func(error)
}

// LeaseTTL is how long a lease survives without renewal. Long enough to ride out a GC
// pause or a slow query; short enough that a dead process's partition is recoverable
// quickly. Renewal runs at a fraction of it.
const LeaseTTL = 10 * time.Second

// AcquireLease claims a partition, taking it from whoever held it before.
//
// Acquisition is unconditional by design: a new owner always wins. The previous owner is
// not asked, because if it were reachable and healthy the new owner would not be starting.
// What protects the previous owner's in-flight work is not being consulted here — it is
// the epoch assertion failing on its next commit.
func AcquireLease(ctx context.Context, pool *pgxpool.Pool, partitionID int, ttl time.Duration) (*Lease, error) {
	if ttl <= 0 {
		ttl = LeaseTTL
	}
	owner := uuid.New()

	var epoch int64
	err := pool.QueryRow(ctx, `
		INSERT INTO partition_leases (partition_id, owner_id, epoch, expires_at)
		VALUES ($1, $2, 1, now() + make_interval(secs => $3))
		ON CONFLICT (partition_id) DO UPDATE
		SET owner_id    = EXCLUDED.owner_id,
		    epoch       = partition_leases.epoch + 1,
		    acquired_at = now(),
		    expires_at  = EXCLUDED.expires_at
		RETURNING epoch`, partitionID, owner, ttl.Seconds()).Scan(&epoch)
	if err != nil {
		return nil, fmt.Errorf("acquire lease for partition %d: %w", partitionID, err)
	}
	return &Lease{pool: pool, partitionID: partitionID, ownerID: owner, epoch: epoch, ttl: ttl}, nil
}

// Epoch is the fencing token this process writes under.
func (l *Lease) Epoch() int64 { return l.epoch }

// PartitionID is the partition this lease covers.
func (l *Lease) PartitionID() int { return l.partitionID }

// Renew extends the lease, and reports ErrLeaseLost if someone else has taken it.
func (l *Lease) Renew(ctx context.Context) error {
	tag, err := l.pool.Exec(ctx, `
		UPDATE partition_leases
		   SET expires_at = now() + make_interval(secs => $3)
		 WHERE partition_id = $1 AND owner_id = $2 AND epoch = $4`,
		l.partitionID, l.ownerID, l.ttl.Seconds(), l.epoch)
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("partition %d taken by another process: %w", l.partitionID, ErrLeaseLost)
	}
	return nil
}

// RunRenewer keeps the lease alive until the context is cancelled.
//
// It renews at a third of the TTL, so two consecutive failures still leave time to notice
// before the lease actually expires. Losing the lease is not retried: another process owns
// the partition now, and the correct response is to stop, not to fight over it.
func (l *Lease) RunRenewer(ctx context.Context) {
	t := time.NewTicker(l.ttl / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := l.Renew(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				if l.OnLost != nil {
					l.OnLost(err)
				}
				if errors.Is(err, ErrLeaseLost) {
					return
				}
			}
		}
	}
}

// Release gives up the lease. Best effort: a crashed process simply lets it expire.
func (l *Lease) Release(ctx context.Context) error {
	_, err := l.pool.Exec(ctx,
		`DELETE FROM partition_leases WHERE partition_id = $1 AND owner_id = $2 AND epoch = $3`,
		l.partitionID, l.ownerID, l.epoch)
	return err
}

// assert verifies inside a transaction that this process still owns the partition.
//
// It reads the lease row FOR SHARE, which does two things: it sees the committed epoch,
// and it blocks a concurrent acquisition from committing until this transaction finishes.
// Without the lock, a takeover could slip in between the assertion and the write, and the
// fence would have a hole exactly the width of one transaction.
func (l *Lease) assert(ctx context.Context, tx pgx.Tx) error {
	var epoch int64
	err := tx.QueryRow(ctx,
		`SELECT epoch FROM partition_leases WHERE partition_id = $1 FOR SHARE`,
		l.partitionID).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("partition %d has no lease: %w", l.partitionID, ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("assert lease: %w", err)
	}
	if epoch != l.epoch {
		return fmt.Errorf("partition %d moved to epoch %d, this process holds %d: %w",
			l.partitionID, epoch, l.epoch, ErrLeaseLost)
	}
	return nil
}

// SetLease binds an engine to a partition lease. Every write transaction from then on
// asserts it. An engine without a lease writes unfenced, which is only safe when exactly
// one process can possibly exist — a one-shot CLI command, or a test.
func (e *Engine) SetLease(l *Lease) { e.lease = l }

// Lease returns the engine's lease, or nil if it is unfenced.
func (e *Engine) Lease() *Lease { return e.lease }

// AssertOwnership fails if this database is not the partition the engine thinks it is.
//
// Connecting a partition-3 process to partition-5's database would otherwise succeed and
// write a perfectly balanced, perfectly wrong ledger — every transaction internally
// consistent, every account in the wrong place, and no invariant able to see it. This is
// cheap and it runs at startup.
func (e *Engine) AssertOwnership(ctx context.Context, partitionID int) error {
	var actual int
	err := e.pool.QueryRow(ctx, `SELECT partition_id FROM partition_identity`).Scan(&actual)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = e.pool.Exec(ctx,
			`INSERT INTO partition_identity (partition_id) VALUES ($1)`, partitionID)
		return err
	}
	if err != nil {
		return fmt.Errorf("read partition identity: %w", err)
	}
	if actual != partitionID {
		return fmt.Errorf("this database is partition %d, but the process is configured as partition %d",
			actual, partitionID)
	}
	return nil
}
