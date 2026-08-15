// Package cluster partitions the trading core across databases and reconciles across them.
//
// Two things change once accounts are spread over partitions, and they are the whole
// content of Phase 4.
//
// First, ownership becomes a mechanism rather than a fact. With one process there was
// trivially one writer; with several, "one writer per account" has to be enforced, and it
// is — by a lease whose epoch is asserted inside every write transaction (see
// engine.Lease).
//
// Second, the invariants stop being SQL. A fill settles as two halves in two partitions,
// which after Phase 4 means two DATABASES, so no join can see both. Conservation becomes
// something a reconciler computes by gathering aggregates from every partition and adding
// them up in application code. That is a real loss of convenience and it is not avoidable:
// it is the price of the partitioning that makes the system scale, and pretending
// otherwise would mean a single shared database and no partitioning at all.
package cluster

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/marketdata"
)

// Router maps accounts to partitions.
//
// It uses the SAME hash as the event log's partitioner, deliberately. With equal counts,
// core partition P and outcome partition P then cover exactly the same set of accounts, so
// a core process consumes precisely the outcomes for the accounts it owns. Any other
// arrangement would have a core partition consuming outcomes for accounts it cannot write,
// and the fix would be a shuffle that this equality makes unnecessary.
type Router struct{ n int32 }

// NewRouter builds a router over n partitions.
func NewRouter(n int) Router {
	if n < 1 {
		n = 1
	}
	return Router{n: int32(n)}
}

// For returns the partition that owns an account.
func (r Router) For(account uuid.UUID) int32 {
	return eventlog.PartitionFor(account.String(), r.n)
}

// Count is the number of partitions.
func (r Router) Count() int { return int(r.n) }

// Partition is one core partition: its own database, its own lease, its own engine.
type Partition struct {
	ID     int
	Pool   *pgxpool.Pool
	Engine *engine.Engine
	Lease  *engine.Lease
}

// Set is every partition this process owns.
type Set struct {
	Router     Router
	Partitions []*Partition
}

// Options configures a partition set.
type Options struct {
	DSNs          []string // one per partition; length defines the partition count
	Engine        engine.Config
	LeaseTTL      time.Duration
	AcquireLeases bool
}

// Open connects every partition, verifies its identity, and takes its lease.
func Open(ctx context.Context, md *marketdata.Cache, opts Options) (*Set, error) {
	if len(opts.DSNs) == 0 {
		return nil, fmt.Errorf("no partition DSNs configured")
	}

	s := &Set{Router: NewRouter(len(opts.DSNs))}
	for i, dsn := range opts.DSNs {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			s.Close(ctx)
			return nil, fmt.Errorf("partition %d: %w", i, err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			s.Close(ctx)
			return nil, fmt.Errorf("partition %d unreachable: %w", i, err)
		}

		eng := engine.New(pool, md, opts.Engine)
		// Fail fast if this database is not the partition we think it is. Writing an
		// account's ledger into the wrong partition produces perfectly balanced,
		// perfectly misplaced records that no invariant can detect.
		if err := eng.AssertOwnership(ctx, i); err != nil {
			pool.Close()
			s.Close(ctx)
			return nil, err
		}

		p := &Partition{ID: i, Pool: pool, Engine: eng}
		if opts.AcquireLeases {
			lease, err := engine.AcquireLease(ctx, pool, i, opts.LeaseTTL)
			if err != nil {
				pool.Close()
				s.Close(ctx)
				return nil, err
			}
			eng.SetLease(lease)
			p.Lease = lease
		}
		s.Partitions = append(s.Partitions, p)
	}
	return s, nil
}

// For returns the partition that owns an account.
func (s *Set) For(account uuid.UUID) *Partition {
	return s.Partitions[s.Router.For(account)]
}

// EngineFor returns the engine that may write an account.
func (s *Set) EngineFor(account uuid.UUID) *engine.Engine { return s.For(account).Engine }

// RunLeaseRenewers keeps every lease alive until the context is cancelled.
func (s *Set) RunLeaseRenewers(ctx context.Context, onLost func(int, error)) {
	for _, p := range s.Partitions {
		if p.Lease == nil {
			continue
		}
		part := p
		if onLost != nil {
			part.Lease.OnLost = func(err error) { onLost(part.ID, err) }
		}
		go part.Lease.RunRenewer(ctx)
	}
}

// Close releases every lease and pool.
func (s *Set) Close(ctx context.Context) {
	for _, p := range s.Partitions {
		if p.Lease != nil {
			_ = p.Lease.Release(ctx)
		}
		if p.Pool != nil {
			p.Pool.Close()
		}
	}
	s.Partitions = nil
}

// ---------------------------------------------------------------------------
// cross-partition reconciliation
// ---------------------------------------------------------------------------

// GlobalInvariants is the verification block for the whole cluster.
//
// Compare it with engine.Invariants, which covers one partition. The per-partition checks
// still matter — they catch a broken trigger or a bad migration — but they are structurally
// incapable of noticing that a fill settled in partition 2 and never settled in partition
// 5. Only these can.
type GlobalInvariants struct {
	Partitions int

	// PerPartition holds each partition's local verification block. Any failure here is
	// a local bug, and is reported separately so the two classes are not confused.
	PerPartition []engine.Invariants

	// ClearingByUnit is the sum of every CLEARING account across every partition, per
	// unit of account. Zero once every fill has both halves settled: the two halves'
	// clearing legs are equal and opposite and live in different databases, so this is
	// the number that proves a bilateral fill actually completed.
	ClearingByUnit map[string]int64

	// OrphanedFills are fills whose halves do not add up to two across the whole cluster,
	// and which are old enough that they should have. In flight is normal; stuck is not.
	OrphanedFills     []uuid.UUID
	InFlightFills     int
	CashConservation  int64
	ShareConservation map[string]int64
}

// OK reports whether the cluster is sound.
func (g GlobalInvariants) OK() bool {
	for _, inv := range g.PerPartition {
		if !inv.OK() {
			return false
		}
	}
	if len(g.OrphanedFills) > 0 || g.CashConservation != 0 {
		return false
	}
	for _, v := range g.ClearingByUnit {
		if v != 0 {
			return false
		}
	}
	for _, v := range g.ShareConservation {
		if v != 0 {
			return false
		}
	}
	return true
}

// String renders the cluster verification block.
func (g GlobalInvariants) String() string {
	s := fmt.Sprintf("Partitions:                %d\n", g.Partitions)
	for i, inv := range g.PerPartition {
		if !inv.OK() {
			s += fmt.Sprintf("  partition %d LOCAL VIOLATION:\n%s", i, inv)
		}
	}
	s += fmt.Sprintf("In-flight fills:           %d  (settling now; not a violation)\n", g.InFlightFills)
	s += fmt.Sprintf("Orphaned fills:            %d\n", len(g.OrphanedFills))
	s += fmt.Sprintf("Cash conservation delta:   %d\n", g.CashConservation)

	units := make([]string, 0, len(g.ClearingByUnit))
	for u := range g.ClearingByUnit {
		units = append(units, u)
	}
	sort.Strings(units)
	for _, u := range units {
		s += fmt.Sprintf("Clearing open  %-10s %d\n", u+":", g.ClearingByUnit[u])
	}

	syms := make([]string, 0, len(g.ShareConservation))
	for k := range g.ShareConservation {
		syms = append(syms, k)
	}
	sort.Strings(syms)
	for _, sym := range syms {
		s += fmt.Sprintf("Share conservation %-6s %d\n", sym+":", g.ShareConservation[sym])
	}
	return s
}

// Reconcile gathers every partition's aggregates and combines them.
//
// The combination happens in Go because it cannot happen in SQL: the terms live in
// different databases. That is the structural consequence of partitioning, and it is why
// this is a service rather than a query.
func (s *Set) Reconcile(ctx context.Context, settleWindow time.Duration) (GlobalInvariants, error) {
	g := GlobalInvariants{
		Partitions:        len(s.Partitions),
		ClearingByUnit:    map[string]int64{},
		ShareConservation: map[string]int64{},
	}

	// fill_id → halves settled anywhere, and the oldest half's age.
	halves := map[uuid.UUID]int{}
	oldest := map[uuid.UUID]time.Time{}

	for _, p := range s.Partitions {
		local, err := p.Engine.CheckInvariants(ctx)
		if err != nil {
			return g, fmt.Errorf("partition %d invariants: %w", p.ID, err)
		}
		g.PerPartition = append(g.PerPartition, local)

		slice, err := p.Engine.ReconcileSlice(ctx)
		if err != nil {
			return g, fmt.Errorf("partition %d slice: %w", p.ID, err)
		}
		for unit, amt := range slice.ClearingByUnit {
			g.ClearingByUnit[unit] += amt
		}
		for sym, qty := range slice.SharesBySymbol {
			g.ShareConservation[sym] += qty
		}
		g.CashConservation += slice.CashDelta
		for id, n := range slice.FillHalves {
			halves[id] += n
			if t, ok := slice.FillFirstSeen[id]; ok {
				if prev, seen := oldest[id]; !seen || t.Before(prev) {
					oldest[id] = t
				}
			}
		}
	}

	cutoff := time.Now().Add(-settleWindow)
	for id, n := range halves {
		if n == 2 {
			continue
		}
		if t, ok := oldest[id]; ok && t.After(cutoff) {
			g.InFlightFills++
			continue
		}
		g.OrphanedFills = append(g.OrphanedFills, id)
	}
	sort.Slice(g.OrphanedFills, func(i, j int) bool {
		return g.OrphanedFills[i].String() < g.OrphanedFills[j].String()
	})
	return g, nil
}

// PartitionDSNs derives one DSN per partition from a base DSN by renaming the database.
//
// Separate databases, not separate schemas and not a shared table with a partition column.
// The point of partitioning is that each partition's writes contend only with themselves:
// its own fsync, its own lock table, its own connection pool. A shared database would give
// the topology of a partitioned system with the throughput ceiling of a single one, and
// the scaling curve would flatten immediately for a reason that had nothing to do with the
// design being measured.
func PartitionDSNs(base string, n int) ([]string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse base DSN: %w", err)
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return nil, fmt.Errorf("base DSN has no database name")
	}

	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		c := *u
		c.Path = "/" + PartitionDBName(name, i)
		out = append(out, c.String())
	}
	return out, nil
}

// PartitionDBName is the database name for one partition.
func PartitionDBName(base string, i int) string { return fmt.Sprintf("%s_p%d", base, i) }

// Bootstrap creates the partition databases if they do not exist. It connects to the base
// database only to issue CREATE DATABASE, which cannot run inside a transaction.
func Bootstrap(ctx context.Context, base string, n int) ([]string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	baseName := strings.TrimPrefix(u.Path, "/")

	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		return nil, fmt.Errorf("connect for bootstrap: %w", err)
	}
	defer admin.Close()

	for i := 0; i < n; i++ {
		name := PartitionDBName(baseName, i)
		var exists bool
		if err := admin.QueryRow(ctx,
			`SELECT true FROM pg_database WHERE datname = $1`, name).Scan(&exists); err == nil {
			continue
		}
		// Identifiers cannot be parameterised; the name is derived from a validated base
		// plus an integer, so there is nothing here an attacker could reach.
		if _, err := admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			return nil, fmt.Errorf("create %s: %w", name, err)
		}
	}
	return PartitionDSNs(base, n)
}
