// Package pipeline wires the split services together.
//
// After Phase 3 the system is three moving parts joined by a log: the trading core accepts
// and settles, the outbox relay publishes, the matching engine matches. In a deployment
// each runs in its own process. For the demo, the CLI, and the tests they run in one, but
// they still communicate only through the log — there is no shortcut path and no shared
// memory between them, so what the tests exercise is what the deployment does.
//
// The one thing this adds beyond wiring is Drain: pump every stage until the system is
// quiet. An asynchronous pipeline is otherwise untestable without sleeping and hoping, and
// a CLI that returned before an order had settled would be reporting on a system it had
// not waited for.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/matching"
	"github.com/ahnafia/trading-system/internal/outbox"
)

// Config describes the shape of the deployment.
type Config struct {
	InboundPartitions int32 // ceiling on matching shards
	OutcomePartitions int32 // ceiling on core partitions
	ShardCount        int   // matching shards actually running here
	Engine            engine.Config
	Matching          matching.Config
	RelayBatch        int
	KafkaSeeds        []string // non-empty selects a broker

	// Unfenced skips the singleton lease. Only correct where exactly one process can
	// exist by construction: a test, or a chaos run that owns its whole database.
	Unfenced bool

	// Ephemeral selects the in-process log. It is ONLY correct when one process owns the
	// whole lifetime of the data — a test, or a single run of chaos. Any deployment where
	// state outlives the process needs a durable log, because consumer offsets are durable
	// and a durable cursor into a non-durable stream loses records silently.
	Ephemeral bool
}

// DefaultConfig is the single-node playground.
func DefaultConfig() Config {
	return Config{
		InboundPartitions: 4,
		OutcomePartitions: 4,
		ShardCount:        1,
		Engine:            engine.DefaultConfig(),
		Matching:          matching.DefaultConfig(),
		RelayBatch:        256,
	}
}

// PrimaryLease names the singleton role: relay, matching, and outcome consumers.
//
// They are one role rather than three because they must not be split across processes.
// A relay in one process and a matching engine in another would work, but a second
// matching engine anywhere is corruption, and one name is far harder to get wrong.
const PrimaryLease = "engine-primary"

// Pipeline owns every stage and the log between them.
type Pipeline struct {
	pool *pgxpool.Pool
	Cfg  Config

	// Primary is true when this process holds the lease and therefore drives the
	// pipeline. A process that does not hold it can still ACCEPT orders — that path is
	// fenced per-partition and is safe from anywhere — but it must not run the loops.
	Primary   bool
	lease     *engine.ServiceLease
	Log       eventlog.Log
	Engine    *engine.Engine
	Relay     *outbox.Relay
	Matching  *matching.Service
	Consumers []*engine.OutcomeConsumer
}

// New builds and recovers every stage.
func New(ctx context.Context, pool *pgxpool.Pool, md *marketdata.Cache, cfg Config) (*Pipeline, error) {
	topics := eventlog.Topics(cfg.InboundPartitions, cfg.OutcomePartitions)

	var (
		log eventlog.Log
		err error
	)
	switch {
	case len(cfg.KafkaSeeds) > 0:
		log, err = eventlog.NewKafkaLog(ctx, cfg.KafkaSeeds, topics)
	case cfg.Ephemeral:
		log = eventlog.NewMemLog(topics)
	default:
		log, err = eventlog.NewPgLog(ctx, pool, topics)
	}
	if err != nil {
		return nil, err
	}

	// The collar the core sizes reservations against and the collar the book enforces must
	// be the same number. If they diverge, a fill can cost more than was reserved for it,
	// and the guarantee that cash cannot go negative quietly stops holding.
	cfg.Matching.CollarBps = cfg.Engine.CollarBps
	cfg.Matching.ShardCount = cfg.ShardCount

	eng := engine.New(pool, md, cfg.Engine)
	relay := outbox.New(pool, log, cfg.RelayBatch)

	match := matching.New(pool, log, cfg.Matching)
	if err := match.Recover(ctx); err != nil {
		log.Close()
		return nil, fmt.Errorf("recover matching: %w", err)
	}

	var consumers []*engine.OutcomeConsumer
	for p := int32(0); p < log.Partitions(eventlog.TopicOrdersOutcomes); p++ {
		c, err := eng.NewOutcomeConsumer(ctx, log, p, 256)
		if err != nil {
			log.Close()
			return nil, fmt.Errorf("outcome consumer %d: %w", p, err)
		}
		consumers = append(consumers, c)
	}

	p := &Pipeline{
		pool: pool, Cfg: cfg, Log: log, Engine: eng, Relay: relay,
		Matching: match, Consumers: consumers,
	}

	// Claim the right to drive the pipeline. Refused means another process is already
	// doing it, which is the normal state for a CLI invocation against a running server
	// and for the old instance during a rolling deploy.
	if !cfg.Unfenced {
		lease, got, err := engine.AcquireService(ctx, pool, PrimaryLease, engine.LeaseTTL)
		if err != nil {
			log.Close()
			return nil, err
		}
		p.Primary, p.lease = got, lease
	} else {
		p.Primary = true
	}
	return p, nil
}

// Drain pumps every stage until nothing moves.
//
// The loop matters more than it looks. One pass is not enough: publishing an order lets
// the book produce fills, applying those fills can terminate an order, and a cancel
// verdict can arrive after the fills it lost to. Quiescence is the only correct stopping
// condition, and reaching it is what makes an asynchronous pipeline deterministic to test.
func (p *Pipeline) Drain(ctx context.Context) error {
	// Another process is driving. Pumping the stages here would be a second matching
	// engine — the exact thing the lease exists to prevent — so wait for the holder to do
	// the work instead of doing it ourselves.
	if !p.Primary {
		return p.waitForPrimary(ctx)
	}
	const maxRounds = 1000
	for round := 0; round < maxRounds; round++ {
		moved := 0

		n, err := p.Relay.Drain(ctx)
		if err != nil {
			return fmt.Errorf("relay: %w", err)
		}
		moved += n

		if n, err = p.Matching.PumpOnce(ctx); err != nil {
			return fmt.Errorf("matching: %w", err)
		}
		moved += n

		for _, c := range p.Consumers {
			if n, err = c.PumpOnce(ctx); err != nil {
				return fmt.Errorf("outcomes: %w", err)
			}
			moved += n
		}

		if moved == 0 {
			return nil
		}
	}
	return fmt.Errorf("pipeline did not settle after %d rounds", maxRounds)
}

// waitForPrimary blocks until the process holding the lease has caught up: the outbox is
// drained and every outcome partition has been consumed to its end.
func (p *Pipeline) waitForPrimary(ctx context.Context) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		backlog, _, err := p.Relay.Lag(ctx)
		if err != nil {
			return err
		}
		caughtUp, err := p.outcomesCaughtUp(ctx)
		if err != nil {
			return err
		}
		if backlog == 0 && caughtUp {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for the primary process to settle "+
				"(outbox backlog %d); is it running?", backlog)
		}
		time.Sleep(60 * time.Millisecond)
	}
}

// outcomesCaughtUp reports whether every outcome partition has been consumed to its end.
func (p *Pipeline) outcomesCaughtUp(ctx context.Context) (bool, error) {
	pg, ok := p.Log.(*eventlog.PgLog)
	if !ok {
		return true, nil // only the durable log is shared between processes
	}
	for i := int32(0); i < p.Log.Partitions(eventlog.TopicOrdersOutcomes); i++ {
		end, err := pg.Len(ctx, eventlog.TopicOrdersOutcomes, i)
		if err != nil {
			return false, err
		}
		var next int64
		err = p.pool.QueryRow(ctx, `
			SELECT coalesce(max(next_offset), 0) FROM consumer_offsets
			 WHERE consumer_group = $1 AND topic = $2 AND partition = $3`,
			engine.ConsumerGroup, eventlog.TopicOrdersOutcomes, i).Scan(&next)
		if err != nil {
			return false, err
		}
		if next < end {
			return false, nil
		}
	}
	return true, nil
}

// Settle drains the pipeline and then checkpoints the books.
//
// The checkpoint is what keeps a short-lived process cheap. A matching shard recovers from
// its latest snapshot plus the log tail, so a process that never snapshots leaves the next
// one replaying from offset zero — fine for the first few commands, O(history) by the
// hundredth. Any command that drains should also leave a checkpoint behind.
func (p *Pipeline) Settle(ctx context.Context) error {
	if err := p.Drain(ctx); err != nil {
		return err
	}
	if !p.Primary {
		return nil // the holder owns the books, and therefore owns their snapshots
	}
	return p.Matching.SnapshotAll(ctx)
}

// Run drives the pipeline, waiting for the right to do so if another process has it.
//
// The waiting is the important part and it is not optional. An instance that failed to
// acquire once and gave up would serve HTTP forever while never matching a single order —
// which is exactly what happens on any restart inside the lease TTL, including every
// rolling deploy. So a process that starts as a client keeps asking, and promotes itself
// the moment the previous holder releases or expires.
func (p *Pipeline) Run(ctx context.Context, onErr func(error)) {
	if p.Primary {
		p.startLoops(ctx, onErr)
		return
	}
	go p.awaitPromotion(ctx, onErr)
}

// awaitPromotion polls for the lease until it is free, then starts driving.
func (p *Pipeline) awaitPromotion(ctx context.Context, onErr func(error)) {
	// A third of the TTL: fast enough that a handover is not noticeable, slow enough that
	// a standby is not hammering the table.
	t := time.NewTicker(engine.LeaseTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lease, got, err := engine.AcquireService(ctx, p.pool, PrimaryLease, engine.LeaseTTL)
		if err != nil {
			if onErr != nil && ctx.Err() == nil {
				onErr(err)
			}
			continue
		}
		if !got {
			continue
		}
		p.lease, p.Primary = lease, true
		slog.Info("promoted to primary; driving relay, matching, and settlement")
		p.startLoops(ctx, onErr)
		return
	}
}

func (p *Pipeline) startLoops(ctx context.Context, onErr func(error)) {
	if p.lease != nil {
		go p.lease.RunRenewer(ctx, onErr)
	}
	go p.Relay.Run(ctx, 20*time.Millisecond, onErr)
	go p.Matching.Run(ctx, 20*time.Millisecond, onErr)
	for _, c := range p.Consumers {
		go c.Run(ctx, 20*time.Millisecond, onErr)
	}
}

// Close releases the log and its readers.
func (p *Pipeline) Close() error {
	for _, c := range p.Consumers {
		_ = c.Close()
	}
	// Releasing rather than letting it expire is what makes a handover take milliseconds
	// instead of a full TTL, which is the difference between a rolling deploy that stalls
	// the market for ten seconds and one nobody notices.
	if p.lease != nil {
		_ = p.lease.Release(context.WithoutCancel(context.Background()))
	}
	return p.Log.Close()
}

// Lag reports the outbox backlog and the outcome consumers' positions, for the status page.
type Lag struct {
	OutboxBacklog int64
	OutboxAge     time.Duration
	Offsets       map[int32]int64
}

// Lag samples the pipeline's backpressure.
func (p *Pipeline) Lag(ctx context.Context) (Lag, error) {
	backlog, age, err := p.Relay.Lag(ctx)
	if err != nil {
		return Lag{}, err
	}
	offsets := make(map[int32]int64, len(p.Consumers))
	for i, c := range p.Consumers {
		offsets[int32(i)] = c.Offset()
	}
	return Lag{OutboxBacklog: backlog, OutboxAge: age, Offsets: offsets}, nil
}

// RestartMatching rebuilds the matching service from its durable state, as a crashed and
// restarted shard would: books from the latest snapshot, then the tail of the log replayed.
//
// Everything the shard had in memory is discarded, including any fills it generated but
// whose outcomes had not yet been consumed. Those get regenerated on the replay — with the
// same identities — and the core recognises and ignores them. That is the property this
// exists to exercise.
func (p *Pipeline) RestartMatching(ctx context.Context) error {
	cfg := p.Cfg.Matching
	cfg.CollarBps = p.Cfg.Engine.CollarBps
	cfg.ShardCount = p.Cfg.ShardCount

	fresh := matching.New(p.pool, p.Log, cfg)
	if err := fresh.Recover(ctx); err != nil {
		return fmt.Errorf("restart matching: %w", err)
	}
	p.Matching = fresh
	return nil
}

// RestartConsumers rebuilds every outcome consumer from its durable offset, as a restarted
// core process would. Anything applied but not committed is redelivered and reapplied,
// which is safe precisely because offsets advance in the same transaction as the state.
func (p *Pipeline) RestartConsumers(ctx context.Context) error {
	for _, c := range p.Consumers {
		_ = c.Close()
	}
	p.Consumers = nil
	for i := int32(0); i < p.Log.Partitions(eventlog.TopicOrdersOutcomes); i++ {
		c, err := p.Engine.NewOutcomeConsumer(ctx, p.Log, i, 256)
		if err != nil {
			return fmt.Errorf("restart consumer %d: %w", i, err)
		}
		p.Consumers = append(p.Consumers, c)
	}
	return nil
}

// Duplicator exposes the log's fault-injection hook, if it has one.
//
// Both the in-process and Postgres logs can duplicate records on demand; a real broker
// cannot be asked to, so over Kafka duplication is induced by killing the relay between
// producing and marking rows published instead.
type Duplicator interface {
	DuplicateWhen(func(eventlog.Record) bool)
}

// Duplicator returns the injection hook, or nil if this transport has none.
func (p *Pipeline) Duplicator() Duplicator {
	if d, ok := p.Log.(Duplicator); ok {
		return d
	}
	return nil
}
