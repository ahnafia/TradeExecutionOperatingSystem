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

// Pipeline owns every stage and the log between them.
type Pipeline struct {
	pool      *pgxpool.Pool
	Cfg       Config
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

	return &Pipeline{
		pool: pool, Cfg: cfg, Log: log, Engine: eng, Relay: relay,
		Matching: match, Consumers: consumers,
	}, nil
}

// Drain pumps every stage until nothing moves.
//
// The loop matters more than it looks. One pass is not enough: publishing an order lets
// the book produce fills, applying those fills can terminate an order, and a cancel
// verdict can arrive after the fills it lost to. Quiescence is the only correct stopping
// condition, and reaching it is what makes an asynchronous pipeline deterministic to test.
func (p *Pipeline) Drain(ctx context.Context) error {
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
	return p.Matching.SnapshotAll(ctx)
}

// Run starts every stage in the background.
func (p *Pipeline) Run(ctx context.Context, onErr func(error)) {
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
