// Package matching is the matching engine, sharded by symbol.
//
// It owns order books and nothing else. It has no database of account state, no idea what
// cash is, and no way to ask: its entire input is one ordered stream per partition, and
// its entire output is another. That isolation is what makes it replayable, and
// replayability is what makes its crash recovery safe.
//
// # Recovery without consumer offsets
//
// This service deliberately does not use the consumer_offsets table that the trading core
// relies on. Its durable state IS its book snapshot, and the snapshot carries the offset
// it reflects. On restart it restores the books and replays the inbound stream from that
// offset, regenerating every fill it had already produced.
//
// That regeneration is the interesting part. Because fill identities are derived from
// (shard, symbol, book_seq) rather than minted randomly, a re-derived fill is the SAME
// fill — the core's unique constraint recognises it and does nothing. So the matching
// engine is free to be at-least-once about its output, and does not need a transaction
// spanning "I decided this" and "I told someone", which it has no way to obtain anyway.
package matching

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/events"
	"github.com/ahnafia/trading-system/internal/ids"
	"github.com/ahnafia/trading-system/internal/money"
)

// Config tunes a shard.
type Config struct {
	ShardID       int
	ShardCount    int   // total shards; this one owns partitions where p % ShardCount == ShardID
	CollarBps     int64 // must match the core's, or reservations stop bounding fills
	DedupWindow   int
	SnapshotEvery int // records consumed between book snapshots
	BatchSize     int

	// Venues a symbol can trade on. One venue is the ordinary case; more than one turns
	// order handling into a routing decision (see router.go).
	Venues []Venue
}

// defaultDedupWindow is how many terminated order ids a book remembers.
//
// It only has to exceed the outbox relay's worst lag — the window in which a republished
// record could still arrive — not the number of orders in a day. Relay lag is monitored
// and is normally milliseconds, so 65k is already several orders of magnitude of headroom.
//
// The previous value of 1<<20 was chosen for a benchmark and never revisited. It is
// allocated once per book and once per partition, so on a three-symbol deployment it was
// seven copies of a structure sized for a million entries.
const defaultDedupWindow = 1 << 16

// DefaultConfig is a single shard owning every partition.
func DefaultConfig() Config {
	return Config{
		ShardID:       0,
		ShardCount:    1,
		CollarBps:     500,
		DedupWindow:   defaultDedupWindow,
		SnapshotEvery: 500,
		BatchSize:     256,
		Venues:        DefaultVenues(),
	}
}

// Service runs the books for the partitions this shard owns.
type Service struct {
	cfg  Config
	log  eventlog.Log
	pool *pgxpool.Pool

	mu    sync.Mutex
	parts map[int32]*partitionState

	// OnOutcome is called for each outcome produced, for metrics.
	OnOutcome func(kind string)

	// suppressed counts inbound records rejected as duplicates. It is the only direct
	// evidence that at-least-once delivery actually happened, which makes it the number
	// that tells you a chaos run's duplication was real rather than configured.
	suppressed int64
}

// Suppressed is how many duplicate inbound records this shard has ignored.
func (s *Service) Suppressed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.suppressed
}

// partitionState is one consume position and the books it covers.
type partitionState struct {
	partition int32
	reader    eventlog.Reader
	next      int64                            // offset to read next
	books     map[string]map[string]*book.Book // symbol → venue → book
	sinceSnap int

	// Duplicate suppression lives on the SHARD, not on each book.
	//
	// With routing, one order can touch several venues, and per-book rings would let a
	// republished record slip through: the venues the original happened to reach would
	// reject it, and any venue it did not reach would accept it and fill again. The shard
	// is the only place that sees the whole routing decision, so it is the only place the
	// decision can be suppressed as a unit.
	seen     map[uuid.UUID]struct{}
	seenRing []uuid.UUID
	seenNext int
}

func (st *partitionState) alreadySeen(id uuid.UUID) bool {
	_, ok := st.seen[id]
	return ok
}

func (st *partitionState) remember(id uuid.UUID) {
	if len(st.seenRing) == 0 {
		return
	}
	if _, ok := st.seen[id]; ok {
		return
	}
	if old := st.seenRing[st.seenNext]; old != uuid.Nil {
		delete(st.seen, old)
	}
	st.seenRing[st.seenNext] = id
	st.seen[id] = struct{}{}
	st.seenNext = (st.seenNext + 1) % len(st.seenRing)
}

// venueBooks returns (creating if needed) every venue's book for a symbol.
func (s *Service) venueBooks(st *partitionState, symbol string) map[string]*book.Book {
	byVenue, ok := st.books[symbol]
	if !ok {
		byVenue = map[string]*book.Book{}
		st.books[symbol] = byVenue
	}
	for _, v := range s.cfg.Venues {
		if _, ok := byVenue[v.Name]; !ok {
			byVenue[v.Name] = s.newBook(symbol, v.Name)
		}
	}
	return byVenue
}

// New builds a shard. Call Recover before Run.
func New(pool *pgxpool.Pool, log eventlog.Log, cfg Config) *Service {
	if cfg.ShardCount < 1 {
		cfg.ShardCount = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = 500
	}
	return &Service{cfg: cfg, log: log, pool: pool, parts: map[int32]*partitionState{}}
}

// Owns reports whether this shard is responsible for a partition.
//
// Modulo assignment, computed the same way everywhere. A shard that disagreed with the
// core about ownership would silently consume orders for books it does not have, and the
// symptom would look like missing liquidity rather than a routing bug.
func (s *Service) Owns(partition int32) bool {
	return int(partition)%s.cfg.ShardCount == s.cfg.ShardID
}

// Recover restores every owned partition from its latest snapshot.
func (s *Service) Recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := s.log.Partitions(eventlog.TopicOrdersInbound)
	for p := int32(0); p < total; p++ {
		if !s.Owns(p) {
			continue
		}
		st := &partitionState{
			partition: p,
			books:     map[string]map[string]*book.Book{},
			seen:      make(map[uuid.UUID]struct{}, s.cfg.DedupWindow),
			seenRing:  make([]uuid.UUID, s.cfg.DedupWindow),
		}

		offset, snap, err := s.loadSnapshot(ctx, p)
		if err != nil {
			return err
		}
		for symbol, byVenue := range snap {
			for venue, bs := range byVenue {
				b := s.newBook(symbol, venue)
				b.LoadSnapshot(bs)
				if st.books[symbol] == nil {
					st.books[symbol] = map[string]*book.Book{}
				}
				st.books[symbol][venue] = b
			}
		}
		st.next = offset

		reader, err := s.log.Reader(eventlog.TopicOrdersInbound, p, offset)
		if err != nil {
			return fmt.Errorf("reader for partition %d: %w", p, err)
		}
		st.reader = reader
		s.parts[p] = st
	}
	return nil
}

func (s *Service) newBook(symbol, venue string) *book.Book {
	return book.New(symbol, venue, s.cfg.ShardID, s.cfg.CollarBps, s.cfg.DedupWindow)
}

// PumpOnce consumes whatever is currently available on every owned partition and returns
// how many records it processed. Used by the drain loop in tests and by Run.
func (s *Service) PumpOnce(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	for _, st := range s.parts {
		recs, err := st.reader.Fetch(ctx, s.cfg.BatchSize)
		if err != nil {
			return total, err
		}
		for _, rec := range recs {
			if err := s.handle(ctx, st, rec); err != nil {
				return total, err
			}
			st.next = rec.Offset + 1
			st.sinceSnap++
			total++
		}
		if st.sinceSnap >= s.cfg.SnapshotEvery {
			if err := s.snapshot(ctx, st); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// Run consumes until the context is cancelled.
func (s *Service) Run(ctx context.Context, idle time.Duration, onErr func(error)) {
	for {
		n, err := s.PumpOnce(ctx)
		if err != nil && ctx.Err() == nil && onErr != nil {
			onErr(err)
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			s.mu.Lock()
			for _, st := range s.parts {
				_ = s.snapshot(context.WithoutCancel(ctx), st)
			}
			s.mu.Unlock()
			return
		case <-time.After(idle):
		}
	}
}

// handle applies one inbound record and publishes whatever the book decided.
func (s *Service) handle(ctx context.Context, st *partitionState, rec eventlog.Record) error {
	env, err := events.Decode(rec.Value)
	if err != nil {
		// Refusing to advance past a record we cannot read is deliberate. Skipping it
		// would drop an order or a cancel and leave no trace of why.
		return fmt.Errorf("partition %d offset %d: %w", rec.Partition, rec.Offset, err)
	}

	switch env.Type {
	case events.TypeOrderAccepted:
		msg, err := events.Into[events.OrderAccepted](env)
		if err != nil {
			return err
		}
		return s.onOrder(ctx, st, msg)

	case events.TypeCancelRequested:
		msg, err := events.Into[events.CancelRequested](env)
		if err != nil {
			return err
		}
		return s.onCancel(ctx, st, msg)

	default:
		return fmt.Errorf("partition %d offset %d: unexpected inbound type %q",
			rec.Partition, rec.Offset, env.Type)
	}
}

func (s *Service) onOrder(ctx context.Context, st *partitionState, msg events.OrderAccepted) error {
	// Suppressed once, for the whole order, before any venue sees it. See partitionState.
	if st.alreadySeen(msg.OrderID) {
		s.suppressed++
		return nil
	}
	st.remember(msg.OrderID)

	books := s.venueBooks(st, msg.Symbol)
	o := &book.Order{
		ID:         msg.OrderID,
		AccountID:  msg.AccountID,
		Side:       parseSide(msg.Side),
		Type:       parseType(msg.Type),
		TIF:        parseTIF(msg.TIF),
		Qty:        money.Qty(msg.Qty),
		Remaining:  money.Qty(msg.Qty),
		LimitPrice: money.Minor(msg.LimitPrice),
		RefPrice:   money.Minor(msg.RefPrice),
	}

	var (
		out    []eventlog.Record
		fills  []book.Fill
		filled money.Qty
	)

	// Each routed slice goes in as IOC: it must take what the plan said was there and not
	// linger. Resting is a separate decision made once, at the primary venue, so an order
	// cannot end up quietly resting in several places at once — which would multiply the
	// liquidity it represents and let it fill more than its own size.
	for _, step := range s.route(o, books) {
		b := books[step.venue]
		slice := *o
		slice.Qty, slice.Remaining = step.qty, step.qty
		slice.TIF = book.IOC

		res := b.Submit(&slice)
		fills = append(fills, res.Fills...)
		for _, f := range res.Fills {
			filled += f.Qty
		}
	}

	remaining := o.Qty - filled
	disposition := book.Cancelled
	reason := ""
	switch {
	case remaining == 0:
		disposition = book.Complete
	case o.Type == book.Limit && o.TIF == book.GTC:
		// The remainder rests at the primary venue. A resting order lives in exactly one
		// book, which is what keeps a later cancel a single, unambiguous instruction.
		rest := *o
		rest.Qty, rest.Remaining = remaining, remaining
		venue := msg.Venue
		if _, known := books[venue]; !known {
			// An unknown venue rests at the primary rather than being rejected: a client
			// naming a venue this shard does not run is asking for something that does not
			// exist, and silently dropping the order would be worse than posting it
			// somewhere real.
			venue = s.primaryVenue()
		}
		host := books[venue]
		if host == nil {
			host = s.newBook(msg.Symbol, venue)
			books[venue] = host
		}
		res := host.Submit(&rest)
		fills = append(fills, res.Fills...)
		for _, f := range res.Fills {
			filled += f.Qty
			remaining -= f.Qty
		}
		if remaining > 0 {
			disposition = book.Rested
		} else {
			disposition = book.Complete
		}
	default:
		reason = s.cancelReason(books, o, remaining)
	}

	for _, f := range fills {
		takerBuying := f.TakerSide == book.Buy
		half := func(side string, account, order uuid.UUID, buying bool) (eventlog.Record, error) {
			payload := events.FillHalf{
				EventID:      ids.FillEventID(f.ShardID, f.Venue, f.Symbol, f.BookSeq, side),
				FillID:       f.FillID,
				ShardID:      f.ShardID,
				Symbol:       f.Symbol,
				Venue:        f.Venue,
				BookSeq:      f.BookSeq,
				Side:         side,
				AccountID:    account,
				OrderID:      order,
				Price:        int64(f.Price),
				Qty:          int64(f.Qty),
				Buying:       buying,
				TakerOrderID: f.TakerOrderID,
				MakerOrderID: f.MakerOrderID,
			}
			b, err := events.Encode(events.TypeFillHalf, payload)
			// Keyed by ACCOUNT: this is where the sharding domain changes from symbol to
			// account, and the two halves of one fill deliberately go to different
			// partitions because they are settled by different owners.
			return eventlog.Record{Topic: eventlog.TopicOrdersOutcomes, Key: account.String(), Value: b}, err
		}

		takerRec, err := half("TAKER", f.TakerAccount, f.TakerOrderID, takerBuying)
		if err != nil {
			return err
		}
		makerRec, err := half("MAKER", f.MakerAccount, f.MakerOrderID, !takerBuying)
		if err != nil {
			return err
		}
		out = append(out, takerRec, makerRec)
	}

	disp, err := events.Encode(events.TypeDisposition, events.Disposition{
		OrderID:     msg.OrderID,
		AccountID:   msg.AccountID,
		Symbol:      msg.Symbol,
		Disposition: disposition.String(),
		Remaining:   int64(remaining),
		Reason:      reason,
	})
	if err != nil {
		return err
	}
	out = append(out, eventlog.Record{
		Topic: eventlog.TopicOrdersOutcomes, Key: msg.AccountID.String(), Value: disp,
	})

	// Fills first, disposition last, in one produce. Both are keyed by the taker's
	// account, so they land in one partition in this order — which is what lets the core
	// apply every fill before deciding what the order's final state is.
	if err := s.log.Produce(ctx, out...); err != nil {
		return err
	}
	s.observe(events.TypeFillHalf, len(fills)*2)
	s.observe(events.TypeDisposition, 1)
	return nil
}

// cancelReason distinguishes a collar clip from an empty book. Both cancel the remainder,
// but only the first means a risk control fired, and that is a number worth graphing
// separately from ordinary illiquidity.
func (s *Service) cancelReason(books map[string]*book.Book, o *book.Order, remaining money.Qty) string {
	if o.Type != book.Market {
		return "IOC_REMAINDER"
	}
	for _, b := range books {
		bids, asks := b.Depth(1)
		levels := asks
		if o.Side == book.Sell {
			levels = bids
		}
		if len(levels) > 0 {
			// There is liquidity somewhere; the only reason it was not taken is the collar.
			return "COLLAR_BREACH"
		}
	}
	return "BOOK_EXHAUSTED"
}

func (s *Service) onCancel(ctx context.Context, st *partitionState, msg events.CancelRequested) error {
	// A resting order lives at exactly one venue, but which one is not recorded anywhere
	// outside the books, so the cancel asks each in turn. Sorted, so the search is
	// deterministic and a replay resolves it identically.
	books := s.venueBooks(st, msg.Symbol)
	names := make([]string, 0, len(books))
	for name := range books {
		names = append(names, name)
	}
	sort.Strings(names)

	var (
		remaining money.Qty
		removed   bool
	)
	for _, name := range names {
		if q, ok := books[name].Cancel(msg.OrderID); ok {
			remaining, removed = q, true
			break
		}
	}
	reason := "ALREADY_TERMINAL"
	if removed {
		reason = "CLIENT_CANCEL"
	}

	payload, err := events.Encode(events.TypeCancelOutcome, events.CancelOutcome{
		OrderID:   msg.OrderID,
		AccountID: msg.AccountID,
		Symbol:    msg.Symbol,
		Cancelled: removed,
		Remaining: int64(remaining),
		Reason:    reason,
	})
	if err != nil {
		return err
	}
	if err := s.log.Produce(ctx, eventlog.Record{
		Topic: eventlog.TopicOrdersOutcomes, Key: msg.AccountID.String(), Value: payload,
	}); err != nil {
		return err
	}
	s.observe(events.TypeCancelOutcome, 1)
	return nil
}

func (s *Service) observe(kind string, n int) {
	if s.OnOutcome == nil {
		return
	}
	for i := 0; i < n; i++ {
		s.OnOutcome(kind)
	}
}

// --- snapshots -------------------------------------------------------------

const snapshotsKept = 3

// partitionSnapshot is every book in a partition: symbol → venue → state.
type partitionSnapshot struct {
	Books map[string]map[string]book.Snapshot `json:"books"`
}

// Snapshot checkpoints one partition's books at its current offset.
func (s *Service) snapshot(ctx context.Context, st *partitionState) error {
	snap := partitionSnapshot{Books: make(map[string]map[string]book.Snapshot, len(st.books))}
	for symbol, byVenue := range st.books {
		snap.Books[symbol] = make(map[string]book.Snapshot, len(byVenue))
		for venue, b := range byVenue {
			snap.Books[symbol][venue] = b.Snapshot()
		}
	}
	blob, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("encode book snapshot: %w", err)
	}

	if _, err := s.pool.Exec(ctx, `
		INSERT INTO book_snapshots (shard_id, partition, next_offset, state)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (shard_id, partition, next_offset) DO UPDATE SET state = EXCLUDED.state`,
		s.cfg.ShardID, st.partition, st.next, blob); err != nil {
		return fmt.Errorf("write book snapshot: %w", err)
	}

	if _, err := s.pool.Exec(ctx, `
		DELETE FROM book_snapshots
		 WHERE shard_id = $1 AND partition = $2
		   AND next_offset NOT IN (
		     SELECT next_offset FROM book_snapshots
		      WHERE shard_id = $1 AND partition = $2
		      ORDER BY next_offset DESC LIMIT $3)`,
		s.cfg.ShardID, st.partition, snapshotsKept); err != nil {
		return fmt.Errorf("prune book snapshots: %w", err)
	}

	st.sinceSnap = 0
	return nil
}

// SnapshotAll checkpoints every owned partition.
func (s *Service) SnapshotAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.parts {
		if err := s.snapshot(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) loadSnapshot(ctx context.Context, partition int32) (int64, map[string]map[string]book.Snapshot, error) {
	var (
		offset int64
		blob   []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT next_offset, state FROM book_snapshots
		 WHERE shard_id = $1 AND partition = $2
		 ORDER BY next_offset DESC LIMIT 1`, s.cfg.ShardID, partition).Scan(&offset, &blob)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("load book snapshot: %w", err)
	}

	var snap partitionSnapshot
	if err := json.Unmarshal(blob, &snap); err != nil {
		return 0, nil, fmt.Errorf("decode book snapshot for partition %d: %w", partition, err)
	}
	return offset, snap.Books, nil
}

// --- inspection ------------------------------------------------------------

// EnsureBooks creates a symbol's books on every venue this shard owns, without waiting for
// an order to arrive. Books are otherwise created lazily on first use, which is fine in
// production and awkward for anything that wants to inspect or seed a venue up front.
func (s *Service) EnsureBooks(symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target := eventlog.PartitionFor(symbol, s.log.Partitions(eventlog.TopicOrdersInbound))
	for _, st := range s.parts {
		if st.partition != target {
			continue
		}
		s.venueBooks(st, symbol)
	}
}

// Book returns a symbol's primary-venue book, if this shard owns it.
func (s *Service) Book(symbol string) *book.Book { return s.BookAt(symbol, s.primaryVenue()) }

// BookAt returns one venue's book for a symbol.
func (s *Service) BookAt(symbol, venue string) *book.Book {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.parts {
		if byVenue, ok := st.books[symbol]; ok {
			if b, ok := byVenue[venue]; ok {
				return b
			}
		}
	}
	return nil
}

// Symbols lists every book this shard holds.
func (s *Service) Symbols() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, st := range s.parts {
		for sym := range st.books {
			out = append(out, sym)
		}
	}
	sort.Strings(out)
	return out
}

// Positions reports each owned partition's consume offset, for the status page.
func (s *Service) Offsets() map[int32]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int32]int64, len(s.parts))
	for p, st := range s.parts {
		out[p] = st.next
	}
	return out
}

func parseSide(s string) book.Side {
	if s == "SELL" {
		return book.Sell
	}
	return book.Buy
}

func parseType(s string) book.Type {
	if s == "LIMIT" {
		return book.Limit
	}
	return book.Market
}

func parseTIF(s string) book.TIF {
	if s == "GTC" {
		return book.GTC
	}
	return book.IOC
}
