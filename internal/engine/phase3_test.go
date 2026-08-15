package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/events"
	"github.com/ahnafia/trading-system/internal/money"
)

// twoSidedMarket builds a maker with inventory and resting asks, plus a funded taker.
func twoSidedMarket(t *testing.T, ctx context.Context, r *rig, sym string, levels int) (uuid.UUID, uuid.UUID) {
	t.Helper()
	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(20_000), 15000); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < levels; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: maker, ClientOrderID: fmt.Sprintf("ask-%d", i), Symbol: sym,
			Side: book.Sell, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(20), LimitPrice: money.Minor(15000 + i*10),
		}); err != nil {
			t.Fatal(err)
		}
	}
	r.drain(ctx)
	return maker, taker
}

// totalCash sums every account's cash, so a test can assert that a replay moved nothing
// without enumerating accounts.
func (r *rig) totalCash(ctx context.Context) int64 {
	r.t.Helper()
	return r.countRows(ctx, `SELECT coalesce(sum(cash), 0) FROM account_balances`)
}

// The transactional outbox is at-least-once by construction: a relay that crashes between
// producing and marking a row published will produce it again. A republished order must
// not become a second resting order — the two fills it would generate are genuinely
// distinct events, so no downstream idempotency key would catch them.
func TestDuplicatePublishDoesNotDoubleTheBook(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	// Every inbound record is published twice, which is the worst case a crashing relay
	// can produce.
	r.log.DuplicateWhen(func(rec eventlog.Record) bool {
		return rec.Topic == eventlog.TopicOrdersInbound
	})

	maker := r.account(ctx, "maker", money.Minor(50_000_000_00))
	taker := r.account(ctx, "taker", money.Minor(5_000_000_00))
	if err := r.eng.SeedShares(ctx, maker, sym, money.FromShares(20_000), 15000); err != nil {
		t.Fatal(err)
	}

	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: maker, ClientOrderID: "ask", Symbol: sym,
		Side: book.Sell, Type: book.Limit, TIF: book.GTC,
		Qty: money.FromShares(50), LimitPrice: 15000,
	}); err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	// Confirm the duplication actually happened, or the test proves nothing.
	var inbound int64
	for p := int32(0); p < testInboundPartitions; p++ {
		inbound += r.log.Len(eventlog.TopicOrdersInbound, p)
	}
	if inbound != 2 {
		t.Fatalf("expected the record to be published twice, log holds %d", inbound)
	}

	// The book must hold 50 shares, not 100. Sweep with a market order big enough to take
	// everything a doubled book would offer.
	view, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "sweep", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	settled, err := r.eng.Order(ctx, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.FilledQty != money.FromShares(50) {
		t.Fatalf("duplicate publish created liquidity: filled %s, want 50", settled.FilledQty)
	}
	r.requireInvariants(ctx)
}

// Redelivery of the outcome stream must move no money.
//
// This is the exactly-once claim stated as a test. The log is entitled to deliver a record
// more than once; what makes the EFFECT happen once is transactions.event_id being unique
// AND deterministic. Rewinding every consumer to zero replays the entire history of
// outcomes — the strongest form of redelivery there is.
func TestOutcomeRedeliveryMovesNoMoney(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)
	_, taker := twoSidedMarket(t, ctx, r, sym, 4)

	for i := 0; i < 3; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: taker, ClientOrderID: fmt.Sprintf("buy-%d", i), Symbol: sym,
			Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(15),
		}); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)
	}

	beforeCash := r.totalCash(ctx)
	beforeTxns := r.countRows(ctx, `SELECT count(*) FROM transactions`)
	beforePostings := r.countRows(ctx, `SELECT count(*) FROM postings`)
	if beforeTxns == 0 {
		t.Fatal("nothing settled; the test would prove nothing")
	}

	// Deliver the entire outcome stream a second time.
	r.rewindOutcomes(ctx)
	r.drain(ctx)

	if got := r.totalCash(ctx); got != beforeCash {
		t.Fatalf("redelivery moved money: %d → %d", beforeCash, got)
	}
	if got := r.countRows(ctx, `SELECT count(*) FROM transactions`); got != beforeTxns {
		t.Fatalf("redelivery created transactions: %d → %d", beforeTxns, got)
	}
	if got := r.countRows(ctx, `SELECT count(*) FROM postings`); got != beforePostings {
		t.Fatalf("redelivery created postings: %d → %d", beforePostings, got)
	}
	r.requireInvariants(ctx)
}

// The consumer's offset must end up exactly at the end of each partition, because it
// advances in the same transaction as the state change it authorizes. An offset that ran
// ahead would mean a record was skipped; one that lagged would mean a record will be
// applied twice (harmless, but it would show as permanent redelivery).
func TestConsumerOffsetTracksTheLogExactly(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)
	_, taker := twoSidedMarket(t, ctx, r, sym, 4)

	if _, err := r.eng.Submit(ctx, SubmitRequest{
		AccountID: taker, ClientOrderID: "buy", Symbol: sym,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(30),
	}); err != nil {
		t.Fatal(err)
	}
	r.drain(ctx)

	for p := int32(0); p < testOutcomePartitions; p++ {
		want := r.log.Len(eventlog.TopicOrdersOutcomes, p)
		if want == 0 {
			continue
		}
		got := r.countRows(ctx, `
			SELECT coalesce(max(next_offset), -1) FROM consumer_offsets
			 WHERE consumer_group = $1 AND topic = $2 AND partition = $3`,
			ConsumerGroup, eventlog.TopicOrdersOutcomes, p)
		if got != want {
			t.Errorf("partition %d: offset %d, log holds %d records", p, got, want)
		}
	}
	r.requireInvariants(ctx)
}

// A matching shard that dies re-derives the fills it had already produced.
//
// This is the recovery path the whole deterministic-identity design exists to make safe.
// The shard restores its books from a snapshot, replays the inbound tail, and regenerates
// fills — with the SAME identities, because they come from (shard, symbol, book_seq)
// rather than being minted fresh. The core sees them, recognises them, and does nothing.
// A random UUID here would double every fill produced after the last snapshot.
func TestMatchingRestartRegeneratesIdenticalFills(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)
	_, taker := twoSidedMarket(t, ctx, r, sym, 6)

	// Checkpoint the books, then trade past the checkpoint so recovery has a tail to
	// replay rather than nothing.
	if err := r.match.SnapshotAll(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: taker, ClientOrderID: fmt.Sprintf("buy-%d", i), Symbol: sym,
			Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(15),
		}); err != nil {
			t.Fatal(err)
		}
		r.drain(ctx)
	}

	beforeFills := r.fillIdentities(ctx)
	beforeCash := r.totalCash(ctx)
	if len(beforeFills) < 3 {
		t.Fatalf("only %d fills before restart; the tail is too short to be interesting", len(beforeFills))
	}

	// Crash and restart: fresh books, fresh consumers, same durable state.
	r.restart(ctx)
	r.drain(ctx)

	afterFills := r.fillIdentities(ctx)
	if len(afterFills) != len(beforeFills) {
		t.Fatalf("restart changed the fill count: %d → %d", len(beforeFills), len(afterFills))
	}
	for id := range beforeFills {
		if _, ok := afterFills[id]; !ok {
			t.Fatalf("fill %s disappeared across restart", id)
		}
	}
	if got := r.totalCash(ctx); got != beforeCash {
		t.Fatalf("restart moved money: %d → %d", beforeCash, got)
	}
	r.requireInvariants(ctx)
}

// fillIdentities returns every settled fill id.
func (r *rig) fillIdentities(ctx context.Context) map[uuid.UUID]struct{} {
	r.t.Helper()
	rows, err := r.pool.Query(ctx, `SELECT fill_id FROM fills`)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	out := map[uuid.UUID]struct{}{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			r.t.Fatal(err)
		}
		out[id] = struct{}{}
	}
	return out
}

// Everything the core enqueues must be readable by the matching engine, and vice versa.
// A schema version the reader does not understand must be REFUSED rather than skipped:
// advancing past an unreadable record would drop an order and leave no trace of why.
func TestUnreadableRecordStopsTheConsumerRatherThanSkipping(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	// A record from a future schema version, on a partition the shard owns.
	future := []byte(`{"schema_version":99,"type":"ORDER_ACCEPTED","payload":{}}`)
	if err := r.log.Produce(ctx, eventlog.Record{
		Topic: eventlog.TopicOrdersInbound, Key: sym, Value: future,
	}); err != nil {
		t.Fatal(err)
	}

	err := r.tryDrain(ctx)
	if err == nil {
		t.Fatal("a record from an unknown schema version was silently consumed")
	}
	t.Logf("refused as expected: %v", err)

	// And the envelope decoder is the thing that refused it, not something incidental.
	if _, decErr := events.Decode(future); decErr == nil {
		t.Fatal("events.Decode accepted a future schema version")
	}
}

// The relay must publish in outbox id order. Two orders for one symbol reaching the book
// out of order is price-time priority being wrong, which is the one thing a matching
// engine may not get wrong.
func TestRelayPublishesInOrder(t *testing.T) {
	ctx, r := newRig(t)
	const sym = "ACME"
	r.mark(sym, 15000)

	acct := r.account(ctx, "acct", money.Minor(50_000_000_00))
	var ids []uuid.UUID
	for i := 0; i < 25; i++ {
		v, err := r.eng.Submit(ctx, SubmitRequest{
			AccountID: acct, ClientOrderID: fmt.Sprintf("bid-%d", i), Symbol: sym,
			Side: book.Buy, Type: book.Limit, TIF: book.GTC,
			Qty: money.FromShares(5), LimitPrice: 14000, // all at one price: pure time priority
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, v.ID)
	}
	r.drain(ctx)

	// Read the inbound partition back and check the orders appear in submission order.
	partition := eventlog.PartitionFor(sym, testInboundPartitions)
	reader, err := r.log.Reader(eventlog.TopicOrdersInbound, partition, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	recs, err := reader.Fetch(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(ids) {
		t.Fatalf("expected %d records on the partition, got %d", len(ids), len(recs))
	}
	for i, rec := range recs {
		env, err := events.Decode(rec.Value)
		if err != nil {
			t.Fatal(err)
		}
		msg, err := events.Into[events.OrderAccepted](env)
		if err != nil {
			t.Fatal(err)
		}
		if msg.OrderID != ids[i] {
			t.Fatalf("record %d is order %s, expected %s — the relay reordered", i, msg.OrderID, ids[i])
		}
	}
}
