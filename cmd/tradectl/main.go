// Command tradectl is the engine and its operator interface.
//
// There is no browser and no web server on purpose. Part 1's demo surface is a CLI, a
// scenario runner, and the invariant block — for an infrastructure audience that is a
// better demonstration than a UI, because the thing worth showing is that the numbers
// stay zero.
//
// As of Phase 3 the system is services joined by an event log. This binary runs them all
// in one process for the demo and the CLI; the transport between them is the same log a
// deployment uses, so what you exercise here is what runs there.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/chaos"
	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/money"
	"github.com/ahnafia/trading-system/internal/pipeline"
	"github.com/ahnafia/trading-system/migrations"
)

const (
	defaultDSN   = "postgres://trading:trading@localhost:5433/trading?sslmode=disable"
	defaultKafka = "localhost:19092"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() string {
	return strings.TrimSpace(`
tradectl — trading engine (phases 1–3)

  migrate                                    create the schema
  demo                                       run the full scenario end to end
  open-account <label>                       create an account
  deposit <account> <dollars>                credit cash from outside the system
  seed-shares <account> <sym> <shares> <px>  buy opening inventory from outside
  submit <account> <sym> buy|sell market|limit <shares> [price] [ioc|gtc]
  cancel <order-id>
  order <order-id>                           status and fills
  positions <account>
  balances <account>
  book <symbol>
  invariants                                 the verification block
  replay [account]                           rebuild from the ledger and compare
  snapshot                                   checkpoint every account
  serve [addr]                               run under load with /metrics (default :9464)
  chaos [orders]                             run under injected faults, print the block

Prices and dollar amounts are decimals ("150.25").
Env: TRADING_DSN, TRADING_KAFKA (set to use a broker; unset keeps the log in Postgres).`)
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Println(usage())
		return nil
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dsn := os.Getenv("TRADING_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	cmd, args := os.Args[1], os.Args[2:]
	if cmd == "migrate" {
		return migrate(ctx, pool)
	}
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database unreachable at %s (is `docker compose up -d` running?): %w", dsn, err)
	}

	md := marketdata.NewCache()
	pl, err := newPipeline(ctx, pool, md)
	if err != nil {
		return err
	}
	defer pl.Close()
	eng := pl.Engine

	switch cmd {
	case "demo":
		return demo(ctx, pl, md)
	case "serve":
		return serve(ctx, pl, md, args)
	case "open-account":
		return openAccount(ctx, eng, args)
	case "deposit":
		return deposit(ctx, eng, args)
	case "seed-shares":
		return seedShares(ctx, eng, args)
	case "submit":
		return submit(ctx, pl, md, args)
	case "cancel":
		return cancelOrder(ctx, pl, args)
	case "order":
		return showOrder(ctx, eng, args)
	case "positions":
		return positions(ctx, pl, md, args)
	case "balances":
		return showBalances(ctx, eng, args)
	case "book":
		return showBook(ctx, pl, args)
	case "invariants":
		return verify(ctx, eng)
	case "replay":
		return replay(ctx, eng, args)
	case "snapshot":
		return snapshot(ctx, eng)
	case "chaos":
		return runChaos(ctx, pl, md, args)
	default:
		fmt.Println(usage())
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// newPipeline builds the whole system. Set TRADING_KAFKA to run over a broker; leave it
// unset and the same code runs over an in-process log with identical semantics.
func newPipeline(ctx context.Context, pool *pgxpool.Pool, md *marketdata.Cache) (*pipeline.Pipeline, error) {
	cfg := pipeline.DefaultConfig()
	if seeds := os.Getenv("TRADING_KAFKA"); seeds != "" {
		if seeds == "1" || seeds == "true" {
			seeds = defaultKafka
		}
		cfg.KafkaSeeds = strings.Split(seeds, ",")
	}
	return pipeline.New(ctx, pool, md, cfg)
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, migrations.TrackingTable); err != nil {
		return fmt.Errorf("create migration tracking: %w", err)
	}
	all, err := migrations.All()
	if err != nil {
		return err
	}

	applied := 0
	for _, m := range all {
		var seen bool
		if err := pool.QueryRow(ctx,
			`SELECT true FROM schema_migrations WHERE name = $1`, m.Name).Scan(&seen); err == nil {
			continue // already applied
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("check %s: %w", m.Name, err)
		}

		// The schema change and the record of it commit together, so a failure cannot
		// leave the database changed but unmarked, or marked but unchanged.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (name) VALUES ($1)`, m.Name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", m.Name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", m.Name, err)
		}
		fmt.Printf("applied %s\n", m.Name)
		applied++
	}

	if applied == 0 {
		fmt.Printf("schema up to date (%d migration(s))\n", len(all))
	}
	return nil
}

// ---------------------------------------------------------------------------
// the scenario
// ---------------------------------------------------------------------------

// demo runs the whole path end to end: a market maker rests real quotes in a real book
// that now lives behind an event log, a taker crosses them, both halves of every fill
// settle into a double-entry ledger, and the invariants are checked afterwards.
func demo(ctx context.Context, pl *pipeline.Pipeline, md *marketdata.Cache) error {
	const symbol = "ACME"
	eng := pl.Engine

	sim := marketdata.NewSimulator(md, map[string]money.Minor{symbol: dollars(150)},
		200*time.Millisecond, 15, 42)
	stop := make(chan struct{})
	go sim.Run(stop)
	defer close(stop)

	maker, err := eng.OpenAccount(ctx, "maker-"+uuid.NewString()[:8])
	if err != nil {
		return err
	}
	taker, err := eng.OpenAccount(ctx, "taker-"+uuid.NewString()[:8])
	if err != nil {
		return err
	}

	// The maker buys its opening inventory rather than being granted it, so it must be
	// funded for the purchase (10,000 x $150) plus working capital to quote the bid side.
	if err := eng.Deposit(ctx, maker, dollars(3_000_000)); err != nil {
		return err
	}
	if err := eng.SeedShares(ctx, maker, symbol, money.FromShares(10_000), dollars(150)); err != nil {
		return err
	}
	if err := eng.Deposit(ctx, taker, dollars(100_000)); err != nil {
		return err
	}

	fmt.Printf("maker %s  (deposited $3,000,000, bought 10,000 %s @ $150)\n", maker, symbol)
	fmt.Printf("taker %s  (cash $100,000)\n", taker)
	fmt.Printf("transport: %s\n\n", transportName(pl))

	if err := restQuotes(ctx, pl, maker, symbol); err != nil {
		return err
	}
	printBook(pl, symbol)

	// A market BUY, collared against the reference price stamped at accept time.
	view, err := eng.Submit(ctx, engine.SubmitRequest{
		AccountID: taker, ClientOrderID: "demo-buy-1", Symbol: symbol,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(120),
	})
	if err != nil {
		return err
	}
	fmt.Printf("\ntaker BUY 120 %s MARKET → acknowledged %s (order %s)\n",
		symbol, view.Status, view.ID.String()[:8])

	// The book is behind a log now, so the fills are not in the response. Wait for the
	// pipeline to settle, then read what actually happened.
	if err := pl.Drain(ctx); err != nil {
		return err
	}
	if err := reportOrder(ctx, eng, view.ID, "  "); err != nil {
		return err
	}

	// Sell part of it back, to exercise the realized-P&L path.
	sell, err := eng.Submit(ctx, engine.SubmitRequest{
		AccountID: taker, ClientOrderID: "demo-sell-1", Symbol: symbol,
		Side: book.Sell, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(50),
	})
	if err != nil {
		return err
	}
	if err := pl.Drain(ctx); err != nil {
		return err
	}
	fmt.Printf("\ntaker SELL 50 %s MARKET → acknowledged %s\n", symbol, sell.Status)
	if err := reportOrder(ctx, eng, sell.ID, "  "); err != nil {
		return err
	}

	// A retry of an earlier client_order_id must not create a second order, and must not
	// enqueue a second publish.
	dup, err := eng.Submit(ctx, engine.SubmitRequest{
		AccountID: taker, ClientOrderID: "demo-buy-1", Symbol: symbol,
		Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: money.FromShares(120),
	})
	if err != nil {
		return err
	}
	fmt.Printf("\nretry of demo-buy-1 → same order %v (idempotent)\n", dup.ID == view.ID)

	fmt.Println()
	for _, acct := range []struct {
		name string
		id   uuid.UUID
	}{{"maker", maker}, {"taker", taker}} {
		b, err := eng.Balances(ctx, acct.id)
		if err != nil {
			return err
		}
		pos, err := eng.Positions(ctx, acct.id)
		if err != nil {
			return err
		}
		fmt.Printf("%s: cash %s · reserved %s · buying power %s · fees %s\n",
			acct.name, b.Cash, b.ReservedCash, b.BuyingPower, b.Fees)
		for _, p := range pos {
			fmt.Printf("    %-6s qty %-12s basis %-12s realized %-10s unrealized %s\n",
				p.Symbol, p.Qty, p.CostBasis, p.RealizedPnL, p.UnrealizedPnL)
		}
	}

	fmt.Println("\n── verification ─────────────────────────────")
	return verify(ctx, eng)
}

// restQuotes places a two-sided quote and waits for it to reach the book.
func restQuotes(ctx context.Context, pl *pipeline.Pipeline, maker uuid.UUID, symbol string) error {
	ref, ok := pl.Engine.MarkPrice(symbol)
	if !ok {
		return errors.New("no reference price to quote around")
	}
	for i := 0; i < 3; i++ {
		off := money.Minor(int64(i+1) * 30)
		for _, q := range []struct {
			side  book.Side
			price money.Minor
		}{{book.Sell, ref + off}, {book.Buy, ref - off}} {
			if _, err := pl.Engine.Submit(ctx, engine.SubmitRequest{
				AccountID: maker, Symbol: symbol, Side: q.side,
				ClientOrderID: fmt.Sprintf("mm-%s-%d-%s", symbol, i, q.side),
				Type:          book.Limit, TIF: book.GTC,
				Qty: money.FromShares(50), LimitPrice: q.price,
			}); err != nil {
				return err
			}
		}
	}
	if err := pl.Drain(ctx); err != nil {
		return err
	}
	fmt.Println("market maker rested 6 quotes (through the log, like any other client)")
	return nil
}

func reportOrder(ctx context.Context, eng *engine.Engine, id uuid.UUID, indent string) error {
	settled, err := eng.Order(ctx, id)
	if err != nil {
		return err
	}
	fills, err := eng.OrderFills(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("%ssettled: %s · filled %s @ avg %s across %d fill(s)\n",
		indent, settled.Status, settled.FilledQty, settled.AvgPrice(), len(fills))
	for _, f := range fills {
		fmt.Printf("%s  fill %s  %s shares @ %s   (book_seq %d)\n",
			indent, f.FillID.String()[:8], f.Qty, f.Price, f.BookSeq)
	}
	if settled.RejectReason != "" {
		fmt.Printf("%s  reason: %s\n", indent, settled.RejectReason)
	}
	return nil
}

func verify(ctx context.Context, eng *engine.Engine) error {
	inv, err := eng.CheckInvariants(ctx)
	if err != nil {
		return err
	}
	fmt.Print(inv)

	worst, err := eng.ReservationBound(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Worst (consumed-reserved):  %d\n", worst)

	accounts, err := eng.Accounts(ctx)
	if err != nil {
		return err
	}
	mismatches := 0
	for _, a := range accounts {
		replayed, err := eng.ReplayAccount(ctx, a)
		if err != nil {
			return err
		}
		projected, err := eng.ProjectedState(ctx, a)
		if err != nil {
			return err
		}
		if string(replayed.Encode()) != string(projected.Encode()) {
			mismatches++
		}
	}
	fmt.Printf("Replay/projection mismatch: %d\n", mismatches)

	if !inv.OK() || worst > 0 || mismatches > 0 {
		return errors.New("INVARIANT VIOLATION")
	}
	fmt.Println("\nall invariants hold")
	return nil
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

func openAccount(ctx context.Context, eng *engine.Engine, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: open-account <label>")
	}
	id, err := eng.OpenAccount(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}

func deposit(ctx context.Context, eng *engine.Engine, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: deposit <account> <dollars>")
	}
	acct, err := eng.AccountRef(ctx, args[0])
	if err != nil {
		return err
	}
	amount, err := parseMinor(args[1])
	if err != nil {
		return err
	}
	if err := eng.Deposit(ctx, acct, amount); err != nil {
		return err
	}
	fmt.Printf("deposited %s\n", amount)
	return nil
}

func seedShares(ctx context.Context, eng *engine.Engine, args []string) error {
	if len(args) < 4 {
		return errors.New("usage: seed-shares <account> <symbol> <shares> <price>")
	}
	acct, err := eng.AccountRef(ctx, args[0])
	if err != nil {
		return err
	}
	qty, err := parseQty(args[2])
	if err != nil {
		return err
	}
	price, err := parseMinor(args[3])
	if err != nil {
		return err
	}
	symbol := strings.ToUpper(args[1])
	if err := eng.SeedShares(ctx, acct, symbol, qty, price); err != nil {
		return err
	}
	fmt.Printf("bought %s %s @ %s from outside the system\n", qty, symbol, price)
	return nil
}

func submit(ctx context.Context, pl *pipeline.Pipeline, md *marketdata.Cache, args []string) error {
	if len(args) < 5 {
		return errors.New("usage: submit <account> <symbol> buy|sell market|limit <shares> [price] [ioc|gtc]")
	}
	eng := pl.Engine
	acct, err := eng.AccountRef(ctx, args[0])
	if err != nil {
		return err
	}
	req := engine.SubmitRequest{
		AccountID:     acct,
		Symbol:        strings.ToUpper(args[1]),
		ClientOrderID: uuid.NewString(),
		TIF:           book.IOC,
	}
	switch strings.ToLower(args[2]) {
	case "buy":
		req.Side = book.Buy
	case "sell":
		req.Side = book.Sell
	default:
		return fmt.Errorf("side must be buy or sell, got %q", args[2])
	}
	switch strings.ToLower(args[3]) {
	case "market":
		req.Type = book.Market
	case "limit":
		req.Type = book.Limit
		req.TIF = book.GTC
	default:
		return fmt.Errorf("type must be market or limit, got %q", args[3])
	}
	if req.Qty, err = parseQty(args[4]); err != nil {
		return err
	}
	if req.Type == book.Limit {
		if len(args) < 6 {
			return errors.New("limit orders need a price")
		}
		if req.LimitPrice, err = parseMinor(args[5]); err != nil {
			return err
		}
	}
	for _, a := range args[5:] {
		switch strings.ToLower(a) {
		case "ioc":
			req.TIF = book.IOC
		case "gtc":
			req.TIF = book.GTC
		}
	}

	// A market order needs a reference price, and no simulator is running in a one-shot
	// CLI invocation. Deriving one from the resting book keeps the command usable
	// standalone without weakening the rule that a market order always has a collar.
	//
	// The drain has to come FIRST. This process just started, so its books are whatever
	// the last checkpoint held plus nothing — the log tail has not been consumed yet, and
	// reading the book before consuming it reports an empty market that is not empty.
	if err := pl.Settle(ctx); err != nil {
		return err
	}
	if req.Type == book.Market {
		if _, ok := md.Ref(req.Symbol, eng.Config().MaxRefStaleness); !ok {
			if b := pl.Matching.Book(req.Symbol); b != nil {
				bid, okBid := b.BestBid()
				ask, okAsk := b.BestAsk()
				switch {
				case okBid && okAsk:
					md.Publish(marketdata.Tick{Symbol: req.Symbol, Price: (bid + ask) / 2, At: time.Now()})
				case okAsk:
					md.Publish(marketdata.Tick{Symbol: req.Symbol, Price: ask, At: time.Now()})
				case okBid:
					md.Publish(marketdata.Tick{Symbol: req.Symbol, Price: bid, At: time.Now()})
				}
			}
		}
	}

	view, err := eng.Submit(ctx, req)
	if err != nil {
		return err
	}
	fmt.Printf("order %s  acknowledged %s\n", view.ID, view.Status)
	if view.Status == "REJECTED" {
		fmt.Printf("reason: %s\n", view.RejectReason)
		return nil
	}

	// Accept returns as soon as the order is durable. Waiting for the pipeline to settle
	// is a courtesy of the CLI, not of the engine — a real client would subscribe.
	if err := pl.Settle(ctx); err != nil {
		return err
	}
	return reportOrder(ctx, eng, view.ID, "")
}

func cancelOrder(ctx context.Context, pl *pipeline.Pipeline, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: cancel <order-id>")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return err
	}
	status, err := pl.Engine.Cancel(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("requested → %s\n", status)
	if err := pl.Settle(ctx); err != nil {
		return err
	}
	final, err := pl.Engine.Order(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("book verdict → %s", final.Status)
	if final.RejectReason != "" {
		fmt.Printf(" (%s)", final.RejectReason)
	}
	fmt.Println()
	return nil
}

func showOrder(ctx context.Context, eng *engine.Engine, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: order <order-id>")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return err
	}
	v, err := eng.Order(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s %s %s %s of %s\n", v.ID, v.Symbol, v.Side, v.Type, v.TIF, v.Qty)
	return reportOrder(ctx, eng, id, "")
}

func positions(ctx context.Context, pl *pipeline.Pipeline, md *marketdata.Cache, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: positions <account>")
	}
	eng := pl.Engine
	acct, err := eng.AccountRef(ctx, args[0])
	if err != nil {
		return err
	}

	// Unrealized P&L needs a mark, and a one-shot CLI has no price feed running. The book
	// mid is the honest stand-in: it is what the market is currently willing to trade at,
	// which is the same thing the simulator would be publishing. Without this the column
	// is permanently blank and the position view is half useless.
	if err := pl.Settle(ctx); err != nil {
		return err
	}
	markFromBooks(pl, md, eng, ctx, acct)

	pos, err := eng.Positions(ctx, acct)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SYMBOL\tQTY\tRESERVED\tBASIS\tMARK\tREALIZED\tUNREALIZED")
	for _, p := range pos {
		mark := "—"
		if p.HasMark {
			mark = p.MarkPrice.String()
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p.Symbol, p.Qty,
			p.ReservedQty, p.CostBasis, mark, p.RealizedPnL, p.UnrealizedPnL)
	}
	return w.Flush()
}

// markFromBooks publishes a mark for each symbol the account holds, taken from the
// midpoint of that symbol's book, or from the one side that is quoted.
func markFromBooks(pl *pipeline.Pipeline, md *marketdata.Cache, eng *engine.Engine,
	ctx context.Context, acct uuid.UUID) {

	pos, err := eng.Positions(ctx, acct)
	if err != nil {
		return
	}
	for _, p := range pos {
		b := pl.Matching.Book(p.Symbol)
		if b == nil {
			continue
		}
		bid, hasBid := b.BestBid()
		ask, hasAsk := b.BestAsk()
		var mark money.Minor
		switch {
		case hasBid && hasAsk:
			mark = (bid + ask) / 2
		case hasAsk:
			mark = ask
		case hasBid:
			mark = bid
		default:
			continue
		}
		md.Publish(marketdata.Tick{Symbol: p.Symbol, Price: mark, At: time.Now()})
	}
}

func showBalances(ctx context.Context, eng *engine.Engine, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: balances <account>")
	}
	acct, err := eng.AccountRef(ctx, args[0])
	if err != nil {
		return err
	}
	b, err := eng.Balances(ctx, acct)
	if err != nil {
		return err
	}
	fmt.Printf("cash          %s\n", b.Cash)
	fmt.Printf("reserved      %s\n", b.ReservedCash)
	fmt.Printf("buying power  %s   (derived: cash − active reservations)\n", b.BuyingPower)
	fmt.Printf("fees paid     %s\n", b.Fees)
	return nil
}

func showBook(ctx context.Context, pl *pipeline.Pipeline, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: book <symbol>")
	}
	// The books live in the matching service and were rebuilt from its snapshots when
	// this process started; anything since is still in the log.
	if err := pl.Settle(ctx); err != nil {
		return err
	}
	printBook(pl, strings.ToUpper(args[0]))
	return nil
}

func printBook(pl *pipeline.Pipeline, symbol string) {
	b := pl.Matching.Book(symbol)
	fmt.Printf("\n%s book\n", symbol)
	if b == nil {
		fmt.Println("  (no book — nothing has been routed for this symbol)")
		return
	}
	bids, asks := b.Depth(5)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  BID QTY\tBID\t\tASK\tASK QTY")
	n := len(bids)
	if len(asks) > n {
		n = len(asks)
	}
	for i := 0; i < n; i++ {
		bq, bp, aq, ap := "", "", "", ""
		if i < len(bids) {
			bq, bp = bids[i].Qty.String(), bids[i].Price.String()
		}
		if i < len(asks) {
			aq, ap = asks[i].Qty.String(), asks[i].Price.String()
		}
		fmt.Fprintf(w, "  %s\t%s\t\t%s\t%s\n", bq, bp, ap, aq)
	}
	_ = w.Flush()
}

func replay(ctx context.Context, eng *engine.Engine, args []string) error {
	var accounts []uuid.UUID
	if len(args) > 0 {
		acct, err := eng.AccountRef(ctx, args[0])
		if err != nil {
			return err
		}
		accounts = []uuid.UUID{acct}
	} else {
		var err error
		if accounts, err = eng.Accounts(ctx); err != nil {
			return err
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ACCOUNT\tSEQ\tREPLAYED CASH\tPROJECTED CASH\tMATCH")
	bad := 0
	for _, a := range accounts {
		replayed, err := eng.ReplayAccount(ctx, a)
		if err != nil {
			return err
		}
		projected, err := eng.ProjectedState(ctx, a)
		if err != nil {
			return err
		}
		match := string(replayed.Encode()) == string(projected.Encode())
		if !match {
			bad++
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%v\n", a.String()[:8], replayed.Seq,
			replayed.Cash, projected.Cash, match)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if bad > 0 {
		return fmt.Errorf("%d account(s) diverge from the ledger", bad)
	}
	fmt.Println("\nevery account's projection is byte-identical to its ledger replay")
	return nil
}

// snapshot checkpoints every account and reports how bounded recovery now is.
func snapshot(ctx context.Context, eng *engine.Engine) error {
	n, err := eng.SnapshotAll(ctx)
	if err != nil {
		return err
	}
	stats, err := eng.SnapshotStats(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("checkpointed %d account(s)\n", n)
	fmt.Printf("%d snapshot(s) retained across %d account(s)\n", stats.Snapshots, stats.Accounts)
	fmt.Printf("worst replay tail: %d transactions past the latest snapshot\n", stats.MaxTail)
	return nil
}

func transportName(pl *pipeline.Pipeline) string {
	if len(pl.Cfg.KafkaSeeds) > 0 {
		return "kafka (" + strings.Join(pl.Cfg.KafkaSeeds, ",") + ")"
	}
	return "in-process log (set TRADING_KAFKA to use a broker)"
}

// ---------------------------------------------------------------------------
// parsing
// ---------------------------------------------------------------------------

func dollars(n int64) money.Minor { return money.Minor(n * 100) }

// parseMinor reads a decimal amount into minor units without ever touching a float.
func parseMinor(s string) (money.Minor, error) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.ReplaceAll(s, ",", ""), "$"))
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" {
		whole = "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad amount %q", s)
	}
	frac = (frac + "00")[:2]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad amount %q", s)
	}
	out := money.Minor(w*100 + f)
	if neg {
		out = -out
	}
	return out, nil
}

// parseQty reads a share count into 1e-6 share units.
func parseQty(s string) (money.Qty, error) {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" {
		whole = "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad quantity %q", s)
	}
	frac = (frac + "000000")[:6]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad quantity %q", s)
	}
	return money.Qty(w*money.QtyScale + f), nil
}

// runChaos executes a fault-injection run and prints the verification block.
//
// The block is the deliverable. Every number in it is zero, and each zero is a claim that
// a specific failure — a republished record, a matching engine that died mid-fill, a core
// that restarted with outcomes in flight — cost nothing.
func runChaos(ctx context.Context, pl *pipeline.Pipeline, md *marketdata.Cache, args []string) error {
	cfg := chaos.DefaultConfig()
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("orders must be a number, got %q", args[0])
		}
		cfg.Orders = n
		// Keep roughly eight faults regardless of run length, so a longer run means more
		// chances to break rather than the same few.
		cfg.FaultEvery = n / 8
		if cfg.FaultEvery < 1 {
			cfg.FaultEvery = 1
		}
	}

	fmt.Printf("chaos run · %d orders · %d symbols · %.0f%% of records delivered twice\n",
		cfg.Orders, len(cfg.Symbols), cfg.DuplicateRate*100)
	fmt.Printf("transport: %s\n\n", transportName(pl))

	rep, err := chaos.Run(ctx, pl, md, cfg)
	if err != nil {
		return err
	}

	fmt.Println("── verification ─────────────────────────────")
	fmt.Print(rep)
	if !rep.OK() {
		return errors.New("CHAOS RUN FAILED")
	}
	fmt.Println("\nall invariants held under fault injection")
	return nil
}
