package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/metrics"
	"github.com/ahnafia/trading-system/internal/mm"
	"github.com/ahnafia/trading-system/internal/money"
	"github.com/ahnafia/trading-system/internal/pipeline"
)

// serveConfig is the scenario the runner drives.
type serveConfig struct {
	addr          string
	symbols       []string
	takers        int
	orderInterval time.Duration
	snapshotEvery time.Duration
	refreshEvery  time.Duration
}

func defaultServeConfig() serveConfig {
	return serveConfig{
		addr:          ":9464",
		symbols:       []string{"ACME", "BETA", "CRUX"},
		takers:        4,
		orderInterval: 40 * time.Millisecond,
		snapshotEvery: 10 * time.Second,
		refreshEvery:  time.Second,
	}
}

// serve runs the engine under continuous synthetic load with metrics exposed.
//
// The load matters as much as the endpoint: invariant gauges pinned at zero on an idle
// system prove nothing. This keeps market makers quoting, takers crossing, cancels racing
// fills, and snapshots being written, so the numbers on the dashboard are the numbers of a
// system that is actually working.
func serve(ctx context.Context, pl *pipeline.Pipeline, md *marketdata.Cache, args []string) error {
	cfg := defaultServeConfig()
	// A managed host tells you which port to bind and will consider the deploy failed if
	// you bind a different one. An explicit argument still wins, for local use.
	if p := os.Getenv("PORT"); p != "" {
		cfg.addr = ":" + p
	}
	if len(args) > 0 && args[0] != "" {
		cfg.addr = args[0]
		if !strings.Contains(cfg.addr, ":") {
			cfg.addr = ":" + cfg.addr
		}
	}

	// Apply the schema at boot. On a managed host there is no convenient shell to run
	// migrations from, and a container that starts against an unmigrated database fails in
	// a way that looks like a code bug rather than a missing step.
	if err := migrate(ctx, pl.Engine.Pool()); err != nil {
		return fmt.Errorf("migrate on boot: %w", err)
	}

	eng := pl.Engine
	m := metrics.New()
	eng.Observe(m)

	seed := make(map[string]money.Minor, len(cfg.symbols))
	for i, s := range cfg.symbols {
		seed[s] = money.Minor(5_000 + i*9_100)
	}
	sim := marketdata.NewSimulator(md, seed, 200*time.Millisecond, 12, 7)

	maker, takers, err := ensureParticipants(ctx, eng, cfg, seed)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)

	var wg sync.WaitGroup
	spawn := func(fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}

	spawn(func() { sim.Run(stop) })

	// Relay, matching, and outcome consumers, each in their own goroutines. In a
	// deployment these are separate processes; here they are separate goroutines talking
	// through the same log, which is the part that matters.
	pl.Run(ctx, func(err error) { fmt.Fprintln(os.Stderr, "pipeline:", err) })

	market := mm.New(eng, md, maker, cfg.symbols, mm.DefaultConfig())
	spawn(func() { market.Run(ctx, stop) })

	for i, t := range takers {
		taker := t
		seedN := int64(i)
		spawn(func() { driveTaker(ctx, eng, cfg, taker, seedN) })
	}

	spawn(func() {
		m.RefreshLoop(ctx, eng, cfg.refreshEvery, func(err error) {
			fmt.Fprintln(os.Stderr, "invariant refresh:", err)
		})
	})

	spawn(func() {
		t := time.NewTicker(cfg.snapshotEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := eng.SnapshotAll(ctx); err != nil {
					fmt.Fprintln(os.Stderr, "snapshot:", err)
				}
				if err := pl.Matching.SnapshotAll(ctx); err != nil {
					fmt.Fprintln(os.Stderr, "book snapshot:", err)
				}
			}
		}
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())

	// Liveness: the process is up. Deliberately does NOT touch the database — a health
	// check that fails when Postgres blips gets the container killed and restarted, which
	// is the worst possible response to a database that is already struggling.
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})

	// Readiness: can this instance actually serve? Separate from liveness precisely so a
	// struggling dependency takes it out of rotation without taking it out of existence.
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := pl.Engine.Pool().Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "database unreachable: %v\n", err)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writeStatus(r.Context(), w, pl, cfg)
	})

	srv := &http.Server{Addr: cfg.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = srv.Shutdown(shutdown)
	}()

	fmt.Printf("engine running · %d symbols · %d takers\n", len(cfg.symbols), cfg.takers)
	fmt.Printf("  metrics  http://localhost%s/metrics\n", cfg.addr)
	fmt.Printf("  status   http://localhost%s/\n", cfg.addr)
	fmt.Println("ctrl-c to stop")

	err = srv.ListenAndServe()
	cancel()
	wg.Wait()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// ensureParticipants finds or creates the harness accounts and funds them.
func ensureParticipants(ctx context.Context, eng *engine.Engine, cfg serveConfig,
	seed map[string]money.Minor) (uuid.UUID, []uuid.UUID, error) {

	const makerLabel = "bench-maker"
	maker, err := eng.AccountRef(ctx, makerLabel)
	if err != nil {
		if maker, err = eng.OpenAccount(ctx, makerLabel); err != nil {
			return uuid.Nil, nil, err
		}
		// Enough cash to buy the opening inventory outright, plus working capital: the
		// maker quotes both sides, so it needs to be able to buy as well as sell.
		if err := eng.Deposit(ctx, maker, money.Minor(500_000_000_00)); err != nil {
			return uuid.Nil, nil, err
		}
		for _, s := range cfg.symbols {
			if err := eng.SeedShares(ctx, maker, s, money.FromShares(500_000), seed[s]); err != nil {
				return uuid.Nil, nil, fmt.Errorf("seed %s: %w", s, err)
			}
		}
	}

	takers := make([]uuid.UUID, 0, cfg.takers)
	for i := 0; i < cfg.takers; i++ {
		label := fmt.Sprintf("bench-taker-%d", i)
		id, err := eng.AccountRef(ctx, label)
		if err != nil {
			if id, err = eng.OpenAccount(ctx, label); err != nil {
				return uuid.Nil, nil, err
			}
			if err := eng.Deposit(ctx, id, money.Minor(5_000_000_00)); err != nil {
				return uuid.Nil, nil, err
			}
		}
		takers = append(takers, id)
	}
	return maker, takers, nil
}

// driveTaker submits synthetic flow: mostly crossing orders, some resting limits that get
// cancelled, so the cancel/fill race is exercised continuously rather than only in tests.
func driveTaker(ctx context.Context, eng *engine.Engine, cfg serveConfig, taker uuid.UUID, seed int64) {
	rng := rand.New(rand.NewSource(1000 + seed))
	t := time.NewTicker(cfg.orderInterval)
	defer t.Stop()

	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n++

		symbol := cfg.symbols[rng.Intn(len(cfg.symbols))]
		side := book.Buy
		if n%2 == 0 {
			side = book.Sell // alternate so inventory stays near flat
		}

		req := engine.SubmitRequest{
			AccountID:     taker,
			ClientOrderID: fmt.Sprintf("%s-%d", taker.String()[:8], n),
			Symbol:        symbol,
			Side:          side,
			Qty:           money.FromShares(int64(1 + rng.Intn(25))),
			Type:          book.Market,
			TIF:           book.IOC,
		}

		restsThenCancels := rng.Intn(6) == 0
		if restsThenCancels {
			ref, ok := eng.MarkPrice(symbol)
			if !ok {
				continue
			}
			// Post away from the touch so it rests rather than crossing, then cancel it.
			offset := money.Minor(rng.Intn(200) + 50)
			req.Type, req.TIF = book.Limit, book.GTC
			if side == book.Buy {
				req.LimitPrice = ref - offset
			} else {
				req.LimitPrice = ref + offset
			}
			if req.LimitPrice <= 0 {
				continue
			}
		}

		view, err := eng.Submit(ctx, req)
		if err != nil || view.Status == "REJECTED" {
			topUp(ctx, eng, taker)
			continue
		}

		if restsThenCancels && view.Status == "ACCEPTED" {
			// Cancel after a beat, so a crossing order has a chance to race it.
			go func(id uuid.UUID) {
				select {
				case <-ctx.Done():
				case <-time.After(time.Duration(rng.Intn(400)) * time.Millisecond):
					_, _ = eng.Cancel(ctx, id)
				}
			}(view.ID)
		}
	}
}

// topUp refunds a taker that has traded itself out of buying power. A harness convenience,
// not engine behaviour: without it the load stops after fees grind the accounts down, and
// the dashboard goes quiet for an uninteresting reason.
func topUp(ctx context.Context, eng *engine.Engine, taker uuid.UUID) {
	bal, err := eng.Balances(ctx, taker)
	if err != nil || bal.BuyingPower > money.Minor(500_000_00) {
		return
	}
	_ = eng.Deposit(ctx, taker, money.Minor(5_000_000_00))
}

func writeStatus(ctx context.Context, w http.ResponseWriter, pl *pipeline.Pipeline, cfg serveConfig) {
	eng := pl.Engine
	inv, err := eng.CheckInvariants(ctx)
	if err != nil {
		fmt.Fprintf(w, "invariant check failed: %v\n", err)
		return
	}
	fmt.Fprintln(w, "── invariants ───────────────────────────────")
	fmt.Fprint(w, inv)

	if worst, err := eng.ReservationBound(ctx); err == nil {
		fmt.Fprintf(w, "Worst (consumed-reserved):  %d\n", worst)
	}
	if stats, err := eng.SnapshotStats(ctx); err == nil {
		fmt.Fprintf(w, "\nSnapshots: %d across %d accounts · worst replay tail %d txns\n",
			stats.Snapshots, stats.Accounts, stats.MaxTail)
	}

	if lag, err := pl.Lag(ctx); err == nil {
		fmt.Fprintf(w, "\nOutbox backlog: %d (oldest %s) · outcome offsets %v\n",
			lag.OutboxBacklog, lag.OutboxAge.Round(time.Millisecond), lag.Offsets)
	}

	fmt.Fprintln(w, "\n── books (in the matching service, behind the log) ───")
	for _, s := range cfg.symbols {
		b := pl.Matching.Book(s)
		if b == nil {
			fmt.Fprintf(w, "%-6s (no book yet)\n", s)
			continue
		}
		bid, hasBid := b.BestBid()
		ask, hasAsk := b.BestAsk()
		bidStr, askStr := "—", "—"
		if hasBid {
			bidStr = bid.String()
		}
		if hasAsk {
			askStr = ask.String()
		}
		fmt.Fprintf(w, "%-6s %10s / %-10s  fills %d\n", s, bidStr, askStr, b.BookSeq())
	}

	if !inv.OK() {
		fmt.Fprintln(w, "\nINVARIANT VIOLATION")
	}
}
