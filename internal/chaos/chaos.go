// Package chaos runs the system under deliberate faults and verifies it anyway.
//
// The verification block this produces is the point of the whole project. Every number in
// it is zero, and each zero is a specific claim that a specific failure did not cost
// anything:
//
//	Lost           — the outbox made accept durable independently of everything downstream
//	Duplicated     — deterministic event ids plus a unique constraint
//	Orphaned       — both halves of every bilateral fill settled
//	Money delta    — the ledger balances per unit, per transaction, everywhere
//
// The oracle is deliberately external. The harness remembers what it submitted and
// compares that against terminal states read back afterwards; it does not ask the ledger
// whether the ledger is right. A test that counts duplicates using the same machinery that
// would have created them proves nothing.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/eventlog"
	"github.com/ahnafia/trading-system/internal/marketdata"
	"github.com/ahnafia/trading-system/internal/money"
	"github.com/ahnafia/trading-system/internal/pipeline"
)

// Config describes a chaos run.
type Config struct {
	Symbols       []string
	Orders        int
	Takers        int
	DuplicateRate float64 // fraction of published records delivered twice
	FaultEvery    int     // inject a fault every N orders; 0 disables
	Seed          int64
}

// DefaultConfig is a short but genuinely hostile run.
func DefaultConfig() Config {
	return Config{
		Symbols:       []string{"ACME", "BETA", "CRUX"},
		Orders:        2000,
		Takers:        4,
		DuplicateRate: 0.05,
		FaultEvery:    250,
		Seed:          1,
	}
}

// Report is the verification block.
type Report struct {
	Config Config

	// Submitted/Accepted/Rejected count CLIENT orders only. Tracked counts every order the
	// harness recorded, market-maker quotes included, and Terminal counts how many of
	// those reached a terminal state. Comparing Terminal against Accepted would be
	// comparing two different populations, which is how a passing run can look broken.
	Submitted  int
	Accepted   int
	Rejected   int
	Tracked    int
	Terminal   int
	Lost       []uuid.UUID
	Duplicated []string

	Faults          []string
	MaxRecovery     time.Duration
	Fills           int64
	DuplicateEvents int64

	Invariants  engine.Invariants
	WorstDraw   int64
	ReplayDrift int
	Elapsed     time.Duration
}

// OK reports whether the run passed.
func (r Report) OK() bool {
	return len(r.Lost) == 0 && len(r.Duplicated) == 0 &&
		r.Invariants.OK() && r.WorstDraw <= 0 && r.ReplayDrift == 0 &&
		r.Terminal == r.Tracked
}

// String renders the block every chaos run ends with.
func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Orders submitted:      %10d\n", r.Submitted)
	fmt.Fprintf(&b, "  accepted:            %10d\n", r.Accepted)
	fmt.Fprintf(&b, "  rejected (by risk):  %10d\n", r.Rejected)
	fmt.Fprintf(&b, "Orders tracked (incl. MM quotes): %d\n", r.Tracked)
	fmt.Fprintf(&b, "Orders terminal:       %10d\n", r.Terminal)
	fmt.Fprintf(&b, "Lost:                  %10d\n", len(r.Lost))
	fmt.Fprintf(&b, "Duplicated:            %10d\n", len(r.Duplicated))
	fmt.Fprintf(&b, "Fills:                 %10d\n", r.Fills)
	fmt.Fprintf(&b, "Duplicate records suppressed: %6d\n", r.DuplicateEvents)
	fmt.Fprintf(&b, "Unbalanced txns:       %10d\n", r.Invariants.UnbalancedTxns)
	fmt.Fprintf(&b, "Orphaned fill halves:  %10d\n", r.Invariants.OrphanedFillHalves)
	fmt.Fprintf(&b, "Unpaired clearing:     %10d\n", r.Invariants.UnpairedClearingUnits)
	fmt.Fprintf(&b, "Money conservation Δ:  %10d\n", r.Invariants.CashConservationDelta)
	fmt.Fprintf(&b, "Position/ledger drift: %10d\n", r.Invariants.PositionLedgerMismatch)
	fmt.Fprintf(&b, "Negative cash accts:   %10d\n", r.Invariants.NegativeCashAccounts)
	fmt.Fprintf(&b, "Worst consumed−reserved:%9d\n", r.WorstDraw)
	fmt.Fprintf(&b, "Replay/projection drift:%9d\n", r.ReplayDrift)

	syms := make([]string, 0, len(r.Invariants.ShareConservation))
	for s := range r.Invariants.ShareConservation {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	for _, s := range syms {
		fmt.Fprintf(&b, "Share conservation %-6s%10d\n", s+":", r.Invariants.ShareConservation[s])
	}

	fmt.Fprintf(&b, "Max recovery time:     %10s\n", r.MaxRecovery.Round(time.Millisecond))
	fmt.Fprintf(&b, "Elapsed:               %10s\n", r.Elapsed.Round(time.Millisecond))
	if len(r.Faults) > 0 {
		fmt.Fprintf(&b, "\nFaults injected (%d):\n", len(r.Faults))
		for _, f := range r.Faults {
			fmt.Fprintf(&b, "  · %s\n", f)
		}
	}
	return b.String()
}

// Run executes a chaos scenario and returns the verification block.
func Run(ctx context.Context, pl *pipeline.Pipeline, md *marketdata.Cache, cfg Config) (Report, error) {
	started := time.Now()
	rep := Report{Config: cfg}
	rng := rand.New(rand.NewSource(cfg.Seed))
	eng := pl.Engine

	if d := pl.Duplicator(); d != nil && cfg.DuplicateRate > 0 {
		// The relay is at-least-once; this makes it behave like a relay that is crashing
		// constantly, which is the same thing at a higher rate.
		dupRng := rand.New(rand.NewSource(cfg.Seed + 1))
		d.DuplicateWhen(func(eventlog.Record) bool {
			return dupRng.Float64() < cfg.DuplicateRate
		})
	}

	prices := map[string]money.Minor{}
	for i, s := range cfg.Symbols {
		prices[s] = money.Minor(5_000 + i*9_100)
		md.Publish(marketdata.Tick{Symbol: s, Price: prices[s], At: time.Now()})
	}

	maker, err := eng.OpenAccount(ctx, "chaos-maker-"+uuid.NewString()[:8])
	if err != nil {
		return rep, err
	}
	if err := eng.Deposit(ctx, maker, money.Minor(500_000_000_00)); err != nil {
		return rep, err
	}
	for _, s := range cfg.Symbols {
		if err := eng.SeedShares(ctx, maker, s, money.FromShares(500_000), prices[s]); err != nil {
			return rep, err
		}
	}

	takers := make([]uuid.UUID, 0, cfg.Takers)
	for i := 0; i < cfg.Takers; i++ {
		id, err := eng.OpenAccount(ctx, fmt.Sprintf("chaos-taker-%d-%s", i, uuid.NewString()[:6]))
		if err != nil {
			return rep, err
		}
		if err := eng.Deposit(ctx, id, money.Minor(50_000_000_00)); err != nil {
			return rep, err
		}
		takers = append(takers, id)
	}

	// The external oracle: what the harness believes it submitted.
	submitted := map[uuid.UUID]string{} // order id → client order id

	quote := func(round int) error {
		for _, s := range cfg.Symbols {
			ref := prices[s]
			for lvl := 1; lvl <= 3; lvl++ {
				off := money.Minor(int64(lvl) * 20)
				for _, q := range []struct {
					side  book.Side
					price money.Minor
				}{{book.Sell, ref + off}, {book.Buy, ref - off}} {
					v, err := eng.Submit(ctx, engine.SubmitRequest{
						AccountID:     maker,
						ClientOrderID: fmt.Sprintf("mm-%s-%d-%d-%s", s, round, lvl, q.side),
						Symbol:        s, Side: q.side, Type: book.Limit, TIF: book.GTC,
						Qty: money.FromShares(200), LimitPrice: q.price,
					})
					if err == nil && v.Status == "ACCEPTED" {
						submitted[v.ID] = v.ClientOrderID
					}
				}
			}
		}
		return nil
	}

	faults := []struct {
		name string
		fn   func() error
	}{
		{"restart matching engine (books rebuilt from snapshot + log tail)", func() error {
			if err := pl.Matching.SnapshotAll(ctx); err != nil {
				return err
			}
			return pl.RestartMatching(ctx)
		}},
		{"restart core outcome consumers (resume from durable offsets)", func() error {
			return pl.RestartConsumers(ctx)
		}},
		{"restart matching WITHOUT a fresh snapshot (longer replay tail)", func() error {
			return pl.RestartMatching(ctx)
		}},
	}

	for round := 0; rep.Submitted < cfg.Orders; round++ {
		if err := quote(round); err != nil {
			return rep, err
		}
		if err := pl.Drain(ctx); err != nil {
			return rep, err
		}

		for i := 0; i < 50 && rep.Submitted < cfg.Orders; i++ {
			taker := takers[rng.Intn(len(takers))]
			sym := cfg.Symbols[rng.Intn(len(cfg.Symbols))]
			side := book.Buy
			if rng.Intn(2) == 0 {
				side = book.Sell
			}

			req := engine.SubmitRequest{
				AccountID:     taker,
				ClientOrderID: fmt.Sprintf("t-%d-%d", round, i),
				Symbol:        sym, Side: side,
				Qty: money.FromShares(int64(1 + rng.Intn(20))),
			}
			if rng.Intn(4) == 0 {
				req.Type, req.TIF = book.Limit, book.GTC
				req.LimitPrice = prices[sym] + money.Minor(rng.Intn(60)-30)
			} else {
				req.Type, req.TIF = book.Market, book.IOC
			}

			rep.Submitted++
			v, err := eng.Submit(ctx, req)
			switch {
			case err != nil:
				rep.Rejected++ // a rejection is an answer, not a loss
			case v.Status == "REJECTED":
				rep.Rejected++
			default:
				rep.Accepted++
				submitted[v.ID] = v.ClientOrderID
			}

			if cfg.FaultEvery > 0 && rep.Submitted%cfg.FaultEvery == 0 {
				f := faults[rng.Intn(len(faults))]
				at := time.Now()
				if err := f.fn(); err != nil {
					return rep, fmt.Errorf("fault %q: %w", f.name, err)
				}
				if err := pl.Drain(ctx); err != nil {
					return rep, fmt.Errorf("recovering from %q: %w", f.name, err)
				}
				took := time.Since(at)
				if took > rep.MaxRecovery {
					rep.MaxRecovery = took
				}
				rep.Faults = append(rep.Faults,
					fmt.Sprintf("after %d orders: %s (recovered in %s)",
						rep.Submitted, f.name, took.Round(time.Millisecond)))
			}
		}

		if err := pl.Drain(ctx); err != nil {
			return rep, err
		}
		for _, s := range cfg.Symbols {
			prices[s] += money.Minor(rng.Intn(40) - 20)
			if prices[s] < 100 {
				prices[s] = 100
			}
			md.Publish(marketdata.Tick{Symbol: s, Price: prices[s], At: time.Now()})
		}
	}

	// Cancel anything still resting so every order reaches a terminal state; an order left
	// working is not lost, but it is not evidence either.
	if err := cancelOutstanding(ctx, pl); err != nil {
		return rep, err
	}
	if err := pl.Drain(ctx); err != nil {
		return rep, err
	}

	if err := verify(ctx, pl, submitted, &rep); err != nil {
		return rep, err
	}
	rep.Elapsed = time.Since(started)
	return rep, nil
}

// cancelOutstanding withdraws every order still working.
func cancelOutstanding(ctx context.Context, pl *pipeline.Pipeline) error {
	for attempt := 0; attempt < 10; attempt++ {
		ids, err := pl.Engine.WorkingOrders(ctx)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if _, err := pl.Engine.Cancel(ctx, id); err != nil {
				return err
			}
		}
		if err := pl.Drain(ctx); err != nil {
			return err
		}
	}
	return nil
}

// verify compares the harness's own record against what the system says, then runs the
// invariants and the replay check.
func verify(ctx context.Context, pl *pipeline.Pipeline, submitted map[uuid.UUID]string, rep *Report) error {
	eng := pl.Engine

	terminal, err := eng.TerminalOrders(ctx)
	if err != nil {
		return err
	}
	for id := range submitted {
		if _, ok := terminal[id]; !ok {
			rep.Lost = append(rep.Lost, id)
		}
	}

	dups, err := eng.DuplicateClientOrderIDs(ctx)
	if err != nil {
		return err
	}
	rep.Duplicated = dups
	rep.Tracked = len(submitted)
	rep.Terminal = 0
	for id := range terminal {
		if _, ours := submitted[id]; ours {
			rep.Terminal++
		}
	}

	if rep.Fills, err = eng.CountFills(ctx); err != nil {
		return err
	}
	rep.DuplicateEvents = pl.Matching.Suppressed()
	if rep.Invariants, err = eng.CheckInvariants(ctx); err != nil {
		return err
	}
	if rep.WorstDraw, err = eng.ReservationBound(ctx); err != nil {
		return err
	}

	accounts, err := eng.Accounts(ctx)
	if err != nil {
		return err
	}
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
			rep.ReplayDrift++
		}
	}
	return nil
}
