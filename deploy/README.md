# Observability stack

Prometheus + Grafana for the trading engine, provisioned to scrape and display
the invariant and RED metrics documented in `internal/metrics/metrics.go` and
ARCHITECTURE.md §12.

## Start it

1. Run the engine so it's exposing metrics: `tradectl serve` (default
   `:9464/metrics`).
2. Start the stack: `docker compose -f docker-compose.yml -f deploy/compose.observability.yml up`
   (or merge `deploy/compose.observability.yml`'s `services:` block into your
   own compose file — it was written standalone on purpose, see the comment
   at the top of that file).
3. Open Grafana at `http://localhost:3001` (3000 may be taken locally).
   Anonymous access is enabled as admin — no login needed, local demo only.
4. The "Trading Engine — Invariants & RED" dashboard is auto-provisioned and
   should already be on the home screen.

## What to look at first

Top row. `invariants_ok`, total `invariant_violations`, worst reservation
overdraw, and worst share-conservation delta should all read **OK / 0**, and
stay that way through a chaos run — per ARCHITECTURE.md §12, "during a chaos
demo, they're the whole story." If any of those four goes red, the
"Invariant violations by check" and "Share conservation delta by symbol"
panels further down (Diagnostics row) tell you which specific check or
symbol broke.

**A non-zero `trading_fill_halves_in_flight` is expected under load and is
not a violation.** A fill is two transactions in two partitions with no
single transaction spanning them, so some number of half-settled fills is the
normal state of a busy system — it's saturation, not breakage. That panel is
deliberately styled blue rather than red/green so it can't be misread as an
alarm; only a *sustained, growing* trend is worth investigating (it means
settlement is falling behind).

Likewise, rising `COLLAR_BREACH` or `NO_REFERENCE_PRICE` counts on the
"Rejections by reason" panel are risk controls firing correctly, not faults.

## Gaps

`outbox_relay_lag_seconds` and per-topic/partition consumer lag are called
out in ARCHITECTURE.md §12 as load-bearing for correctness, but
`internal/metrics/metrics.go` doesn't register them yet (that surface belongs
to the outbox relay / Kafka consumer, Part 2 / §14b). The dashboard has a
placeholder panel in that spot noting this; wire it up once those collectors
exist.
