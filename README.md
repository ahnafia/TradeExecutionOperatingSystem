# Trade Execution Operating System

A stock exchange you can run on your laptop: a **matching engine**, a **double-entry
ledger**, and an **event log** between them — built so that no order is lost, no fill is
applied twice, and money is provably conserved even while the system is being broken on
purpose.

Simulated money, real mechanics. Orders match by price-time priority against resting
liquidity, fills settle as balanced double-entry transactions, and every guarantee the
design makes is checked continuously rather than asserted in a comment.

```
make chaos
```

That restarts the matching engine mid-flight, restarts the settlement consumers, and
delivers 5% of all internal records twice. Then it prints this:

```
Orders submitted:            3000      Unbalanced txns:              0
Orders tracked:              3815      Orphaned fill halves:         0
Orders terminal:             3815      Unpaired clearing:            0
Lost:                           0      Money conservation Δ:         0
Duplicated:                     0      Position/ledger drift:        0
Fills:                       2829      Replay/projection drift:      0
Duplicate records suppressed:  49      Share conservation:           0
Max recovery time:          1.67s
```

The `49 duplicate records suppressed` matters as much as the zeros — it is the evidence
that at-least-once delivery genuinely happened and was absorbed, rather than never
occurring.

---

## Quick start

Requires Docker and Go 1.26.

```bash
make demo      # Postgres up, schema applied, a full trade narrated end to end
make chaos     # the verification block above
make serve     # a live server on :9464 with market makers quoting
```

`make serve` runs the whole exchange: HTTP API, order matching, settlement, and simulated
market makers posting two-sided quotes so there is something to trade against.

To sign in locally and place an order:

```bash
TRADING_DEV_LOGIN=1 make serve            # in one terminal

curl -c jar -X POST localhost:9464/auth/dev \
  -H 'content-type: application/json' -d '{"name":"me"}'

curl -b jar -X POST localhost:9464/api/orders \
  -H 'content-type: application/json' \
  -d '{"symbol":"AAPL","side":"buy","type":"market","qty":"10"}'

curl -b jar -N localhost:9464/api/fills    # live fills, streamed
```

---

## What it does

**Place an order** and it is accepted in a single durable transaction — risk checked, funds
reserved, recorded — and acknowledged in about 4 ms. Matching happens behind an event log,
so the acknowledgement does not wait on it.

**A fill settles as two independent half-transactions**, one per counterparty, each
balancing on its own against a clearing account. When the two counterparties live in
different database partitions — which is the normal case — there is no transaction spanning
them, so conservation becomes a pairing invariant that a reconciler checks.

**Everything is derived from the ledger.** Positions, balances, cost basis, and realized
P&L are all reconstructible from postings alone; the tests prove it by rebuilding each
account from scratch and comparing byte for byte.

### HTTP API

```
GET  /auth/status                    which login methods are enabled
GET  /auth/github                    begin OAuth
POST /auth/logout

GET  /api/me                         cash, reserved, buying power
GET  /api/book/{symbol}              live order book
POST /api/orders                     {symbol, side, type, qty, limit_price?, tif?}
GET  /api/orders                     your recent orders
POST /api/orders/{id}/cancel
GET  /api/positions                  holdings, cost basis, realized and unrealized P&L
GET  /api/fills                      Server-Sent Events: fills and status changes, live
```

`POST /api/orders` returns **202** as soon as the order is durable. A rejection is **422** —
the request was well-formed and the system declined it, which is a different thing from an
error.

### Command line

```
tradectl demo · chaos · serve
tradectl submit <account> <sym> buy|sell market|limit <shares> [price] [ioc|gtc]
tradectl positions · balances · book · order · cancel
tradectl invariants        the verification block
tradectl replay            rebuild every account from the ledger and compare
```

---

## How it works

```
              ┌────────── one durable transaction ──────────┐
client ─────► │ ACCEPT: risk · reserve · order · outbox row │
              └──────────────────────┬──────────────────────┘
                                     │  acknowledged here (~4 ms): the order is now safe
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
 everything below may crash, restart, and repeat without losing anything
                                     ▼
                                     outbox relay
                                     │
                                     ▼  orders.inbound   (key = symbol)
                                     MATCHING — price-time books, venue routing
                                     │
                                     ▼  orders.outcomes  (key = account_id)
                                     SETTLEMENT — double-entry ledger,
                                                  offset committed in the same transaction
```

A **file-by-file map** of every box above lives at [`docs/architecture-map.html`](docs/architecture-map.html)
— clone and open it in a browser, or read [`ARCHITECTURE.md`](ARCHITECTURE.md) for the
reasoning behind each decision.

Four ideas carry the design:

**One durable write on the hot path.** Accept is a single Postgres transaction under the
account's row lock. There is no RPC whose failure could leave an order reserved but
unrouted, because there is no RPC — the intent to publish is a row in the same transaction.

**Event identity is derived, never minted.** A fill's id is a UUIDv5 over
`(shard, venue, symbol, book_seq, side)`. A matching engine that crashes rebuilds its books
and regenerates the fills it already produced — with the *same* identities — so the ledger
recognises and ignores them. A random UUID here would double every fill after a crash.

**Offsets advance in the transaction they authorize.** The log delivers at least once; the
database makes the *effect* happen once. There is no window where one advanced and the
other did not.

**Ownership is a mechanism, not an assumption.** A partition lease's epoch is asserted
inside every write, so a process that lost its partition cannot commit — regardless of what
any router or consumer group believes.

---

## Tests

```
make test     # unit, property, and integration
make race     # the same under the race detector
```

The ones that matter are properties, not examples:

- money and shares conserved across arbitrary random activity
- realized cost never exceeds the amount reserved at accept time
- concurrent buys on one account cannot overdraw it
- redelivering the entire outcome stream moves no money
- a matching-engine restart regenerates identical fill identities
- `replay(genesis) == replay(snapshot + tail)`, byte-identical
- a writer that lost its lease cannot commit, on any path
- a fill spanning two partition databases reconciles; a stranded half is caught
- fee arithmetic telescopes, so slicing an order never costs more than not slicing it

---

## Deploying

One always-on service and one Postgres. `render.yaml` describes both.

The event log runs *in Postgres*, so a deployment needs **no Kafka and no ClickHouse** —
add a broker when one database stops keeping up, not before. Kafka remains supported
(`TRADING_KAFKA=host:9092`) and is exercised by the same tests.

| Variable | For |
|---|---|
| `DATABASE_URL` | injected by the host |
| `PUBLIC_URL` | your external origin; OAuth callbacks and cookie security depend on it |
| `GITHUB_CLIENT_ID` / `_SECRET` | GitHub OAuth app, callback `$PUBLIC_URL/auth/github/callback` |

Two flags exist for local work and are **disqualifying in public**: `TRADING_DEV_LOGIN`
signs anyone in as any name, and `TRADING_TRUST_HEADER` lets any caller name any account.
Neither is implied by anything, so a misconfigured deploy ends up with no login at all
rather than a silently open one.

Sessions are server-side and only their hash is stored — the cookie holds the sole copy of
the token. Identity lives in its own `identity` schema; the trading engine has no column
anywhere naming an email, an OAuth subject, or a session.

---

## Status

Working: the engine, the HTTP API, the live fill stream, authentication, chaos
verification, Prometheus metrics and Grafana dashboards.

Not done, honestly:

- **No web UI.** Trading is `curl` or the CLI.
- **Throughput is unmeasured.** The scaling curve across 1→16 partitions is the most
  interesting missing number, and the harness for it does not exist yet.
- **Every test drives the synchronous drain path**; production drives background loops.
  That asymmetry is why several real bugs were found by using the CLI by hand rather than
  by 2,700 lines of tests.
- **ClickHouse analytics is built but has never run** against the engine.

[`ARCHITECTURE.md`](ARCHITECTURE.md) is the design document. It records 20 defects found
along the way — 18 by reviewing the plan before writing code, and the rest by building and
using it — including the one that mattered most: a durable consumer offset pointing into a
non-durable stream loses data silently, and both halves look correct in isolation.
