.PHONY: up down migrate demo serve chaos snapshot test race verify reset fmt observability observability-down

up:
	docker compose up -d
	@until docker exec trading-pg pg_isready -U trading -d trading >/dev/null 2>&1; do sleep 0.3; done
	@echo "postgres ready on :5433"

down:
	docker compose down

migrate: up
	go run ./cmd/tradectl migrate

# The full Phase 1 path: real book, real market maker, both halves of every fill,
# then the verification block.
demo: migrate
	go run ./cmd/tradectl demo

# Continuous synthetic load with the invariant gauges on :9464/metrics.
serve: migrate
	go run ./cmd/tradectl serve

# The headline: run under injected faults and print the verification block.
# Every number in it must be zero. `make chaos ORDERS=20000` for a longer run.
ORDERS ?= 2000
chaos: migrate
	go run ./cmd/tradectl chaos $(ORDERS)

# Prometheus + Grafana. Start `make serve` first so there is something to scrape.
observability:
	docker compose -f docker-compose.yml -f deploy/compose.observability.yml up -d prometheus grafana
	@echo "grafana http://localhost:3001  (dashboard: Trading Invariants)"

observability-down:
	docker compose -f docker-compose.yml -f deploy/compose.observability.yml down

snapshot:
	go run ./cmd/tradectl snapshot

test: up
	go test ./...

race: up
	go test -race -count=1 ./...

verify:
	go run ./cmd/tradectl invariants

# Wipe all engine state, keeping the schema.
#
# The log and the database must be reset TOGETHER. Truncating Postgres alone leaves the
# topics full of history the database no longer knows about, and the next start replays
# events referring to accounts that no longer exist. The log outlives the database, which
# is the correct durability relationship and an easy operational trap.
reset: reset-log
	docker exec trading-pg psql -U trading -d trading -c "TRUNCATE postings, transactions, \
		ledger_accounts, positions, account_balances, reservations, fills, orders, accounts, \
		snapshots, outbox, consumer_offsets, book_snapshots, partition_leases, \
		partition_identity, log_records, log_partitions RESTART IDENTITY CASCADE"

# Delete and recreate the topics. No-op if Redpanda is not running (the in-process log
# has nothing to reset).
reset-log:
	@docker exec trading-redpanda rpk topic delete orders.inbound orders.outcomes >/dev/null 2>&1 || true

fmt:
	gofmt -w ./cmd ./internal ./migrations
	go vet ./...
