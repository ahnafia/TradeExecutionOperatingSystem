-- Phase 3: the matching engine moves out of the process, and a log goes between.
--
-- Everything here exists because two components that used to share a transaction no
-- longer do. The outbox replaces "and then call the book"; consumer offsets replace "and
-- then it happened"; book snapshots replace "the book is right there in memory".

BEGIN;

-- The transactional outbox.
--
-- An accepted order and its intent to publish commit together, in one transaction. There
-- is no two-phase commit across Postgres and Kafka, and there does not need to be: the
-- order is durable the moment accept commits, and publishing is a separate, retryable
-- step driven off this table. A crash between commit and publish loses nothing — the row
-- is still here with published_at NULL.
--
-- The cost is at-least-once delivery, which is why every consumer downstream is
-- idempotent. That is a deliberate trade: at-least-once plus idempotency is a system you
-- can reason about, and exactly-once across two systems is one you cannot.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    topic        text   NOT NULL,
    key          text   NOT NULL,       -- partition key: symbol, or account_id
    payload      bytea  NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz NULL
);
-- The relay's working set is only the unpublished tail, which stays small even as the
-- table grows; a partial index keeps the scan proportional to the backlog, not history.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Consumer offsets, advanced in the SAME transaction as the state change they authorize.
--
-- This is the whole exactly-once story, and it is worth being precise about what it buys:
-- the log delivers at least once, and the database makes the EFFECT happen once. A crash
-- between applying and committing rolls back both the state change and the offset, so the
-- record is redelivered and reapplied safely. A crash after commit has both, so it is not
-- redelivered. There is no window in which one advanced without the other.
CREATE TABLE consumer_offsets (
    consumer_group text   NOT NULL,
    topic          text   NOT NULL,
    partition      int    NOT NULL,
    next_offset    bigint NOT NULL,     -- the offset to read NEXT, not the last one read
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

-- Book snapshots.
--
-- A matching shard's books are memory. Rebuilding them by replaying orders.accepted from
-- the beginning is correct but unbounded, so a shard checkpoints its books plus the
-- offset it had consumed to, and resumes from there.
--
-- book_seq is part of the snapshot for a reason that is easy to miss: fill identities are
-- derived from it (§5.3). A shard that restored its books but restarted its counter would
-- mint identities that collide with fills already settled, and the dedup that is supposed
-- to protect the ledger would silently discard real executions instead.
-- The snapshot unit is a PARTITION, not a symbol, and that is forced rather than chosen.
-- A partition carries many symbols on one ordered stream, so there is exactly one consume
-- position for all of them. Snapshotting per symbol would give each its own offset, and
-- resuming would have to rewind to the oldest — replaying records the other books had
-- already applied. One offset, all the books it covers, written together.
CREATE TABLE book_snapshots (
    shard_id    int    NOT NULL,
    partition   int    NOT NULL,
    next_offset bigint NOT NULL,        -- position in orders.inbound this state reflects
    state       bytea  NOT NULL,        -- every book in the partition, with its counters
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (shard_id, partition, next_offset)
);

COMMIT;
