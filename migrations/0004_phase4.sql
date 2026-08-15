-- Phase 4: partition the core, and fence it.
--
-- Up to now there was exactly one writer process, so nothing could contend for an account.
-- Once accounts are spread across partitions and partitions can move between processes,
-- "exactly one writer" stops being a fact about the deployment and has to become a
-- mechanism.

BEGIN;

-- The fence.
--
-- Single-writer-per-account depends on two INDEPENDENT membership views agreeing: the
-- gateway's idea of which process owns a partition, and the log's idea of which consumer
-- owns a partition. During a rebalance or a network partition they can disagree, and
-- neither of them can stop a stalled process that still holds a live database connection
-- from committing. Kafka does not fence writers. A consumer group does not fence writers.
--
-- So ownership is arbitrated by the thing that also holds the money. A core process
-- acquires a partition by bumping `epoch`, renews it on a timer, and asserts its epoch
-- inside every write transaction. A process that lost the lease finds its epoch stale and
-- its transaction aborts — regardless of what the gateway or the log believe.
--
-- This is the honest version of ARCHITECTURE.md §0's first principle. Correctness here
-- comes from a mechanism, not from hoping two membership views agree.
CREATE TABLE partition_leases (
    partition_id int PRIMARY KEY,
    owner_id     uuid   NOT NULL,
    epoch        bigint NOT NULL,
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL
);

-- Which partition this database IS.
--
-- One row, set at bootstrap. It exists so a process that connects to the wrong database
-- fails immediately and loudly, rather than writing an account's ledger into a partition
-- that does not own it — a corruption that no invariant would catch, because every
-- individual transaction would still be perfectly balanced.
CREATE TABLE partition_identity (
    partition_id int PRIMARY KEY,
    singleton    boolean NOT NULL DEFAULT true UNIQUE CHECK (singleton)
);

COMMIT;
