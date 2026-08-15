-- A durable event log in Postgres.
--
-- The in-process log was built for tests and single-process runs, and it is correct for
-- those. It is actively dangerous for anything else, and the failure is worth recording
-- because both halves of it are individually right:
--
--   * Consumer offsets are durable, and advance in the same transaction as the state
--     change they authorize. That is the exactly-once guarantee and it is not negotiable.
--   * The in-process log is not durable, so a new process starts with an empty log.
--
-- Together they lose data. A second process resumes at offset 4 against a log that has one
-- record in it, never reads the record, and the order it described is stuck forever with
-- its reservation held. Nothing warns you: every component is behaving exactly as designed.
--
-- The lesson generalizes past this table. **A durable cursor into a non-durable stream is
-- always a bug**, and the two are usually owned by different people who each believe their
-- side is fine.
--
-- So the log gets the same durability as the offsets that point into it. This is also a
-- legitimate small deployment on its own: one Postgres, no broker, same semantics as
-- Kafka. The Kafka implementation remains for when throughput needs a real broker.

BEGIN;

CREATE TABLE log_records (
    topic      text   NOT NULL,
    partition  int    NOT NULL,
    "offset"   bigint NOT NULL,
    key        text   NOT NULL,
    value      bytea  NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, "offset")
);

-- The offset allocator. Offsets are assigned by bumping this row inside the produce
-- transaction, so they are gap-free and strictly increasing per partition — the same
-- reason accounts.next_seq exists rather than a bigserial. A sequence would be allocated
-- before commit and could interleave, and a consumer reading in offset order would skip
-- records that committed late.
CREATE TABLE log_partitions (
    topic       text   NOT NULL,
    partition   int    NOT NULL,
    next_offset bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, partition)
);

COMMIT;
