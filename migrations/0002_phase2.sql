-- Phase 2: ledger hardening.
--
-- Three changes, each closing a gap that Phase 1 left open deliberately.

BEGIN;

-- 1. Bounded recovery.
--
-- Replaying an account from genesis is O(history) and gets monotonically worse. A
-- snapshot is a checkpoint of derived state at a known account_seq; recovery loads the
-- latest one and replays only what came after.
--
-- `state` is a versioned canonical binary encoding, NOT jsonb: the acceptance test
-- compares snapshots byte for byte, and jsonb normalises key order and numeric
-- representation, which would make that comparison either flaky or vacuous.
CREATE TABLE snapshots (
    account_id  uuid   NOT NULL REFERENCES accounts(id),
    account_seq bigint NOT NULL,
    state       bytea  NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, account_seq)
);

-- 2. Proportional reservation release.
--
-- Releasing a BUY reservation's collar headroom incrementally, as each partial fill
-- lands, requires knowing the per-share price the reservation was sized at and the fee
-- rate applied. Recomputing them from config at fill time would be wrong: the config can
-- change between accept and fill, and the reservation must be released against the terms
-- it was actually taken under.
ALTER TABLE reservations ADD COLUMN unit_price bigint NULL;
ALTER TABLE reservations ADD COLUMN fee_bps    bigint NOT NULL DEFAULT 0;

-- 3. Cancel adjudication.
--
-- An order awaiting the book's verdict on a cancel is neither live nor terminal. Without
-- this index the pending set is a sequential scan, and it is polled.
CREATE INDEX orders_pending_cancel ON orders (symbol) WHERE status = 'PENDING_CANCEL';

COMMIT;
