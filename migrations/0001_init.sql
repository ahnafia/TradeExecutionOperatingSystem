-- Phase 1 schema. Ledger is the source of truth; positions and orders are projections
-- written in the same transaction as the postings that justify them.

BEGIN;

CREATE TYPE ledger_kind   AS ENUM ('CASH','POSITION','FEES','CLEARING','EXTERNAL');
CREATE TYPE txn_kind      AS ENUM ('DEPOSIT','FILL_HALF','CANCEL_RELEASE','FEE','ADJUSTMENT');
CREATE TYPE order_side    AS ENUM ('BUY','SELL');
CREATE TYPE order_type    AS ENUM ('MARKET','LIMIT');
CREATE TYPE order_tif     AS ENUM ('IOC','GTC');
CREATE TYPE order_status  AS ENUM ('ACCEPTED','PARTIALLY_FILLED','FILLED','CANCELLED','REJECTED','PENDING_CANCEL','EXPIRED');
CREATE TYPE res_kind      AS ENUM ('CASH','SHARES');
CREATE TYPE fill_side     AS ENUM ('TAKER','MAKER');

-- accounts.next_seq is the per-account clock (ARCHITECTURE.md §2.5c, §9.1).
-- It is allocated under SELECT ... FOR UPDATE, so it is gap-free and commit-ordered,
-- which bigserial is NOT — a bigserial id is assigned before commit, so a lower id can
-- commit after a higher one and be skipped forever by a replay watermark.
CREATE TABLE accounts (
    id          uuid PRIMARY KEY,
    label       text        NOT NULL DEFAULT '',
    status      text        NOT NULL DEFAULT 'ACTIVE',
    next_seq    bigint      NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- `unit` is the unit of account a posting is denominated in: 'USD' for money,
-- the symbol for shares. Balance is asserted per (txn, unit) — see below.
CREATE TABLE ledger_accounts (
    id          bigserial PRIMARY KEY,
    account_id  uuid NOT NULL REFERENCES accounts(id),
    kind        ledger_kind NOT NULL,
    symbol      text NULL,
    unit        text NOT NULL,
    CONSTRAINT position_requires_symbol CHECK (kind <> 'POSITION' OR symbol IS NOT NULL),
    CONSTRAINT cash_kinds_have_no_symbol CHECK (kind NOT IN ('CASH','FEES') OR symbol IS NULL)
);
CREATE UNIQUE INDEX ledger_accounts_uniq
    ON ledger_accounts (account_id, kind, coalesce(symbol, ''));
CREATE INDEX ledger_accounts_by_account ON ledger_accounts (account_id);

CREATE TABLE transactions (
    id          bigserial PRIMARY KEY,
    account_id  uuid   NOT NULL REFERENCES accounts(id),
    account_seq bigint NOT NULL,
    kind        txn_kind NOT NULL,
    -- Deterministic, never random (§5.3). This is what makes a fill re-derived after a
    -- crash idempotent instead of money-creating.
    event_id    uuid   NOT NULL UNIQUE,
    order_id    uuid   NULL,
    fill_id     uuid   NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, account_seq)
);
CREATE INDEX transactions_by_fill  ON transactions (fill_id) WHERE fill_id IS NOT NULL;
CREATE INDEX transactions_by_order ON transactions (order_id) WHERE order_id IS NOT NULL;

CREATE TABLE postings (
    id          bigserial PRIMARY KEY,
    txn_id      bigint NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    ledger_acct bigint NOT NULL REFERENCES ledger_accounts(id),
    amount      bigint NOT NULL
);
CREATE INDEX postings_by_txn    ON postings (txn_id);
CREATE INDEX postings_by_ledger ON postings (ledger_acct);

-- A CHECK constraint cannot express this: it is evaluated per row and cannot see the
-- other postings of the same transaction. A DEFERRABLE constraint trigger firing at
-- COMMIT can, and it lets intermediate states during a multi-posting insert be legal.
CREATE FUNCTION assert_txn_balanced() RETURNS trigger AS $$
DECLARE
    offending text;
BEGIN
    SELECT string_agg(u.unit || '=' || u.total::text, ', ')
      INTO offending
      FROM (
        SELECT la.unit, sum(p.amount) AS total
          FROM postings p
          JOIN ledger_accounts la ON la.id = p.ledger_acct
         WHERE p.txn_id = NEW.txn_id
         GROUP BY la.unit
        HAVING sum(p.amount) <> 0
      ) u;

    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'unbalanced transaction %: %', NEW.txn_id, offending
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER postings_balance
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_txn_balanced();

-- Projections. Written in the same transaction as the postings that justify them, so
-- they cannot drift; comparing them back to the ledger detects corruption and
-- application bugs rather than drift (§8.3).
--
-- account_balances exists for a boring reason with a sharp consequence: deriving cash by
-- summing an account's postings is O(history), which turns the risk check on the hot
-- path into a scan that grows without bound. The projection makes it O(1).
CREATE TABLE account_balances (
    account_id uuid PRIMARY KEY REFERENCES accounts(id),
    cash       bigint NOT NULL DEFAULT 0,
    fees       bigint NOT NULL DEFAULT 0
);

CREATE TABLE positions (
    account_id   uuid NOT NULL REFERENCES accounts(id),
    symbol       text NOT NULL,
    qty          bigint NOT NULL DEFAULT 0,   -- 1e-6 share units
    cost_basis   bigint NOT NULL DEFAULT 0,   -- minor units
    realized_pnl bigint NOT NULL DEFAULT 0,   -- minor units
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE orders (
    id                uuid PRIMARY KEY,
    account_id        uuid NOT NULL REFERENCES accounts(id),
    symbol            text NOT NULL,
    side              order_side  NOT NULL,
    type              order_type  NOT NULL,
    tif               order_tif   NOT NULL,
    qty               bigint NOT NULL,
    limit_price       bigint NULL,
    -- Reference price stamped at accept time and carried into the book (§4.2). The book
    -- collars against THIS value rather than re-querying market data, which is what keeps
    -- book replay free of external dependencies.
    ref_price         bigint NULL,
    status            order_status NOT NULL,
    reject_reason     text NULL,
    filled_qty        bigint NOT NULL DEFAULT 0,
    filled_notional   bigint NOT NULL DEFAULT 0,
    client_order_id   text NOT NULL,
    version           int  NOT NULL DEFAULT 1,
    seq_no            bigserial,          -- book time-priority tiebreak on rebuild only
    created_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, client_order_id)
);
CREATE INDEX orders_resting ON orders (symbol, seq_no)
    WHERE status IN ('ACCEPTED','PARTIALLY_FILLED');

-- Amounts, not a tri-state enum: partial fills consume a reservation incrementally.
CREATE TABLE reservations (
    order_id   uuid PRIMARY KEY REFERENCES orders(id),
    account_id uuid NOT NULL REFERENCES accounts(id),
    kind       res_kind NOT NULL,
    symbol     text NULL,
    reserved   bigint NOT NULL,
    consumed   bigint NOT NULL DEFAULT 0,
    released   bigint NOT NULL DEFAULT 0,
    CONSTRAINT reservation_not_overdrawn CHECK (consumed + released <= reserved),
    CONSTRAINT reservation_non_negative  CHECK (consumed >= 0 AND released >= 0 AND reserved >= 0),
    CONSTRAINT shares_reservation_has_symbol CHECK (kind <> 'SHARES' OR symbol IS NOT NULL)
);
CREATE INDEX reservations_active ON reservations (account_id, kind)
    WHERE consumed + released < reserved;

-- One row per fill; the two half-fills reference it via transactions.fill_id.
CREATE TABLE fills (
    fill_id        uuid PRIMARY KEY,
    shard_id       int    NOT NULL,
    symbol         text   NOT NULL,
    book_seq       bigint NOT NULL,
    price          bigint NOT NULL,
    qty            bigint NOT NULL,
    taker_order_id uuid   NOT NULL REFERENCES orders(id),
    maker_order_id uuid   NOT NULL REFERENCES orders(id),
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (shard_id, symbol, book_seq)
);

COMMIT;
