-- Drop the foreign keys that cannot survive partitioning.
--
-- `fills` records both sides of an execution: taker_order_id and maker_order_id. Both
-- referenced `orders`, which was correct while every account lived in one database and is
-- impossible once they do not. The counterparty to a fill is, by construction, usually in
-- ANOTHER partition — that is what partitioning by account means — so its order row is in
-- another database and no foreign key can reach it.
--
-- This surfaced the first time a cross-partition fill was settled, as a constraint
-- violation on the maker's side. It is worth recording the general shape, because it will
-- recur: **a foreign key is a single-database construct, and any reference that crosses a
-- partition boundary has to be enforced somewhere else or not at all.** Here it is "not at
-- all" by design — the reference is informational, and the invariant that actually matters
-- (both halves of every fill settled) is checked by the reconciler across partitions,
-- which is the only place it can be checked.
--
-- The local reference is kept where it IS local: transactions.order_id still points at an
-- order in the same database, because a transaction and the order it settles always share
-- a partition.

BEGIN;

ALTER TABLE fills DROP CONSTRAINT IF EXISTS fills_taker_order_id_fkey;
ALTER TABLE fills DROP CONSTRAINT IF EXISTS fills_maker_order_id_fkey;

COMMIT;
