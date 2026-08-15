-- The venue belongs in the fill's uniqueness constraint, for the same reason it belongs in
-- the fill's identity.
--
-- Each venue's book keeps its own book_seq, so (shard, symbol, book_seq) stopped being
-- unique the moment a symbol traded in two books. The derived identity was fixed first
-- (ids.FillID), and this is the schema catching up: the constraint that was meant to
-- enforce "one row per execution" was instead enforcing "one row per sequence number",
-- which two venues collide on immediately.
--
-- Worth noting the failure mode this would have had in production: the second venue's
-- fill would be rejected at settlement, one half of a bilateral fill would never apply,
-- and it would surface as a stranded clearing position rather than as anything pointing at
-- a constraint.

BEGIN;

ALTER TABLE fills ADD COLUMN venue text NOT NULL DEFAULT 'PRIMARY';
ALTER TABLE fills DROP CONSTRAINT IF EXISTS fills_shard_id_symbol_book_seq_key;
ALTER TABLE fills ADD CONSTRAINT fills_shard_venue_symbol_seq_key
    UNIQUE (shard_id, venue, symbol, book_seq);

COMMIT;
