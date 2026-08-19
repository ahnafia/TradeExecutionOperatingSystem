-- Fence the singletons.
--
-- partition_leases (0004) fences the trading core: an account's writes go through one
-- process, and a stale one aborts on the epoch assertion. Nothing fenced the two other
-- components that must also have exactly one holder — the matching engine and the outbox
-- relay — because until there was a server, only one process ever existed.
--
-- Running two matching engines against one log corrupts state, and it is not subtle: two
-- shards with diverging book state assign the same book_seq to different executions, the
-- derived fill identities collide, and half-fills strand. A single test run produced 112
-- orphaned halves.
--
-- This is not a hypothetical for a deployed service. A rolling deploy overlaps the old and
-- new instance by design, so two matching engines would start on EVERY deploy.
--
-- The semantics differ from a partition lease in one important way. A partition lease is
-- stolen unconditionally: the new owner always wins, and the old one is fenced when its
-- epoch assertion fails inside a write transaction. A service lease must NOT steal a live
-- holder, because the matching engine's output goes to a log and there is no equivalent
-- assertion to fence it with. Prevention is the only mechanism available, so acquisition
-- succeeds only when nobody holds it or the holder has expired.
CREATE TABLE service_leases (
    name        text PRIMARY KEY,
    owner_id    uuid   NOT NULL,
    epoch       bigint NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL
);
