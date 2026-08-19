-- Identity, in its own schema.
--
-- Seam contract #1 says the engine knows only an opaque account_id — no email, no OAuth
-- subject, no display name in any engine table. The separate schema makes that literal
-- rather than a convention: a query in the trading path physically cannot reach a user's
-- email without naming another schema, so the boundary is visible in every statement that
-- would cross it.
--
-- It is deliberately NOT partitioned. Identity is small, read once per request, and maps
-- to the account_id that routing then uses; sharding it would add a lookup hop to
-- everything in exchange for scaling a table that will never be the constraint.
CREATE SCHEMA IF NOT EXISTS identity;

-- A person. One row per (provider, subject) — the provider's own stable id, never the
-- email, because emails change hands and get reused and are not an identity.
CREATE TABLE identity.users (
    id            uuid PRIMARY KEY,
    provider      text NOT NULL,          -- github | dev
    subject       text NOT NULL,          -- the provider's immutable user id
    email         text NULL,              -- display only; never used to match a login
    display_name  text NOT NULL,
    account_id    uuid NOT NULL,          -- the engine's opaque handle; the ONLY crossing
    created_at    timestamptz NOT NULL DEFAULT now(),
    last_login_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (provider, subject)
);
CREATE UNIQUE INDEX users_account ON identity.users (account_id);

-- Sessions, stored as a HASH of the token.
--
-- The cookie holds the only copy of the real token. A database leak therefore yields
-- password-equivalents that cannot be replayed, which is the same reason nobody stores
-- passwords in the clear — a session token is a bearer credential and deserves the same
-- treatment.
--
-- Server-side sessions rather than a signed stateless token, because these are revocable.
-- A JWT cannot be withdrawn before it expires, and "log out everywhere" is a thing people
-- legitimately need after they lose a laptop.
CREATE TABLE identity.sessions (
    token_hash bytea PRIMARY KEY,
    user_id    uuid NOT NULL REFERENCES identity.users(id) ON DELETE CASCADE,
    issued_at  timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    user_agent text NULL
);
CREATE INDEX sessions_user ON identity.sessions (user_id);
CREATE INDEX sessions_expiry ON identity.sessions (expires_at);
