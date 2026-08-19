// Package identity maps a person to an account, and holds sessions.
//
// It is the only place in the system that knows a user is a person. Everything below it
// deals in an opaque account_id, which is what lets the trading engine stay ignorant of
// email addresses, OAuth providers, and the entire question of who anybody is (seam
// contract #1). Swapping GitHub for Google, or adding password login, changes this package
// and nothing underneath it.
package identity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SessionTTL is how long a login lasts.
//
// Long enough that a playground does not nag; short enough that an abandoned session on a
// shared machine stops working within a week rather than never.
const SessionTTL = 7 * 24 * time.Hour

// ErrNoSession means the token was absent, unknown, or expired. The three are deliberately
// indistinguishable to the caller: telling an attacker which of them applied is free
// information about what tokens exist.
var ErrNoSession = errors.New("no valid session")

// User is a person and the account they trade through.
type User struct {
	ID          uuid.UUID
	Provider    string
	Subject     string
	Email       string
	DisplayName string
	AccountID   uuid.UUID
}

// Store is identity persistence.
type Store struct{ pool *pgxpool.Pool }

// New wraps a pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Provision finds the user behind a provider identity, creating them — and their trading
// account — on first sight.
//
// The match is on (provider, subject), never on email. A provider's subject is immutable
// and unique; an email can be changed, released, and reassigned to a different person, and
// matching on it means a stranger who acquires an old address inherits the account.
//
// newAccount is called only when a user is actually created, so account provisioning and
// its opening balance happen exactly once per person.
func (s *Store) Provision(ctx context.Context, provider, subject, email, displayName string,
	newAccount func(context.Context) (uuid.UUID, error)) (User, bool, error) {

	var u User
	err := s.pool.QueryRow(ctx, `
		SELECT id, provider, subject, coalesce(email,''), display_name, account_id
		  FROM identity.users WHERE provider = $1 AND subject = $2`,
		provider, subject).Scan(&u.ID, &u.Provider, &u.Subject, &u.Email, &u.DisplayName, &u.AccountID)
	if err == nil {
		_, _ = s.pool.Exec(ctx,
			`UPDATE identity.users SET last_login_at = now() WHERE id = $1`, u.ID)
		return u, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return u, false, fmt.Errorf("look up user: %w", err)
	}

	account, err := newAccount(ctx)
	if err != nil {
		return u, false, fmt.Errorf("provision account: %w", err)
	}

	u = User{
		ID: uuid.New(), Provider: provider, Subject: subject,
		Email: email, DisplayName: displayName, AccountID: account,
	}
	var emailArg any
	if email != "" {
		emailArg = email
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO identity.users (id, provider, subject, email, display_name, account_id)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		u.ID, u.Provider, u.Subject, emailArg, u.DisplayName, u.AccountID); err != nil {
		return u, false, fmt.Errorf("create user: %w", err)
	}
	return u, true, nil
}

// IssueSession mints a token and stores only its hash.
//
// The returned string is the sole copy of the real token and goes straight into a cookie.
// Nothing logs it, nothing stores it, and it cannot be recovered from the database — which
// is the whole point, because a session token is a bearer credential.
func (s *Store) IssueSession(ctx context.Context, userID uuid.UUID, userAgent string) (string, time.Time, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("generate session token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	expires := time.Now().Add(SessionTTL)

	if len(userAgent) > 250 {
		userAgent = userAgent[:250]
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO identity.sessions (token_hash, user_id, expires_at, user_agent)
		VALUES ($1,$2,$3,$4)`, sum[:], userID, expires, userAgent); err != nil {
		return "", time.Time{}, fmt.Errorf("store session: %w", err)
	}
	return token, expires, nil
}

// Resolve turns a cookie value back into a user.
func (s *Store) Resolve(ctx context.Context, token string) (User, error) {
	if token == "" {
		return User{}, ErrNoSession
	}
	sum := sha256.Sum256([]byte(token))

	var u User
	var expires time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT u.id, u.provider, u.subject, coalesce(u.email,''), u.display_name,
		       u.account_id, s.expires_at
		  FROM identity.sessions s
		  JOIN identity.users u ON u.id = s.user_id
		 WHERE s.token_hash = $1`, sum[:]).
		Scan(&u.ID, &u.Provider, &u.Subject, &u.Email, &u.DisplayName, &u.AccountID, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNoSession
	}
	if err != nil {
		return User{}, fmt.Errorf("resolve session: %w", err)
	}
	if time.Now().After(expires) {
		// Delete on discovery: an expired session is dead weight, and cleaning it here
		// means the common path keeps the table trimmed without a scheduled job.
		_, _ = s.pool.Exec(ctx, `DELETE FROM identity.sessions WHERE token_hash = $1`, sum[:])
		return User{}, ErrNoSession
	}
	return u, nil
}

// Revoke ends one session.
func (s *Store) Revoke(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	sum := sha256.Sum256([]byte(token))
	_, err := s.pool.Exec(ctx, `DELETE FROM identity.sessions WHERE token_hash = $1`, sum[:])
	return err
}

// RevokeAll ends every session for a user — what "log out everywhere" needs, and the
// reason these are server-side rather than a stateless token.
func (s *Store) RevokeAll(ctx context.Context, userID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM identity.sessions WHERE user_id = $1`, userID)
	return err
}

// PurgeExpired removes dead sessions. Cheap; call it on a slow ticker.
func (s *Store) PurgeExpired(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM identity.sessions WHERE expires_at < now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// NewStateToken mints a random value for the OAuth state parameter.
func NewStateToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// SameToken compares in constant time, so a comparison cannot be used to guess a value one
// byte at a time.
func SameToken(a, b string) bool {
	return len(a) > 0 && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
