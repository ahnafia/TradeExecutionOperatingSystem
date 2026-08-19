package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/identity"
)

// The interceptor chain.
//
// ARCHITECTURE.md seam contracts #2 and #6 say auth and rate limiting are interceptors
// from the very first day, and that no handler ever calls getCurrentUser(). Both slots are
// here and both are near-empty, which is the point: Part 2 replaces one function each,
// rather than touching every route.
//
// The order matters and is not arbitrary. Recovery is outermost so a panic anywhere below
// still returns a response. The request id is next so everything after it can be
// correlated. Rate limiting comes before auth deliberately — an unauthenticated flood
// should be cheap to reject, and putting auth first would make every junk request pay for
// a session lookup.
func chain(h http.Handler, deps *Server) http.Handler {
	return recoverer(requestID(logging(rateLimit(deps, authenticate(deps, h)))))
}

type ctxKey string

const (
	ctxRequestID ctxKey = "request_id"
	ctxAccount   ctxKey = "account_id"
	ctxUser      ctxKey = "user"
)

// recoverer turns a panic into a 500 rather than a dropped connection.
//
// A trading API that drops the socket on a nil dereference leaves the client unable to
// tell a crash from a network fault, and the difference decides whether retrying is safe.
func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				slog.Error("panic serving request",
					"path", r.URL.Path, "panic", v, "stack", string(debug.Stack()))
				writeErr(w, http.StatusInternalServerError, "INTERNAL", "unexpected server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxRequestID, id)))
	})
}

// statusRecorder captures the status code so logging can report it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush lets Server-Sent Events through. Without it the recorder swallows the flusher and
// every streamed event sits in a buffer until the connection closes, which for a live fill
// feed means it never arrives at all.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		// The fill stream is long-lived by design; logging its duration on close would
		// report minutes and drown the useful lines.
		if r.URL.Path == "/api/fills" {
			return
		}
		slog.Info("request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"ms", time.Since(started).Milliseconds(),
			"request_id", r.Context().Value(ctxRequestID))
	})
}

// rateLimit is the slot from seam contract #6.
//
// It is a fixed-window counter per account, which is crude — it permits a burst of 2N
// across a window boundary. That is deliberately not worth fixing yet: the point of the
// slot is that it EXISTS, so filling it in later touches this function and nothing else.
// A public deployment should replace the body with a token bucket and a shared store.
func rateLimit(s *Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.RequestsPerMinute <= 0 {
			next.ServeHTTP(w, r)
			return
		}
		// Limit per account where we know one, per address otherwise. Keying only on the
		// account would leave unauthenticated endpoints — signup, login — unlimited, which
		// is where a public service actually gets hammered.
		key := r.RemoteAddr
		if id, ok := accountOf(r); ok {
			key = id.String()
		}
		if !s.limiter.allow(key, s.cfg.RequestsPerMinute) {
			w.Header().Set("Retry-After", "60")
			writeErr(w, http.StatusTooManyRequests, "RATE_LIMITED",
				"too many requests; slow down")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// accountHeader is the Part 1 bypass, now off unless explicitly enabled.
const accountHeader = "X-Account-Id"

// authenticate is the slot from seam contract #2, with the real implementation in it.
//
// Note what did NOT change to get here: no handler, no route, no engine call. A handler
// still receives an account id from the context and has never known where it came from,
// which is exactly what the contract was for. The whole of Part 2's identity work lands in
// this function and the package it calls.
//
// The header path survives only when TRADING_TRUST_HEADER is set, because it is
// impersonation-as-a-feature: with it on, anyone can name any account. It is genuinely
// useful for local scripting against a throwaway database and genuinely disqualifying
// anywhere else, so it must be asked for by name.
func authenticate(s *Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie(sessionCookie); err == nil {
			user, err := s.ident.Resolve(r.Context(), c.Value)
			if err == nil {
				ctx := context.WithValue(r.Context(), ctxAccount, user.AccountID)
				ctx = context.WithValue(ctx, ctxUser, user)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			// A stale cookie is cleared rather than left to fail every request, so the
			// browser stops presenting it and the user simply looks logged out.
			http.SetCookie(w, &http.Cookie{
				Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
				HttpOnly: true, SameSite: http.SameSiteLaxMode,
			})
		}

		if s.trustHeader {
			if raw := r.Header.Get(accountHeader); raw != "" {
				id, err := s.eng.AccountRef(r.Context(), raw)
				if err != nil {
					writeErr(w, http.StatusUnauthorized, "UNKNOWN_ACCOUNT",
						fmt.Sprintf("no account %q", raw))
					return
				}
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxAccount, id)))
				return
			}
		}

		next.ServeHTTP(w, r) // unauthenticated; each route decides what that means
	})
}

// userOf returns the signed-in person, when there is one. Handlers that only need an
// account use accountOf and stay ignorant of identity entirely.
func userOf(r *http.Request) (identity.User, bool) {
	u, ok := r.Context().Value(ctxUser).(identity.User)
	return u, ok
}

// accountOf returns the authenticated account, or false if the request carried none.
func accountOf(r *http.Request) (uuid.UUID, bool) {
	id, ok := r.Context().Value(ctxAccount).(uuid.UUID)
	return id, ok
}

// --- responses --------------------------------------------------------------

type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encoding response", "err", err)
	}
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, apiError{Error: kind, Message: msg})
}

// --- the crude limiter ------------------------------------------------------

type limiter struct {
	mu     sync.Mutex
	window time.Time
	counts map[string]int
}

func newLimiter() *limiter {
	return &limiter{window: time.Now().Truncate(time.Minute), counts: map[string]int{}}
}

func (l *limiter) allow(key string, max int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if now := time.Now().Truncate(time.Minute); now.After(l.window) {
		l.window = now
		l.counts = map[string]int{}
	}
	l.counts[key]++
	return l.counts[key] <= max
}
