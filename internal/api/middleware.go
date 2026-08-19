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
		key := r.Header.Get(accountHeader)
		if key == "" {
			key = r.RemoteAddr
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

// accountHeader is what the no-op authenticator trusts.
const accountHeader = "X-Account-Id"

// authenticate is the slot from seam contract #2.
//
// Right now it trusts a header, exactly as the contract describes for Part 1 — anyone can
// claim to be any account. That is fine for a CLI and a local demo and is NOT fine on the
// public internet, which is why this is one function: Part 2 swaps the body for a session
// or OAuth lookup and no handler changes, because no handler ever asks who the caller is.
func authenticate(s *Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.Header.Get(accountHeader)
		if raw == "" {
			next.ServeHTTP(w, r) // public routes decide for themselves
			return
		}
		id, err := s.eng.AccountRef(r.Context(), raw)
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "UNKNOWN_ACCOUNT",
				fmt.Sprintf("no account %q", raw))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxAccount, id)))
	})
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
