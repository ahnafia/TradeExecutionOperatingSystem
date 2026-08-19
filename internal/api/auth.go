package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/identity"
)

// Authentication.
//
// The interceptor this replaces trusted an X-Account-Id header, which means anyone could
// name any account and trade it. That was correct for Part 1 — the seam contract says so
// explicitly — and is catastrophic the moment the service has a public URL. What follows
// is the swap the contract was designed to make cheap: one function changes, no handler
// moves, and the engine still knows nothing but an account id.

const (
	sessionCookie = "tos_session"
	stateCookie   = "tos_oauth_state"
)

// AuthConfig decides which login methods exist.
type AuthConfig struct {
	GitHubClientID     string
	GitHubClientSecret string
	// PublicURL is the externally reachable origin, used to build the OAuth callback and
	// to decide whether cookies may be marked Secure.
	PublicURL string
	// DevLogin allows password-free login as an arbitrary name. Local development only:
	// it is authentication that authenticates nothing.
	DevLogin bool
}

// AuthConfigFromEnv reads the deployment's auth posture.
//
// Dev login is opt-in and never implied. A misconfigured production deploy should fail
// closed — no login at all — rather than silently fall back to a mode where anyone can
// become anyone.
func AuthConfigFromEnv() AuthConfig {
	return AuthConfig{
		GitHubClientID:     os.Getenv("GITHUB_CLIENT_ID"),
		GitHubClientSecret: os.Getenv("GITHUB_CLIENT_SECRET"),
		PublicURL:          strings.TrimRight(os.Getenv("PUBLIC_URL"), "/"),
		DevLogin:           os.Getenv("TRADING_DEV_LOGIN") == "1",
	}
}

func (a AuthConfig) gitHubEnabled() bool {
	return a.GitHubClientID != "" && a.GitHubClientSecret != ""
}

// secureCookies reports whether cookies may carry the Secure flag.
//
// Secure cookies are dropped by browsers over plain HTTP, so forcing it on would make
// local development silently fail to log in. Anything not obviously localhost is treated
// as public and gets the flag.
func (a AuthConfig) secureCookies() bool {
	if a.PublicURL == "" {
		return false
	}
	return !strings.Contains(a.PublicURL, "localhost") && !strings.Contains(a.PublicURL, "127.0.0.1")
}

func (s *Server) authRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/status", s.handleAuthStatus)
	mux.HandleFunc("POST /auth/logout", s.handleLogout)
	if s.auth.gitHubEnabled() {
		mux.HandleFunc("GET /auth/github", s.handleGitHubStart)
		mux.HandleFunc("GET /auth/github/callback", s.handleGitHubCallback)
	}
	if s.auth.DevLogin {
		mux.HandleFunc("POST /auth/dev", s.handleDevLogin)
	}
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	methods := []string{}
	if s.auth.gitHubEnabled() {
		methods = append(methods, "github")
	}
	if s.auth.DevLogin {
		methods = append(methods, "dev")
	}

	resp := map[string]any{"methods": methods, "authenticated": false}
	if u, ok := userOf(r); ok {
		resp["authenticated"] = true
		resp["display_name"] = u.DisplayName
		resp["account_id"] = u.AccountID.String()
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- GitHub ------------------------------------------------------------------

func (s *Server) handleGitHubStart(w http.ResponseWriter, r *http.Request) {
	state, err := identity.NewStateToken()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "could not start login")
		return
	}

	// The state parameter is CSRF protection for the login itself: it is echoed back by
	// the provider and compared against a cookie only this browser has. Without it an
	// attacker can complete a login flow in someone else's browser and leave them signed
	// in as the attacker.
	http.SetCookie(w, &http.Cookie{
		Name: stateCookie, Value: state, Path: "/auth",
		HttpOnly: true, Secure: s.auth.secureCookies(),
		SameSite: http.SameSiteLaxMode, MaxAge: 600,
	})

	q := url.Values{}
	q.Set("client_id", s.auth.GitHubClientID)
	q.Set("redirect_uri", s.auth.PublicURL+"/auth/github/callback")
	q.Set("scope", "read:user") // no email scope: we do not need one, so we do not ask
	q.Set("state", state)
	http.Redirect(w, r, "https://github.com/login/oauth/authorize?"+q.Encode(), http.StatusFound)
}

func (s *Server) handleGitHubCallback(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(stateCookie)
	if err != nil || !identity.SameToken(cookie.Value, r.URL.Query().Get("state")) {
		writeErr(w, http.StatusBadRequest, "BAD_STATE",
			"login state did not match; start again at /auth/github")
		return
	}
	// One use only.
	http.SetCookie(w, &http.Cookie{Name: stateCookie, Value: "", Path: "/auth", MaxAge: -1})

	code := r.URL.Query().Get("code")
	if code == "" {
		writeErr(w, http.StatusBadRequest, "NO_CODE", "github did not return a code")
		return
	}

	token, err := s.exchangeGitHubCode(r.Context(), code)
	if err != nil {
		slog.Warn("github token exchange failed", "err", err)
		writeErr(w, http.StatusBadGateway, "OAUTH_FAILED", "could not complete login")
		return
	}
	profile, err := s.fetchGitHubUser(r.Context(), token)
	if err != nil {
		slog.Warn("github profile fetch failed", "err", err)
		writeErr(w, http.StatusBadGateway, "OAUTH_FAILED", "could not read your github profile")
		return
	}

	name := profile.Login
	if profile.Name != "" {
		name = profile.Name
	}
	s.completeLogin(w, r, "github", fmt.Sprint(profile.ID), profile.Email, name)
}

type githubProfile struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func (s *Server) exchangeGitHubCode(ctx context.Context, code string) (string, error) {
	form := url.Values{}
	form.Set("client_id", s.auth.GitHubClientID)
	form.Set("client_secret", s.auth.GitHubClientSecret)
	form.Set("code", code)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://github.com/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if out.AccessToken == "" {
		// Deliberately does not include the body: it can echo the client secret back.
		return "", fmt.Errorf("github refused the code (%s)", out.Error)
	}
	return out.AccessToken, nil
}

func (s *Server) fetchGitHubUser(ctx context.Context, token string) (githubProfile, error) {
	var p githubProfile
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user", nil)
	if err != nil {
		return p, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := s.http.Do(req)
	if err != nil {
		return p, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return p, fmt.Errorf("github returned %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return p, json.Unmarshal(body, &p)
}

// --- dev login ----------------------------------------------------------------

type devLoginReq struct {
	Name string `json:"name"`
}

// handleDevLogin signs in as a name, with no proof of anything.
//
// It exists so the app is usable without registering an OAuth application, and it is gated
// behind an explicit environment variable because it is not authentication — it is a
// bypass wearing authentication's clothes.
func (s *Server) handleDevLogin(w http.ResponseWriter, r *http.Request) {
	var req devLoginReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "expected {\"name\": \"...\"}")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" || len(req.Name) > 40 {
		writeErr(w, http.StatusBadRequest, "BAD_NAME", "name must be 1–40 characters")
		return
	}
	s.completeLogin(w, r, "dev", strings.ToLower(req.Name), "", req.Name)
}

// --- shared login tail ---------------------------------------------------------

// completeLogin provisions on first sight, issues a session, and sets the cookie.
func (s *Server) completeLogin(w http.ResponseWriter, r *http.Request, provider, subject, email, name string) {
	user, created, err := s.ident.Provision(r.Context(), provider, subject, email, name,
		func(ctx context.Context) (uuid.UUID, error) {
			id, err := s.eng.OpenAccount(ctx, fmt.Sprintf("%s:%s", provider, subject))
			if err != nil {
				return uuid.Nil, err
			}
			if s.cfg.SignupDeposit > 0 {
				if err := s.eng.Deposit(ctx, id, s.cfg.SignupDeposit); err != nil {
					return uuid.Nil, err
				}
			}
			return id, nil
		})
	if err != nil {
		slog.Error("provisioning user", "provider", provider, "err", err)
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "could not complete login")
		return
	}

	token, expires, err := s.ident.IssueSession(r.Context(), user.ID, r.UserAgent())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "could not start a session")
		return
	}

	// HttpOnly keeps the token out of reach of any script on the page, so an XSS bug
	// cannot walk off with a login. SameSite=Lax means a form on another origin cannot
	// POST an order with this cookie attached, which is the CSRF case that matters here.
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/",
		HttpOnly: true, Secure: s.auth.secureCookies(),
		SameSite: http.SameSiteLaxMode, Expires: expires,
	})

	if provider == "dev" || r.Header.Get("Accept") == "application/json" {
		writeJSON(w, http.StatusOK, map[string]any{
			"display_name": user.DisplayName,
			"account_id":   user.AccountID.String(),
			"new_account":  created,
		})
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		_ = s.ident.Revoke(r.Context(), c.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: s.auth.secureCookies(), SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "signed out"})
}

// PurgeSessions removes expired rows on a slow ticker.
func (s *Server) PurgeSessions(ctx context.Context) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.ident.PurgeExpired(ctx); err == nil && n > 0 {
				slog.Info("purged expired sessions", "count", n)
			}
		}
	}
}
