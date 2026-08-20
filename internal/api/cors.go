package api

import (
	"net/http"
	"os"
	"strings"
)

// Cross-origin support, for a frontend served from somewhere else.
//
// Same-origin needs none of this — the browser just sends the cookie. A frontend on
// Vercel calling an API on Render is a different origin, and two separate things then
// have to be true or nothing works:
//
//   - CORS must name the frontend's origin explicitly and allow credentials. The wildcard
//     is not an option: browsers refuse "*" together with credentials, and rightly so —
//     it would let any site on the internet make authenticated calls on a user's behalf.
//
//   - The session cookie must be SameSite=None, because Lax withholds cookies on
//     cross-site requests. None requires Secure, so cross-origin only works over HTTPS.
//
// Leave FRONTEND_URL unset and none of this activates, which keeps the same-origin
// deployment as tight as it was.
func frontendOrigins() []string {
	raw := os.Getenv("FRONTEND_URL")
	if raw == "" {
		return nil
	}
	var out []string
	for _, o := range strings.Split(raw, ",") {
		if o = strings.TrimRight(strings.TrimSpace(o), "/"); o != "" {
			out = append(out, o)
		}
	}
	return out
}

// cors answers preflights and tags allowed responses.
func cors(s *Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && s.allowsOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			// The response varies by Origin, so a cache must not serve one origin's
			// response to another.
			w.Header().Add("Vary", "Origin")
		}

		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allowsOrigin reports whether an origin is on the list. Exact match only — a prefix or
// suffix check is how "myapp.vercel.app.attacker.com" gets let in.
func (s *Server) allowsOrigin(origin string) bool {
	for _, o := range s.origins {
		if o == origin {
			return true
		}
	}
	return false
}

// crossOrigin reports whether this deployment serves a frontend on another origin, which
// is what forces SameSite=None on the session cookie.
func (s *Server) crossOrigin() bool { return len(s.origins) > 0 }

// afterLogin is where a completed login should land the browser.
//
// Same-origin, that is the app itself. Cross-origin, the browser is mid-redirect from the
// provider and pointing it at the API would leave the user staring at JSON.
func (s *Server) afterLogin() string {
	if len(s.origins) > 0 {
		return s.origins[0] + "/"
	}
	return "/"
}
