package panel

// ai-generated: the whole file (panel).

import (
	"crypto/sha256"
	"crypto/subtle"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	cookieName = "olcrtc_panel"
	// sessionTTL is how long a login lasts.
	sessionTTL = 12 * time.Hour
	// maxFailures logins may fail from one address within failWindow before
	// it is refused until the window has passed.
	maxFailures = 5
	failWindow  = 15 * time.Minute
	// csrfHeader must be on every request that changes something: a form
	// or a link on another site cannot set it.
	csrfHeader = "X-Olcrtc-Panel"
)

// loginResult is what a login attempt comes to.
type loginResult int

const (
	loginOK loginResult = iota
	loginWrong
	loginLimited
)

// auth checks the admin password and keeps the logins it let in.
type auth struct {
	password [sha256.Size]byte
	secure   bool // cookies only over TLS
	path     string

	mu       sync.Mutex
	sessions map[string]time.Time
	failures map[string][]time.Time
}

func newAuth(password, path string, secure bool) *auth {
	return &auth{
		password: sha256.Sum256([]byte(password)),
		secure:   secure,
		path:     path,
		sessions: make(map[string]time.Time),
		failures: make(map[string][]time.Time),
	}
}

// login checks password from addr and, when it is right, starts a session
// and sets its cookie. An address that failed too often is not even checked.
func (a *auth) login(w http.ResponseWriter, addr, password string) loginResult {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pruneLocked(now)
	recent := a.failures[addr][:0]
	for _, at := range a.failures[addr] {
		if now.Sub(at) < failWindow {
			recent = append(recent, at)
		}
	}
	a.failures[addr] = recent
	if len(recent) >= maxFailures {
		return loginLimited
	}
	sum := sha256.Sum256([]byte(password))
	if subtle.ConstantTimeCompare(sum[:], a.password[:]) != 1 {
		a.failures[addr] = append(recent, now)
		return loginWrong
	}
	delete(a.failures, addr)
	token := newID(32)
	a.sessions[token] = now.Add(sessionTTL)
	http.SetCookie(w, a.cookie(token, int(sessionTTL.Seconds())))
	return loginOK
}

// pruneLocked drops the sessions that ran out and the addresses whose last
// failure left the window, so neither table grows without bound.
func (a *auth) pruneLocked(now time.Time) {
	for token, until := range a.sessions {
		if now.After(until) {
			delete(a.sessions, token)
		}
	}
	for host, at := range a.failures {
		if len(at) == 0 || now.Sub(at[len(at)-1]) >= failWindow {
			delete(a.failures, host)
		}
	}
}

// logout ends the request's session and clears its cookie.
func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(cookieName); err == nil {
		a.mu.Lock()
		delete(a.sessions, c.Value)
		a.mu.Unlock()
	}
	http.SetCookie(w, a.cookie("", -1))
}

// valid reports whether r carries a live session.
func (a *auth) valid(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	until, ok := a.sessions[c.Value]
	return ok && time.Now().Before(until)
}

// cookie is the session cookie: Secure over TLS only, since a panel on
// loopback behind an SSH tunnel is plain HTTP.
func (a *auth) cookie(value string, maxAge int) *http.Cookie {
	return &http.Cookie{ //nolint:gosec // Secure follows TLS, see above
		Name: cookieName, Value: value, Path: a.path, MaxAge: maxAge,
		HttpOnly: true, Secure: a.secure, SameSite: http.SameSiteStrictMode,
	}
}

// require lets r through to next only with a live session, and one that
// changes something only with the panel's header.
func (a *auth) require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.valid(r) {
			writeError(w, http.StatusUnauthorized, "login required")
			return
		}
		if r.Method != http.MethodGet && r.Header.Get(csrfHeader) == "" {
			writeError(w, http.StatusForbidden, "missing "+csrfHeader)
			return
		}
		next(w, r)
	}
}

// remoteHost is the address r came from, without the port.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
