package panel

// ai-generated: bounded persistent administrator journal and HTTP recording.

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
	auditLogin    = "login"
	auditLimit    = 500
	auditMaxBytes = 1 << 20
)

type auditEntry struct {
	At     time.Time `json:"at"`
	IP     string    `json:"ip"`
	Action string    `json:"action"`
	Target string    `json:"target,omitempty"`
	Status int       `json:"status"`
}

type auditView struct {
	Entries []auditEntry `json:"entries"`
	Error   string       `json:"error,omitempty"`
}

type auditLog struct {
	mu      sync.Mutex
	path    string
	entries []auditEntry
	errText string
}

// ai-generated: load a size-limited journal, retaining its latest events.
func loadAudit(path string) (*auditLog, error) {
	a := &auditLog{path: path, entries: make([]auditEntry, 0, auditLimit)}
	f, err := os.Open(path) // #nosec G304 -- derived from the configured panel store
	if errors.Is(err, os.ErrNotExist) {
		return a, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open panel journal: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, auditMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read panel journal: %w", err)
	}
	if len(raw) > auditMaxBytes {
		return nil, fmt.Errorf("%w: panel journal exceeds size limit", ErrInvalid)
	}
	if err := json.Unmarshal(raw, &a.entries); err != nil {
		return nil, fmt.Errorf("parse panel journal: %w", err)
	}
	if len(a.entries) > auditLimit {
		a.entries = slices.Clone(a.entries[len(a.entries)-auditLimit:])
	}
	return a, nil
}

// ai-generated: persist bounded events; expose write failures without losing memory history.
func (a *auditLog) add(entry auditEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.entries) == auditLimit {
		copy(a.entries, a.entries[1:])
		a.entries = a.entries[:auditLimit-1]
	}
	a.entries = append(a.entries, entry)
	a.errText = ""
	if err := writeFileAtomic(a.path, a.entries); err != nil {
		a.errText = "journal persistence failed"
		logger.Warnf("panel: journal: %v", err)
	}
}

// ai-generated: return a detached newest-first journal snapshot.
func (a *auditLog) snapshot() auditView {
	a.mu.Lock()
	defer a.mu.Unlock()
	entries := slices.Clone(a.entries)
	slices.Reverse(entries)
	return auditView{Entries: entries, Error: a.errText}
}

// ai-generated: serve the administrator journal behind session authentication.
func (p *panel) handleAudit(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, p.audit.snapshot())
}

// ai-generated: record only known actions and safe IDs, excluding all request bodies.
func (p *panel) audited(action string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authed := p.auth.valid(r)
		res := &auditResponse{ResponseWriter: w, status: http.StatusOK}
		next(res, r)
		if action == auditLogin {
			if res.status != http.StatusOK && res.status != http.StatusUnauthorized {
				return
			}
		} else if !authed {
			return
		}
		target := r.PathValue("id")
		if decoded, err := hex.DecodeString(target); err != nil || len(decoded) != 8 {
			target = ""
		}
		p.audit.add(auditEntry{At: time.Now().UTC(), IP: remoteHost(r),
			Action: action, Target: target, Status: res.status})
	}
}

type auditResponse struct {
	http.ResponseWriter
	status int
	wrote  bool
}

// ai-generated: capture the response status for the journal.
func (w *auditResponse) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// ai-generated: capture implicit successful responses.
func (w *auditResponse) Write(body []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	if err != nil {
		return n, fmt.Errorf("write panel response: %w", err)
	}
	return n, nil
}
