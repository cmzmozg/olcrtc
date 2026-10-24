package panel

// ai-generated: verify monitoring authorization and journal privacy, durability and bounds.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ai-generated: exercise monitor endpoints through real session authentication.
func TestMonitoringRequiresLogin(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	for _, path := range []string{"metrics", "audit"} {
		tp.api(t, http.MethodGet, path, nil, http.StatusUnauthorized)
	}
	tp.login(t)
	client := tp.addClient(t, "device")
	tp.api(t, http.MethodPatch, "clients/"+client.ID, map[string]bool{"disabled": true}, http.StatusOK)
	var metrics metricsView
	raw := tp.api(t, http.MethodGet, "metrics", nil, http.StatusOK)
	if err := json.Unmarshal(raw, &metrics); err != nil {
		t.Fatal(err)
	}
	if metrics.Clients != 1 || metrics.ClientsEnabled != 0 || metrics.Rooms != 1 || metrics.HeapBytes == 0 || metrics.Goroutines == 0 || metrics.UptimeSec <= 0 {
		t.Fatalf("bad metrics: %+v", metrics)
	}
	if runtime.GOOS == "linux" && (metrics.Host.MemTotal == 0 || metrics.Host.DiskTotal == 0) {
		t.Fatalf("missing Linux metrics: %+v", metrics.Host)
	}
	for _, secret := range []string{configKey, testPassword, testPath, tp.opts.Store} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("metrics exposed %q", secret)
		}
	}
}

// ai-generated: verify journal order, safe targets, failures and secret exclusion.
func TestJournalRecordsActionsWithoutSecrets(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.api(t, http.MethodPost, "login", map[string]string{"password": "private-wrong-password"}, http.StatusUnauthorized)
	tp.login(t)
	client := tp.addClient(t, "private-client-name")
	tp.api(t, http.MethodGet, "clients/"+client.ID+"/links", nil, http.StatusOK)
	tp.api(t, http.MethodPatch, "clients/"+client.ID, map[string]bool{"disabled": true}, http.StatusOK)
	tp.api(t, http.MethodDelete, "clients/private-attacker-secret", nil, http.StatusNotFound)
	room := tp.addRoom(t, "private-room-name", vkRoom)
	tp.api(t, http.MethodDelete, "rooms/"+room.ID, nil, http.StatusOK)
	raw := tp.api(t, http.MethodGet, "audit", nil, http.StatusOK)
	var view auditView
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	want := []string{"room-delete", "room-create", "client-delete", "client-update", "client-links", "client-create", "login", "login"}
	if len(view.Entries) != len(want) {
		t.Fatalf("journal: %s", raw)
	}
	for i, action := range want {
		if view.Entries[i].Action != action || view.Entries[i].IP == "" || view.Entries[i].At.IsZero() {
			t.Fatalf("entry %d = %+v", i, view.Entries[i])
		}
	}
	if view.Entries[2].Target != "" || view.Entries[2].Status != http.StatusNotFound || view.Entries[3].Target != client.ID || view.Entries[7].Status != http.StatusUnauthorized {
		t.Fatalf("unexpected targets or results: %s", raw)
	}
	clients := tp.p.store.snapshot().Clients
	secrets := make([]string, 0, 7+2*len(clients))
	secrets = append(secrets, testPassword, testPath, vkRoom, "private-wrong-password", "private-client-name", "private-attacker-secret", "private-room-name")
	for _, c := range clients {
		secrets = append(secrets, c.Key, c.Sub)
	}
	for _, secret := range secrets {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("journal exposed %q", secret)
		}
	}
	reloaded, err := loadAudit(tp.opts.Store + ".audit.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.snapshot().Entries) != len(want) {
		t.Fatal("journal lost events on reload")
	}
	info, err := os.Stat(tp.opts.Store + ".audit.json")
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != storeMode {
		t.Fatalf("journal mode = %v", info.Mode())
	}
}

// ai-generated: ensure rate-limited failures do not continuously rewrite the journal.
func TestJournalSkipsBlockedLogins(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	for range maxFailures {
		tp.api(t, http.MethodPost, "login", map[string]string{"password": "wrong"}, http.StatusUnauthorized)
	}
	tp.api(t, http.MethodPost, "login", map[string]string{"password": "wrong"}, http.StatusTooManyRequests)
	if got := len(tp.p.audit.snapshot().Entries); got != maxFailures {
		t.Fatalf("events = %d", got)
	}
}

// ai-generated: bound journal storage and make failed persistence visible.
func TestJournalBoundsAndPersistenceFailure(t *testing.T) {
	a, err := loadAudit(filepath.Join(t.TempDir(), "audit.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range auditLimit {
		a.entries = append(a.entries, auditEntry{Action: strconv.Itoa(i)})
	}
	a.add(auditEntry{Action: "latest"})
	view := a.snapshot()
	if len(view.Entries) != auditLimit || view.Entries[0].Action != "latest" || view.Entries[auditLimit-1].Action != "1" {
		t.Fatal("journal did not retain newest events")
	}
	a.path = t.TempDir()
	a.add(auditEntry{Action: "unsaved"})
	if view = a.snapshot(); view.Error == "" || view.Entries[0].Action != "unsaved" {
		t.Fatal("persistence failure was hidden")
	}
}

// ai-generated: reject oversized or malformed files and exercise concurrent journal access.
func TestJournalLoadAndConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.json")
	for _, raw := range []string{"broken", strings.Repeat(" ", auditMaxBytes+1)} {
		if err := os.WriteFile(path, []byte(raw), storeMode); err != nil {
			t.Fatal(err)
		}
		if _, err := loadAudit(path); err == nil {
			t.Fatal("invalid journal accepted")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	a, err := loadAudit(path)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 12 {
		group.Go(func() {
			a.add(auditEntry{At: time.Now().UTC(), Action: "login", Status: http.StatusOK})
			_ = a.snapshot()
		})
	}
	group.Wait()
	if len(a.snapshot().Entries) != 12 {
		t.Fatal("concurrent events lost")
	}
}

// ai-generated: ignore spoofed forwarded headers and omit unauthorized mutations.
func TestJournalUsesPeerAddress(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	req := httptest.NewRequest(http.MethodPost, "/"+testPath+"/api/login", strings.NewReader(`{"password":"wrong"}`))
	req.RemoteAddr = "192.0.2.10:1200"
	req.Header.Set(csrfHeader, "1")
	req.Header.Set("X-Forwarded-For", "203.0.113.99")
	tp.p.handler().ServeHTTP(httptest.NewRecorder(), req)
	tp.api(t, http.MethodPost, "clients", map[string]string{"name": "device"}, http.StatusUnauthorized)
	view := tp.p.audit.snapshot()
	if len(view.Entries) != 1 || view.Entries[0].IP != "192.0.2.10" {
		t.Fatalf("journal = %+v", view)
	}
}
