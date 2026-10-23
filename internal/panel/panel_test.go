package panel

// ai-generated: the whole file (panel).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/link"
	"github.com/openlibrecommunity/olcrtc/internal/server"
)

const (
	testPath     = "secret-path-1"
	testPassword = "correct horse battery"
	configKey    = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	vkRoom       = "https://vk.ru/call/join/abc123"
)

// fakeRooms stands in for the servers: a room runs from its start until its
// context ends, and reports itself up.
type fakeRooms struct {
	mu      sync.Mutex
	running map[string]Room
	starts  map[string]int
}

func (f *fakeRooms) run(ctx context.Context, room Room, _ *server.Keys, _ *server.Meter,
	onLink func(server.LinkState),
) error {
	f.mu.Lock()
	f.running[room.ID] = room
	f.starts[room.ID]++
	f.mu.Unlock()
	onLink(server.LinkUp)
	<-ctx.Done()
	f.mu.Lock()
	delete(f.running, room.ID)
	f.mu.Unlock()
	return nil
}

// waitRunning waits until room id runs (want) or does not.
func (f *fakeRooms) waitRunning(t *testing.T, id string, want bool) Room {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		room, ok := f.running[id]
		f.mu.Unlock()
		if ok == want {
			return room
		}
		if time.Now().After(deadline) {
			t.Fatalf("room %s running = %v, want %v", id, ok, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type testPanel struct {
	p     *panel
	srv   *httptest.Server
	rooms *fakeRooms
	http  *http.Client
	opts  Options
}

func testOptions(t *testing.T, rooms *fakeRooms, store string) Options {
	t.Helper()
	return Options{
		Listen: "127.0.0.1:0", Path: testPath, Password: testPassword, Store: store,
		ConfigRoom: &Room{ID: ConfigRoomID, Name: ConfigRoomID, Provider: "vkcalls", Transport: "vp8channel", URL: vkRoom + "0"},
		ConfigKeys: []string{configKey},
		Provider:   "vkcalls", Transport: "vp8channel",
		Providers: []string{"vkcalls", "telemost", "salutejazz"}, Transports: []string{"vp8channel", "datachannel"},
		Creators: []string{"salutejazz"}, VP8FPS: 60, VP8Batch: 64,
		RunRoom: rooms.run,
		CreateRoom: func(context.Context, string) (string, error) {
			return "made:pass", nil
		},
	}
}

func startPanel(t *testing.T, store string) *testPanel {
	t.Helper()
	rooms := &fakeRooms{running: map[string]Room{}, starts: map[string]int{}}
	opts := testOptions(t, rooms, store)
	ctx, cancel := context.WithCancel(context.Background())
	p, err := newPanel(ctx, opts)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv := httptest.NewServer(p.handler())
	jar, _ := cookiejar.New(nil)
	t.Cleanup(func() {
		srv.Close()
		cancel()
		p.rooms.stop()
	})
	return &testPanel{p: p, srv: srv, rooms: rooms, http: &http.Client{Jar: jar}, opts: opts}
}

// call sends a request under the panel's path; csrf adds the panel's header.
func (tp *testPanel) call(t *testing.T, method, path string, body any, csrf bool) (int, []byte) {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, tp.srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if csrf {
		req.Header.Set(csrfHeader, "1")
	}
	res, err := tp.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}

// api calls the panel's API and wants code back.
func (tp *testPanel) api(t *testing.T, method, path string, body any, code int) []byte {
	t.Helper()
	got, raw := tp.call(t, method, "/"+testPath+"/api/"+path, body, true)
	if got != code {
		t.Fatalf("%s %s = %d %s, want %d", method, path, got, raw, code)
	}
	return raw
}

func (tp *testPanel) login(t *testing.T) {
	t.Helper()
	tp.api(t, http.MethodPost, "login", map[string]string{"password": testPassword}, http.StatusOK)
}

type stateBody struct {
	Rooms   []roomView   `json:"rooms"`
	Clients []clientView `json:"clients"`
}

func (tp *testPanel) state(t *testing.T) stateBody {
	t.Helper()
	var s stateBody
	if err := json.Unmarshal(tp.api(t, http.MethodGet, "state", nil, http.StatusOK), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func (tp *testPanel) addRoom(t *testing.T, name, url string) roomView {
	t.Helper()
	tp.api(t, http.MethodPost, "rooms", map[string]any{
		"name": name, "provider": "vkcalls", "transport": "vp8channel", "room": url,
	}, http.StatusOK)
	s := tp.state(t)
	return s.Rooms[len(s.Rooms)-1]
}

func (tp *testPanel) addClient(t *testing.T, name string) Client {
	t.Helper()
	tp.api(t, http.MethodPost, "clients", map[string]string{"name": name}, http.StatusOK)
	clients := tp.p.store.snapshot().Clients
	return clients[len(clients)-1]
}

func keyIDOf(t *testing.T, key string) string {
	t.Helper()
	id, err := crypto.KeyIDFromHex(key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPanelAnswersOnlyUnderItsPath(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	for _, path := range []string{"/", "/api/state", "/other-path/", "/" + testPath + "x/"} {
		if code, _ := tp.call(t, http.MethodGet, path, nil, false); code != http.StatusNotFound {
			t.Fatalf("GET %s = %d, want 404", path, code)
		}
	}
	tp.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if code, _ := tp.call(t, http.MethodGet, "/"+testPath, nil, false); code != http.StatusMovedPermanently {
		t.Fatalf("GET /%s = %d, want a redirect to the slash", testPath, code)
	}
	res, err := tp.http.Get(tp.srv.URL + "/" + testPath + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK || !bytes.Contains(page, []byte("app.js")) {
		t.Fatalf("the page = %d %.80s", res.StatusCode, page)
	}
	if csp := res.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
		t.Fatalf("CSP = %q", csp)
	}
}

func TestPanelLoginGuardsTheAPI(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.api(t, http.MethodGet, "state", nil, http.StatusUnauthorized)
	if code, _ := tp.call(t, http.MethodPost, "/"+testPath+"/api/login",
		map[string]string{"password": testPassword}, false); code != http.StatusBadRequest {
		t.Fatalf("a login without the panel's header = %d, want 400", code)
	}
	tp.api(t, http.MethodPost, "login", map[string]string{"password": "wrong"}, http.StatusUnauthorized)
	tp.login(t)
	tp.state(t)
	if code, _ := tp.call(t, http.MethodPost, "/"+testPath+"/api/clients",
		map[string]string{"name": "x"}, false); code != http.StatusForbidden {
		t.Fatalf("a change without the panel's header = %d, want 403", code)
	}
	tp.api(t, http.MethodPost, "logout", nil, http.StatusOK)
	tp.api(t, http.MethodGet, "state", nil, http.StatusUnauthorized)
}

func TestPanelLoginLocksOutAfterRepeatedFailures(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	for range maxFailures {
		tp.api(t, http.MethodPost, "login", map[string]string{"password": "wrong"}, http.StatusUnauthorized)
	}
	tp.api(t, http.MethodPost, "login", map[string]string{"password": testPassword}, http.StatusTooManyRequests)
	tp.api(t, http.MethodGet, "state", nil, http.StatusUnauthorized)
}

func TestPanelRoomRunsWhileEnabled(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.login(t)
	tp.rooms.waitRunning(t, ConfigRoomID, true)

	room := tp.addRoom(t, "vk-2", vkRoom)
	if got := tp.rooms.waitRunning(t, room.ID, true); got.URL != vkRoom || got.Provider != "vkcalls" {
		t.Fatalf("the room ran as %+v", got)
	}
	tp.api(t, http.MethodPatch, "rooms/"+room.ID, map[string]bool{"disabled": true}, http.StatusOK)
	tp.rooms.waitRunning(t, room.ID, false)
	if s := tp.state(t); s.Rooms[1].State != stateStopped {
		t.Fatalf("a disabled room reads %q", s.Rooms[1].State)
	}
	tp.api(t, http.MethodPatch, "rooms/"+room.ID, map[string]bool{"disabled": false}, http.StatusOK)
	tp.rooms.waitRunning(t, room.ID, true)

	tp.api(t, http.MethodPatch, "rooms/"+room.ID, map[string]string{"room": vkRoom + "-new"}, http.StatusOK)
	deadline := time.Now().Add(5 * time.Second)
	for tp.rooms.waitRunning(t, room.ID, true).URL != vkRoom+"-new" {
		if time.Now().After(deadline) {
			t.Fatal("the room kept its old link")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tp.api(t, http.MethodPatch, "rooms/"+room.ID, map[string]string{"name": "renamed"}, http.StatusOK)
	tp.rooms.mu.Lock()
	starts := tp.rooms.starts[room.ID]
	tp.rooms.mu.Unlock()
	if starts != 3 {
		t.Fatalf("the room started %d times, want 3: a rename must not restart it", starts)
	}
	if s := tp.state(t); s.Rooms[1].Name != "renamed" || s.Rooms[1].State != string(server.LinkUp) {
		t.Fatalf("the room reads %+v", s.Rooms[1])
	}

	tp.api(t, http.MethodDelete, "rooms/"+room.ID, nil, http.StatusOK)
	tp.rooms.waitRunning(t, room.ID, false)
	tp.api(t, http.MethodDelete, "rooms/"+ConfigRoomID, nil, http.StatusNotFound)
	tp.rooms.waitRunning(t, ConfigRoomID, true)
}

func TestPanelRefusesBadRooms(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.login(t)
	bad := []map[string]any{
		{"name": "x", "provider": "nope", "transport": "vp8channel", "room": vkRoom},
		{"name": "x", "provider": "vkcalls", "transport": "nope", "room": vkRoom},
		{"name": "", "provider": "vkcalls", "transport": "vp8channel", "room": vkRoom},
		{"name": "a$b", "provider": "vkcalls", "transport": "vp8channel", "room": vkRoom},
		{"name": "x", "provider": "vkcalls", "transport": "vp8channel", "room": ""},
		{"name": "x", "provider": "vkcalls", "transport": "vp8channel", "room": vkRoom + "#frag"},
		{"name": "x", "provider": "vkcalls", "transport": "vp8channel", "room": vkRoom + " x"},
		{"name": "x", "provider": "vkcalls", "transport": "vp8channel", "create": true},
	}
	for _, body := range bad {
		tp.api(t, http.MethodPost, "rooms", body, http.StatusBadRequest)
	}
	if n := len(tp.state(t).Rooms); n != 1 {
		t.Fatalf("%d rooms after refused adds, want the config's alone", n)
	}
	tp.api(t, http.MethodPost, "rooms", map[string]any{
		"name": "jazz", "provider": "salutejazz", "transport": "vp8channel", "create": true,
	}, http.StatusOK)
	if s := tp.state(t); s.Rooms[1].URL != "made:pass" {
		t.Fatalf("a created room got %q", s.Rooms[1].URL)
	}
}

func TestPanelClientKeyFollowsTheClient(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.login(t)
	if !tp.p.keys.Ring().Has(keyIDOf(t, configKey)) || tp.p.keys.Ring().Len() != 1 {
		t.Fatal("the config's key is not alone in the ring")
	}
	c := tp.addClient(t, "phone")
	id := keyIDOf(t, c.Key)
	if len(c.Key) != 64 || len(c.Sub) != 64 || !tp.p.keys.Ring().Has(id) {
		t.Fatalf("a new client got key %d chars, sub %d chars, in ring %v", len(c.Key), len(c.Sub), tp.p.keys.Ring().Has(id))
	}
	if raw := tp.api(t, http.MethodGet, "state", nil, http.StatusOK); bytes.Contains(raw, []byte(c.Key)) {
		t.Fatal("the state gives a client's key away")
	}
	tp.api(t, http.MethodPatch, "clients/"+c.ID, map[string]bool{"disabled": true}, http.StatusOK)
	if tp.p.keys.Ring().Has(id) {
		t.Fatal("a disabled client's key is still in the ring")
	}
	tp.api(t, http.MethodPatch, "clients/"+c.ID, map[string]bool{"disabled": false}, http.StatusOK)
	if !tp.p.keys.Ring().Has(id) {
		t.Fatal("an enabled client's key is not in the ring")
	}
	tp.api(t, http.MethodDelete, "clients/"+c.ID, nil, http.StatusOK)
	if tp.p.keys.Ring().Has(id) || !tp.p.keys.Ring().Has(keyIDOf(t, configKey)) {
		t.Fatal("deleting a client left its key, or took the config's")
	}
	tp.api(t, http.MethodDelete, "clients/"+c.ID, nil, http.StatusNotFound)
	tp.api(t, http.MethodPost, "clients", map[string]string{"name": " "}, http.StatusBadRequest)
}

type linksBody struct {
	Sub   string `json:"sub"`
	Links []struct {
		Room     string `json:"room"`
		Link     string `json:"link"`
		Disabled bool   `json:"disabled"`
	} `json:"links"`
}

func TestPanelLinksCarryTheClientKeyIntoEveryRoom(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.login(t)
	room := tp.addRoom(t, "vk-2", vkRoom)
	c := tp.addClient(t, "phone")
	var links linksBody
	if err := json.Unmarshal(tp.api(t, http.MethodGet, "clients/"+c.ID+"/links", nil, http.StatusOK), &links); err != nil {
		t.Fatal(err)
	}
	if links.Sub != tp.srv.URL+"/sub/"+c.Sub {
		t.Fatalf("sub = %q", links.Sub)
	}
	if len(links.Links) != 2 {
		t.Fatalf("%d links, want one per room", len(links.Links))
	}
	l, err := link.Parse(links.Links[1].Link)
	if err != nil {
		t.Fatal(err)
	}
	want := link.Link{Provider: "vkcalls", Transport: "vp8channel", Room: room.URL, Key: c.Key,
		Label: "phone / vk-2", VP8FPS: 60, VP8Batch: 64}
	if l != want {
		t.Fatalf("link = %+v, want %+v", l, want)
	}
	tp.api(t, http.MethodGet, "clients/nope/links", nil, http.StatusNotFound)
}

func TestPanelSubscriptionFollowsRoomsAndClient(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.login(t)
	room := tp.addRoom(t, "vk-2", vkRoom)
	c := tp.addClient(t, "phone")
	sub := func(token string, code int) string {
		t.Helper()
		anon := &http.Client{}
		res, err := anon.Get(tp.srv.URL + "/sub/" + token)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != code {
			t.Fatalf("GET /sub = %d, want %d", res.StatusCode, code)
		}
		return string(raw)
	}
	body := sub(c.Sub, http.StatusOK)
	if !strings.HasPrefix(body, "#name: olcrtc\n") || strings.Count(body, "olcrtc://") != 2 ||
		!strings.Contains(body, "##name: vk-2\n") || !strings.Contains(body, "#"+c.Key) {
		t.Fatalf("subscription:\n%s", body)
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "olcrtc://") {
			if _, err := link.Parse(line); err != nil {
				t.Fatalf("a subscription line does not parse: %v", err)
			}
		}
	}
	tp.api(t, http.MethodPatch, "rooms/"+room.ID, map[string]bool{"disabled": true}, http.StatusOK)
	if body = sub(c.Sub, http.StatusOK); strings.Count(body, "olcrtc://") != 1 || strings.Contains(body, vkRoom+"#") {
		t.Fatalf("a disabled room is still in the subscription:\n%s", body)
	}
	tp.api(t, http.MethodPatch, "clients/"+c.ID, map[string]bool{"rotate_sub": true}, http.StatusOK)
	sub(c.Sub, http.StatusNotFound)
	fresh := tp.p.store.snapshot().Clients[0]
	sub(fresh.Sub, http.StatusOK)
	tp.api(t, http.MethodPatch, "clients/"+c.ID, map[string]bool{"disabled": true}, http.StatusOK)
	sub(fresh.Sub, http.StatusNotFound)
	sub("nope", http.StatusNotFound)
}

func TestPanelPublicURLLeadsTheSubscription(t *testing.T) {
	tp := startPanel(t, filepath.Join(t.TempDir(), "panel.json"))
	tp.p.opts.PublicURL = "https://vpn.example.com:8443/"
	tp.login(t)
	c := tp.addClient(t, "phone")
	var links linksBody
	_ = json.Unmarshal(tp.api(t, http.MethodGet, "clients/"+c.ID+"/links", nil, http.StatusOK), &links)
	if links.Sub != "https://vpn.example.com:8443/sub/"+c.Sub {
		t.Fatalf("sub = %q", links.Sub)
	}
}

func TestPanelStoreOutlivesTheProcess(t *testing.T) {
	store := filepath.Join(t.TempDir(), "panel.json")
	tp := startPanel(t, store)
	tp.login(t)
	room := tp.addRoom(t, "vk-2", vkRoom)
	c := tp.addClient(t, "phone")
	tp.api(t, http.MethodPatch, "clients/"+c.ID, map[string]string{"note": "Anna"}, http.StatusOK)
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(store); err != nil || info.Mode().Perm() != storeMode {
			t.Fatalf("store mode = %v, %v; want %v", info.Mode().Perm(), err, os.FileMode(storeMode))
		}
	}

	again := startPanel(t, store)
	data := again.p.store.snapshot()
	if len(data.Rooms) != 1 || data.Rooms[0].ID != room.ID || len(data.Clients) != 1 ||
		data.Clients[0].Key != c.Key || data.Clients[0].Note != "Anna" {
		t.Fatalf("store read back as %+v", data)
	}
	if !again.p.keys.Ring().Has(keyIDOf(t, c.Key)) {
		t.Fatal("a stored client's key is not in the ring after a restart")
	}
	again.rooms.waitRunning(t, room.ID, true)
}

func TestPanelRefusesABadStore(t *testing.T) {
	store := filepath.Join(t.TempDir(), "panel.json")
	if err := os.WriteFile(store, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	rooms := &fakeRooms{running: map[string]Room{}, starts: map[string]int{}}
	if _, err := newPanel(context.Background(), testOptions(t, rooms, store)); err == nil {
		t.Fatal("a broken store was taken")
	}
}

func TestCheckOptionsRefusesAnUnprotectedPanel(t *testing.T) {
	rooms := &fakeRooms{}
	good := testOptions(t, rooms, "panel.json")
	cases := map[string]func(*Options){
		"short password":        func(o *Options) { o.Password = "short" },
		"short path":            func(o *Options) { o.Path = "abc" },
		"path with a slash":     func(o *Options) { o.Path = "abcd/efgh" },
		"public without tls":    func(o *Options) { o.Listen = "0.0.0.0:8443" },
		"cert without key":      func(o *Options) { o.Cert = "cert.pem" },
		"listen without a port": func(o *Options) { o.Listen = "127.0.0.1" },
		"no room runner":        func(o *Options) { o.RunRoom = nil },
	}
	for name, mutate := range cases {
		opts := good
		mutate(&opts)
		if err := checkOptions(opts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, listen := range []string{"127.0.0.1:8884", "[::1]:8884", "localhost:8884"} {
		opts := good
		opts.Listen = listen
		if err := checkOptions(opts); err != nil {
			t.Errorf("%s: %v", listen, err)
		}
	}
	opts := good
	opts.Listen, opts.Cert, opts.Key = "0.0.0.0:8443", "cert.pem", "key.pem"
	if err := checkOptions(opts); err != nil {
		t.Errorf("public with tls: %v", err)
	}
}

func TestRoomStateReadsStoppedForAnUnknownRoom(t *testing.T) {
	rr := newRoomRunner((&fakeRooms{}).run, nil, nil)
	if got := rr.state("nope").State; got != stateStopped {
		t.Fatalf("state = %q", got)
	}
}

func TestRoomWaitsForItsPreviousServerToLeave(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	starts, live, maxLive := 0, 0, 0
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return starts
	}
	run := func(ctx context.Context, _ Room, _ *server.Keys, _ *server.Meter, _ func(server.LinkState)) error {
		mu.Lock()
		starts, live = starts+1, live+1
		maxLive = max(maxLive, live)
		mu.Unlock()
		<-ctx.Done()
		<-release
		mu.Lock()
		live--
		mu.Unlock()
		return nil
	}
	rr := newRoomRunner(run, nil, nil)
	room := Room{ID: "r", URL: vkRoom}
	off := room
	off.Disabled = true
	ctx := context.Background()
	rr.sync(ctx, []Room{room})
	for count() != 1 {
		time.Sleep(time.Millisecond)
	}
	rr.sync(ctx, []Room{off})
	rr.sync(ctx, []Room{room})
	time.Sleep(50 * time.Millisecond)
	if n := count(); n != 1 {
		t.Fatalf("the room started %d servers while the first was still leaving", n)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for count() != 2 {
		if time.Now().After(deadline) {
			t.Fatal("the room never started again")
		}
		time.Sleep(time.Millisecond)
	}
	rr.stop()
	if maxLive != 1 {
		t.Fatalf("%d servers ran in the room at once", maxLive)
	}
}
