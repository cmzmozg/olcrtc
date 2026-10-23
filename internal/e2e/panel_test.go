package e2e

// ai-generated: the whole file (panel).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/client"
	"github.com/openlibrecommunity/olcrtc/internal/link"
	"github.com/openlibrecommunity/olcrtc/internal/panel"
)

const (
	e2ePanelPath     = "e2e-panel-path"
	e2ePanelPassword = "e2e-panel-password"
)

var errPanelEcho = errors.New("echo mismatch")

// panelAPI drives a running panel's API as its page does.
type panelAPI struct {
	base string
	http *http.Client
}

func (a *panelAPI) do(t *testing.T, method, path string, body, out any) int {
	t.Helper()
	return a.doCtx(context.Background(), t, method, path, body, out)
}

// doCtx is do within ctx.
func (a *panelAPI) doCtx(ctx context.Context, t *testing.T, method, path string, body, out any) int {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Olcrtc-Panel", "1")
	res, err := a.http.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = res.Body.Close() }()
	if out != nil {
		_ = json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode
}

// startPanelServer runs a panel-managed server over the memory provider and
// logs in to its panel.
func startPanelServer(ctx context.Context, t *testing.T, providerName string) *panelAPI {
	t.Helper()
	addr := freeLocalAddr(ctx, t)
	cfg := session.Config{
		Mode: session.ModeSrv, Provider: providerName, Transport: transportVP8,
		DNSServer: localDNSServer, UDPDisabled: true, UnsafeAllowPrivateTargets: true,
	}
	opts := panel.Options{
		Listen: addr, Path: e2ePanelPath, Password: e2ePanelPassword,
		Store: filepath.Join(t.TempDir(), "panel.json"),
	}
	go func() { _ = session.RunPanel(ctx, cfg, opts) }()
	jar, _ := cookiejar.New(nil)
	api := &panelAPI{base: "http://" + addr + "/" + e2ePanelPath + "/api/", http: &http.Client{Jar: jar}}
	deadline := time.Now().Add(10 * time.Second)
	for api.doCtx(ctx, t, http.MethodPost, "login", map[string]string{"password": e2ePanelPassword}, nil) != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("the panel never let the admin in")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return api
}

// panelClientTraffic is what the panel's state shows for its first client.
func panelClientTraffic(t *testing.T, api *panelAPI) (string, uint64, uint64) {
	t.Helper()
	var state struct {
		Clients []struct {
			ID       string `json:"id"`
			Up, Down uint64
		} `json:"clients"`
	}
	if code := api.do(t, http.MethodGet, "state", nil, &state); code != http.StatusOK || len(state.Clients) == 0 {
		t.Fatalf("state = %d with %d clients", code, len(state.Clients))
	}
	c := state.Clients[0]
	return c.ID, c.Up, c.Down
}

// echoAttempt runs one SOCKS echo round trip within a few seconds.
func echoAttempt(socksAddr, echoAddr, tag string) error {
	conn, err := net.DialTimeout("tcp4", socksAddr, time.Second)
	if err != nil {
		return fmt.Errorf("dial socks: %w", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	host, portText, _ := net.SplitHostPort(echoAddr)
	port, _ := strconv.Atoi(portText)
	req := append([]byte{5, 1, 0, 5, 1, 0, 1}, net.ParseIP(host).To4()...)
	req = binary.BigEndian.AppendUint16(req, uint16(port)) //nolint:gosec // a TCP port
	if _, err = conn.Write(req); err != nil {
		return fmt.Errorf("socks request: %w", err)
	}
	reply := make([]byte, 12)
	if _, err = io.ReadFull(conn, reply); err != nil || reply[3] != 0 {
		return fmt.Errorf("socks reply %v: %w", reply, err)
	}
	payload := []byte(tag + "\n")
	if _, err = conn.Write(payload); err != nil {
		return fmt.Errorf("echo write: %w", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("echo read: %w", err)
	}
	if !bytes.Equal(line, payload) {
		return errPanelEcho
	}
	return nil
}

// TestPanelIssuedLinkRelaysAndDeletionCutsItOff walks the panel's whole job
// over the real smux and vp8channel stack: a room added in the panel starts
// a server, a client added gets a link, the link carries a tunnel that the
// panel meters, and deleting the client cuts the tunnel off.
func TestPanelIssuedLinkRelaysAndDeletionCutsItOff(t *testing.T) {
	echoAddr := startEchoServer(t)
	providerName, room := registerMemoryProvider(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	api := startPanelServer(ctx, t, providerName)

	if code := api.do(t, http.MethodPost, "rooms", map[string]string{
		"name": "mem", "provider": providerName, "transport": transportVP8, "room": testRoom,
	}, nil); code != http.StatusOK {
		t.Fatalf("add room = %d", code)
	}
	room.waitConnected(t, 1)
	if code := api.do(t, http.MethodPost, "clients", map[string]string{"name": "phone"}, nil); code != http.StatusOK {
		t.Fatalf("add client = %d", code)
	}
	id, _, _ := panelClientTraffic(t, api)
	var links struct {
		Links []struct{ Link string } `json:"links"`
	}
	if code := api.do(t, http.MethodGet, "clients/"+id+"/links", nil, &links); code != http.StatusOK || len(links.Links) != 1 {
		t.Fatalf("links = %d, %d links", code, len(links.Links))
	}
	l, err := link.Parse(links.Links[0].Link)
	if err != nil {
		t.Fatal(err)
	}

	socksAddr := freeLocalAddr(ctx, t)
	ready := make(chan struct{})
	go func() {
		_ = client.RunWithReady(ctx, client.Config{
			Transport: l.Transport, TransportOptions: e2eTransportOptions(l.Transport),
			Provider: l.Provider, RoomURL: l.Room, KeyHex: l.Key,
			DeviceID: testClientDeviceID, LocalAddr: socksAddr, DNSServer: localDNSServer,
		}, func() { close(ready) })
	}()
	select {
	case <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("the client with the panel's link never paired")
	}
	if err = echoAttempt(socksAddr, echoAddr, "olcrtc-panel"); err != nil {
		t.Fatalf("echo through the panel's room: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		_, up, down := panelClientTraffic(t, api)
		if up > 0 && down > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the panel metered %d up, %d down", up, down)
		}
		time.Sleep(200 * time.Millisecond)
	}

	if code := api.do(t, http.MethodDelete, "clients/"+id, nil, nil); code != http.StatusOK {
		t.Fatalf("delete client = %d", code)
	}
	deadline = time.Now().Add(30 * time.Second)
	for echoAttempt(socksAddr, echoAddr, "olcrtc-revoked") == nil {
		if time.Now().After(deadline) {
			t.Fatal("a deleted client still tunnels")
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	if echoAttempt(socksAddr, echoAddr, "olcrtc-revoked") == nil {
		t.Fatal("a deleted client got its tunnel back")
	}
}
