package vkcalls

// ai-generated: a loopback VK signaling/SFU fixture with real Pion negotiation.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

type lifecycleSFU struct {
	mu          sync.Mutex
	connections []*websocket.Conn
	peers       []*webrtc.PeerConnection
	joined      atomic.Int32
	active      atomic.Int32
	directFirst bool
	stopping    bool
	errors      chan error
}

// ai-generated: own every fixture connection and peer until test cleanup.
func newLifecycleSFU(t *testing.T, directFirst bool) (*lifecycleSFU, string) {
	t.Helper()
	f := &lifecycleSFU{directFirst: directFirst, errors: make(chan error, 32)}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() {
		f.mu.Lock()
		f.stopping = true
		f.mu.Unlock()
		server.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, conn := range f.connections {
			_ = conn.Close()
		}
		for _, peer := range f.peers {
			_ = peer.Close()
		}
	})
	return f, "ws" + strings.TrimPrefix(server.URL, "http") + "?userId=1"
}

// ai-generated: the first join may stay DIRECT; subsequent joins negotiate media.
func (f *lifecycleSFU) serve(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	if f.stopping {
		f.mu.Unlock()
		_ = conn.Close()
		return
	}
	f.connections = append(f.connections, conn)
	f.mu.Unlock()
	defer func() { _ = conn.Close() }()
	f.active.Add(1)
	defer f.active.Add(-1)
	topology := topologyServer
	if f.joined.Add(1) == 1 && f.directFirst {
		topology = "DIRECT"
	}
	_ = conn.WriteJSON(map[string]any{
		"notification": noteConnection, "conversation": map[string]any{"topology": topology},
	})
	for {
		_, raw, readErr := conn.ReadMessage()
		if readErr != nil {
			return
		}
		if err := f.command(r.Context(), conn, raw); err != nil {
			f.errors <- err
			return
		}
	}
}

// ai-generated: answer the actual engine's commands and accept its shaped SDP.
func (f *lifecycleSFU) command(ctx context.Context, conn *websocket.Conn, raw []byte) error {
	var frame struct {
		Command     string `json:"command"`
		Sequence    int64  `json:"sequence"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	if frame.Command == "hangup" {
		return nil
	}
	f.mu.Lock()
	var peer *webrtc.PeerConnection
	if len(f.peers) > 0 {
		peer = f.peers[len(f.peers)-1]
	}
	f.mu.Unlock()
	if frame.Command == "accept-producer" {
		if err := peer.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeAnswer, SDP: frame.Description + "\r\n",
		}); err != nil {
			return err
		}
	}
	if err := conn.WriteJSON(map[string]any{"sequence": frame.Sequence, "response": frame.Command}); err != nil {
		return err
	}
	if frame.Command != "allocate-consumer" {
		return nil
	}
	peer, offer, err := lifecycleOffer(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	if f.stopping {
		f.mu.Unlock()
		_ = peer.Close()
		return nil
	}
	f.peers = append(f.peers, peer)
	f.mu.Unlock()
	return conn.WriteJSON(map[string]any{
		"notification": noteProducer, "sessionId": strconv.FormatInt(frame.Sequence, 10), "description": offer,
	})
}

// ai-generated: create an SFU offer on local interfaces, without external services.
func lifecycleOffer(ctx context.Context) (*webrtc.PeerConnection, string, error) {
	settings := webrtc.SettingEngine{}
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	peer, err := webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, "", err
	}
	if _, addErr := peer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); addErr != nil {
		_ = peer.Close()
		return nil, "", addErr
	}
	if _, channelErr := peer.CreateDataChannel("fixture", nil); channelErr != nil {
		_ = peer.Close()
		return nil, "", channelErr
	}
	complete := webrtc.GatheringCompletePromise(peer)
	offer, err := peer.CreateOffer(nil)
	if err == nil {
		err = peer.SetLocalDescription(offer)
	}
	if err != nil {
		_ = peer.Close()
		return nil, "", err
	}
	select {
	case <-complete:
		return peer, peer.LocalDescription().SDP, nil
	case <-ctx.Done():
		_ = peer.Close()
		return nil, "", ctx.Err()
	}
}

// ai-generated: build an actual VK session with its VP8 publication track.
func lifecycleSession(t *testing.T, endpoint string) *Session {
	t.Helper()
	session, err := New(t.Context(), engine.Config{URL: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	s := session.(*Session)
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddVideoTrack(track); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
