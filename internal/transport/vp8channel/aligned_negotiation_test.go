package vp8channel

// ai-generated: the whole file (negotiation of aligned datagram frames).

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// rtpRoom forwards every member's RTP packets, cut by pion's packetizer, to a
// track reader of every other member, as an SFU forwards a publisher's track
// to each subscriber, and counts the datagram frames and keepalive markers on
// the wire.
type rtpRoom struct {
	members []*streamTransport
	readers map[[2]*streamTransport]*trackReader // receiver, sender
	mu      sync.Mutex
	wire    map[wireKey]int
}

// wireKey is a kind of frame from a sender to a destination epoch.
type wireKey struct {
	from  *streamTransport
	dst   uint32
	magic string
}

// join binds every member's track to the room and connects them all.
func (r *rtpRoom) join(t *testing.T) {
	t.Helper()
	r.readers, r.wire = map[[2]*streamTransport]*trackReader{}, map[wireKey]int{}
	for _, sender := range r.members {
		for _, receiver := range r.members {
			if receiver != sender {
				r.readers[[2]*streamTransport{receiver, sender}] = newTrackReader(receiver)
			}
		}
		bindCapture(t, sender, func(pkt *rtp.Packet) {
			r.inspect(sender, pkt)
			for _, receiver := range r.members {
				if receiver != sender {
					r.readers[[2]*streamTransport{receiver, sender}].handleRTP(pkt)
				}
			}
		})
	}
	for _, tr := range r.members {
		if err := tr.Connect(context.Background()); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		t.Cleanup(func() { _ = tr.Close() })
	}
}

// inspect counts the first packet of every datagram frame and keepalive.
func (r *rtpRoom) inspect(from *streamTransport, pkt *rtp.Packet) {
	chunk := pkt.Payload[vp8DescriptorLen(pkt.Payload):]
	if !vp8PartitionStart(pkt) || len(chunk) < epochHdrLen+4 {
		return
	}
	magic := string(chunk[epochHdrLen : epochHdrLen+4])
	switch magic {
	case "OLUA", "OLUB", "OLUD", "OLUC":
		r.mu.Lock()
		r.wire[wireKey{from, binary.BigEndian.Uint32(chunk[dstOff:crcOff]), magic}]++
		r.mu.Unlock()
	}
}

func (r *rtpRoom) seen(from *streamTransport, dst uint32, magic string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wire[wireKey{from, dst, magic}]
}

// inbox counts the datagrams a member received, by content.
type inbox struct {
	mu  sync.Mutex
	got map[string]int
}

func (b *inbox) add(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.got[s]++
}

func (b *inbox) count(prefix string, n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range n {
		if b.got[fmt.Sprintf("%s%04d", prefix, i)] != 1 {
			return false
		}
	}
	return true
}

// datagram is datagram i from prefix, padded so a few fill a frame.
func datagram(prefix string, i int) []byte {
	b := make([]byte, 900)
	copy(b, fmt.Sprintf("%s%04d", prefix, i))
	return b
}

func label(b []byte) string {
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}

// TestAlignedFramesNegotiatePerPeer runs a server with a new and an old
// client, and a new client with an old server, through pion's packetizer.
// Aligned frames go only between two new builds, in both directions; an old
// build neither sends nor gets them, nor the keepalive marker that announces
// them, and datagrams reach every peer whatever its build.
func TestAlignedFramesNegotiatePerPeer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		serverOld bool
		clientOld []bool
	}{
		{"new server, new and old client", false, []bool{false, true}},
		{"old server, new client", true, []bool{false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runNegotiation(t, tc.serverOld, tc.clientOld)
		})
	}
}

func runNegotiation(t *testing.T, serverOld bool, clientOld []bool) {
	t.Helper()
	const sent = 30
	cfg := transport.Config{ChannelID: "negotiation-test"}
	serverIn := &inbox{got: map[string]int{}}
	serverCfg := cfg
	serverCfg.OnPeerData = func(string, []byte) {}
	serverCfg.OnPeerDatagram = func(peer string, b []byte) { serverIn.add(peer + ":" + label(b)) }
	server := newStreamTransport(&fakeVideoStream{canSend: true}, nil, serverCfg, Options{FPS: 30, BatchSize: 64})
	server.legacyWire = serverOld
	room := &rtpRoom{members: []*streamTransport{server}}
	clients := make([]*streamTransport, len(clientOld))
	inboxes := make([]*inbox, len(clientOld))
	for i, old := range clientOld {
		in := &inbox{got: map[string]int{}}
		clientCfg := cfg
		clientCfg.OnData = func([]byte) {}
		clientCfg.OnDatagram = func(b []byte) { in.add(label(b)) }
		clients[i] = newStreamTransport(&fakeVideoStream{canSend: true}, nil, clientCfg, Options{FPS: 30, BatchSize: 64})
		clients[i].legacyWire = old
		inboxes[i] = in
		room.members = append(room.members, clients[i])
	}
	room.join(t)
	for _, c := range clients {
		if err := c.ConfirmPeer(server.LocalPeerID()); err != nil {
			t.Fatalf("ConfirmPeer: %v", err)
		}
	}
	waitFor(t, 5*time.Second, "the peers' keepalives", func() bool {
		for i, c := range clients {
			sess := server.peers.lookup(c.localEpochValue())
			if sess == nil {
				return false
			}
			if !serverOld && !clientOld[i] && (!sess.aligned.Load() || c.alignedPeer.Load() != server.localEpochValue()) {
				return false
			}
		}
		return true
	})
	time.Sleep(300 * time.Millisecond) // several keepalives more each way
	for i, c := range clients {
		if (serverOld || clientOld[i]) && (server.peers.lookup(c.localEpochValue()).aligned.Load() || c.alignedPeer.Load() != 0) {
			t.Fatalf("client %d: a pair with an old build took the other for a new one", i)
		}
	}

	for i, c := range clients {
		peer := c.LocalPeerID()
		for n := range sent {
			if err := c.SendDatagram(datagram(fmt.Sprintf("c%d-", i), n)); err != nil {
				t.Fatalf("client SendDatagram: %v", err)
			}
			if err := server.SendDatagramTo(peer, datagram(fmt.Sprintf("s%d-", i), n)); err != nil {
				t.Fatalf("server SendDatagramTo: %v", err)
			}
		}
	}
	for i, c := range clients {
		peer := c.LocalPeerID()
		waitFor(t, 5*time.Second, "every datagram", func() bool {
			return inboxes[i].count(fmt.Sprintf("s%d-", i), sent) && serverIn.count(peer+":"+fmt.Sprintf("c%d-", i), sent)
		})
		assertWire(t, room, server, c, !serverOld && !clientOld[i])
	}
}

// assertWire checks what server and client put on the wire for each other.
func assertWire(t *testing.T, room *rtpRoom, server, client *streamTransport, aligned bool) {
	t.Helper()
	up, down := server.localEpochValue(), client.localEpochValue()
	upAligned, downAligned := room.seen(client, up, "OLUA"), room.seen(server, down, "OLUA")
	upLegacy := room.seen(client, up, "OLUB") + room.seen(client, up, "OLUD")
	downLegacy := room.seen(server, down, "OLUB") + room.seen(server, down, "OLUD")
	if aligned && (upAligned == 0 || downAligned == 0 || upLegacy+downLegacy != 0) {
		t.Fatalf("two new builds: aligned %d up, %d down; legacy %d up, %d down",
			upAligned, downAligned, upLegacy, downLegacy)
	}
	if !aligned && (upAligned+downAligned != 0 || upLegacy == 0 || downLegacy == 0) {
		t.Fatalf("an old build in the pair: aligned %d up, %d down; legacy %d up, %d down",
			upAligned, downAligned, upLegacy, downLegacy)
	}
	for _, tr := range []*streamTransport{server, client} {
		if marked := room.seen(tr, 0, "OLUC") > 0; marked == tr.legacyWire {
			t.Fatalf("a build with legacyWire=%v sent the aligned marker: %v", tr.legacyWire, marked)
		}
	}
}
