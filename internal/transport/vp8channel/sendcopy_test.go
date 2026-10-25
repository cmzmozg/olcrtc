package vp8channel

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ai-generated: assert that only server VP8 opts into synchronous send copies.
func TestCopiesOnSendServerOnly(t *testing.T) {
	for _, server := range []bool{false, true} {
		tr := &streamTransport{serverMode: server}
		var copying transport.CopyingSender = tr
		if copying.CopiesOnSend() != server {
			t.Fatalf("CopiesOnSend with server=%t", server)
		}
	}
}

// ai-generated: carry concurrent encrypted server writes over real VP8 KCP data, control and lane paths.
func TestServerSendScratchAcrossVP8Planes(t *testing.T) {
	channels := []chan []byte{make(chan []byte, 16), make(chan []byte, 16), make(chan []byte, 16)}
	server, client := sendScratchPair(t, channels)
	psk := []byte("01234567890123456789012345678901")
	sender, err := crypto.NewKeySet(psk, crypto.Server)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := crypto.NewKeySet(psk, crypto.Client)
	if err != nil {
		t.Fatal(err)
	}
	peer := client.LocalPeerID()
	conns := []*muxconn.Conn{
		muxconn.NewPeer(server, sender, peer),
		muxconn.NewPeerControlUnbound(server, sender, peer),
		muxconn.NewPeerLaneUnboundGrouped(server, muxconn.PrePinned(sender, ""), peer),
	}
	for i, plane := range []string{"data", "control", "lane"} {
		t.Run(plane, func(t *testing.T) {
			conn := conns[i]
			defer func() { _ = conn.Close() }()
			writeScratchRecords(t, conn)
			readScratchRecords(t, receiver, channels[i], []byte("olcrtc/muxconn/v2/"+plane))
		})
	}
}

// ai-generated: build an in-memory VP8 pair and create every server send plane.
func sendScratchPair(t *testing.T, channels []chan []byte) (*streamTransport, *streamTransport) {
	t.Helper()
	toClient, toServer := newLossyRelay(0, time.Millisecond, 1), newLossyRelay(0, time.Millisecond, 2)
	client := newLossyTestTransport(t, transport.Config{
		OnData: func(b []byte) { channels[0] <- bytes.Clone(b) },
	}, toServer)
	client.SetControlOnData(func(b []byte) { channels[1] <- bytes.Clone(b) })
	client.SetLaneOnData(func(b []byte) { channels[2] <- bytes.Clone(b) })
	server := newLossyTestTransport(t, transport.Config{OnPeerData: func(string, []byte) {}}, toClient)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go toClient.run(ctx, client.handleIncomingFrame)
	go toServer.run(ctx, server.handleIncomingFrame)
	if err := client.ConfirmPeer(server.LocalPeerID()); err != nil {
		t.Fatal(err)
	}
	if err := client.ControlSend([]byte("claim control")); err != nil {
		t.Fatal(err)
	}
	if err := client.LaneSend([]byte("claim lane")); err != nil {
		t.Fatal(err)
	}
	peer := client.LocalPeerID()
	waitFor(t, 5*time.Second, "server send planes", func() bool {
		return server.peers.lookup(client.localEpochValue()) != nil &&
			server.ControlPeerCanSend(peer) && server.LanePeerCanSend(peer)
	})
	return server, client
}

// ai-generated: force concurrent pool borrows and immediately mutate released plaintext inputs.
func writeScratchRecords(t *testing.T, conn *muxconn.Conn) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		value := byte(i)
		wg.Go(func() {
			p := bytes.Repeat([]byte{value}, 4*1024)
			_, err := conn.Write(p)
			clear(p)
			errs <- err
		})
	}
	wg.Wait()
	for range 8 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// ai-generated: verify every delayed ciphertext and lane AAD after the send scratch was reused.
func readScratchRecords(t *testing.T, keys *crypto.KeySet, records <-chan []byte, aad []byte) {
	t.Helper()
	seen := make([]bool, 8)
	for range 8 {
		select {
		case record := <-records:
			p, err := keys.Open(record, aad)
			if err != nil || len(p) != 4*1024 || int(p[0]) >= len(seen) {
				t.Fatalf("received invalid scratch record: %v", err)
			}
			i := int(p[0])
			if seen[i] || !bytes.Equal(p, bytes.Repeat([]byte{p[0]}, len(p))) {
				t.Fatal("scratch record was overwritten")
			}
			seen[i] = true
		case <-time.After(5 * time.Second):
			t.Fatal("server scratch record did not reach client")
		}
	}
}
