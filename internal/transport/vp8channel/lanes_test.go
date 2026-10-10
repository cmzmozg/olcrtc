package vp8channel

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// TestSecondLaneCarriesItsOwnFramesBothWays checks that what the client sends
// on the second lane reaches the server's lane callback and not its data one,
// that the server's answer on that peer's lane comes back on the client's
// lane, and that the data plane keeps its own frames.
//
// ai-generated: the whole test (second lane).
func TestSecondLaneCarriesItsOwnFramesBothWays(t *testing.T) {
	toClient, toServer := newLossyRelay(0, time.Millisecond, 1), newLossyRelay(0, time.Millisecond, 2)
	clientLane, clientData := make(chan []byte, 4), make(chan []byte, 4)
	client := newLossyTestTransport(t, transport.Config{
		OnData: func(b []byte) { clientData <- append([]byte(nil), b...) },
	}, toServer)
	client.SetLaneOnData(func(b []byte) { clientLane <- append([]byte(nil), b...) })
	serverLane, serverData := make(chan [2]string, 4), make(chan string, 4)
	server := newLossyTestTransport(t, transport.Config{
		OnPeerData: func(_ string, b []byte) { serverData <- string(b) },
	}, toClient)
	server.SetLaneOnPeerData(func(peer string, b []byte) { serverLane <- [2]string{peer, string(b)} })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go toClient.run(ctx, client.handleIncomingFrame)
	go toServer.run(ctx, server.handleIncomingFrame)
	if err := client.ConfirmPeer(server.LocalPeerID()); err != nil {
		t.Fatalf("ConfirmPeer: %v", err)
	}
	if err := client.LaneSend([]byte("lane hello")); err != nil {
		t.Fatalf("LaneSend: %v", err)
	}
	if err := client.Send([]byte("data hello")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	var peer string
	for range 2 {
		select {
		case m := <-serverLane:
			if m[1] != "lane hello" {
				t.Fatalf("server lane got %q", m[1])
			}
			peer = m[0]
		case d := <-serverData:
			if d != "data hello" {
				t.Fatalf("server data got %q, a lane frame on the data plane?", d)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the server did not get both frames")
		}
	}
	if err := server.LaneSendTo(peer, []byte("lane answer")); err != nil {
		t.Fatalf("LaneSendTo: %v", err)
	}
	select {
	case b := <-clientLane:
		if !bytes.Equal(b, []byte("lane answer")) {
			t.Fatalf("client lane got %q", b)
		}
	case b := <-clientData:
		t.Fatalf("the lane answer came back on the data plane: %q", b)
	case <-time.After(5 * time.Second):
		t.Fatal("the client did not get the lane answer")
	}
	if data, lane := client.LaneLoads(); data < 0 || lane < 0 {
		t.Fatalf("LaneLoads = %d, %d", data, lane)
	}
}
