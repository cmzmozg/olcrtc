package server

// ai-generated: the whole file (panel).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
)

func ringKey(b byte) string { return strings.Repeat(string("0123456789abcdef"[b%16]), 64) }

func TestKeysCutOffOnlyWhenAKeyLeaves(t *testing.T) {
	keys, err := NewKeys(nil)
	if err != nil {
		t.Fatal(err)
	}
	cut := keys.cutoff()
	if err = keys.Set([]string{ringKey(1), ringKey(2)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cut:
		t.Fatal("keys added cut peers off")
	default:
	}
	if err = keys.Set([]string{ringKey(1)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cut:
	default:
		t.Fatal("a key removed cut nobody off")
	}
	if err = keys.Set([]string{"zz"}); err == nil || keys.Ring().Len() != 1 {
		t.Fatalf("a bad key was taken: err %v, %d keys", err, keys.Ring().Len())
	}
}

func TestRevokedKeyCutsItsPeersOffAndNoOthers(t *testing.T) {
	s, cleanup := newPeerLifecycleServer(t)
	defer cleanup()
	keys, err := NewKeys([]string{ringKey(1), ringKey(2)})
	if err != nil {
		t.Fatal(err)
	}
	s.keys = keys
	entries := keys.Ring().Entries()
	kept := s.peerSessionIn("peer-kept", muxconn.PrePinned(entries[0].Keys, entries[0].KeyID))
	s.peerSessionIn("peer-cut", muxconn.PrePinned(entries[1].Keys, entries[1].KeyID))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go s.watchKeys(ctx)

	if err = keys.Set([]string{ringKey(1), ringKey(3)}); err != nil {
		t.Fatal(err)
	}
	if err = keys.Set([]string{ringKey(1)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for lookupPeer(s, "peer-cut") != nil {
		if time.Now().After(deadline) {
			t.Fatal("the peer of the removed key is still served")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lookupPeer(s, "peer-kept") != kept {
		t.Fatal("the peer of a kept key was cut off")
	}
}
