package server

// ai-generated: the whole file (panel: a key ring that changes while servers
// run on it).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
)

// Keys is the key ring servers pair peers under. The panel changes it while
// they run: a key added pairs its client from the next first record on, and
// a key removed cuts its peers off in every server on the ring.
type Keys struct {
	ring atomic.Pointer[crypto.KeyRing]
	mu   sync.Mutex
	// cut closes when a key leaves the ring, and is replaced.
	cut chan struct{}
}

// NewKeys builds a ring of keysHex, 64 hex chars each; none is a ring no
// record opens under.
func NewKeys(keysHex []string) (*Keys, error) {
	k := &Keys{cut: make(chan struct{})}
	if err := k.Set(keysHex); err != nil {
		return nil, err
	}
	return k, nil
}

// staticKeys wraps a ring that never changes.
func staticKeys(ring *crypto.KeyRing) *Keys {
	k := &Keys{cut: make(chan struct{})}
	k.ring.Store(ring)
	return k
}

// Set replaces the ring with keysHex. The peers of a key it drops are cut
// off.
func (k *Keys) Set(keysHex []string) error {
	ring := crypto.EmptyKeyRing()
	if len(keysHex) > 0 {
		var err error
		if ring, err = crypto.NewKeyRing(keysHex, crypto.Server); err != nil {
			return fmt.Errorf("key ring: %w", err)
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if old := k.ring.Swap(ring); old != nil && dropsAny(old, ring) {
		close(k.cut)
		k.cut = make(chan struct{})
	}
	return nil
}

// Ring is the ring as it is now.
func (k *Keys) Ring() *crypto.KeyRing { return k.ring.Load() }

// cutoff closes the next time a key leaves the ring.
func (k *Keys) cutoff() <-chan struct{} {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.cut
}

// dropsAny reports whether next lacks a key of prev.
func dropsAny(prev, next *crypto.KeyRing) bool {
	for _, entry := range prev.Entries() {
		if !next.Has(entry.KeyID) {
			return true
		}
	}
	return false
}

// watchKeys cuts off the peers of every key that leaves the ring, until ctx
// ends. The channel is taken before the ring is read, so a key that leaves
// between the two is caught on the next turn.
func (s *Server) watchKeys(ctx context.Context) {
	for {
		cut := s.keys.cutoff()
		s.dropRevoked()
		select {
		case <-ctx.Done():
			return
		case <-cut:
		}
	}
}

// dropRevoked ends every peer session whose key the ring no longer has, an
// unpinned one too: its group holds the ring it was built on. The single
// link is rebuilt when its pinned key is gone.
func (s *Server) dropRevoked() {
	ring := s.keys.Ring()
	s.sessMu.RLock()
	var gone []*peerSession
	for _, peer := range s.peerSessions {
		if !ring.Has(peer.group.KeyID()) {
			gone = append(gone, peer)
		}
	}
	single := s.group.KeyID()
	s.sessMu.RUnlock()
	for _, peer := range gone {
		s.removePeer(peer, "key revoked")
	}
	if single != "" && !ring.Has(single) {
		s.reconnectLink("key revoked")
	}
}
