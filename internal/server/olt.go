package server

import (
	"bytes"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/olt"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// A peer's olt data plane is built on its first olt packet, once the peer has
// a session: a client starts one only after a welcome that announced it. Its
// streams are served as the data session's are, under the same session id.
//
// ai-generated: the whole file (olt data plane).

// oltBurst is the frame size olt allows for when the transport names none.
const oltBurst = 64 << 10

// handleOLT takes a datagram for a peer's olt data plane, and reports whether
// it was one.
func (s *Server) handleOLT(peerID string, datagram []byte) bool {
	if !bytes.HasPrefix(datagram, []byte(tunnelcore.OLTMagic)) {
		return false
	}
	s.sessMu.Lock()
	peer := s.peerSessions[peerID]
	s.sessMu.Unlock()
	if peer == nil || peer.sid() == "" {
		return true
	}
	entry := peer.group.Pinned()
	if entry == nil {
		return true
	}
	packet, _ := tunnelcore.OLTPacket(entry.Keys, datagram)
	if packet == nil {
		return true
	}
	if conn := s.peerOLT(peer, entry.Keys); conn != nil {
		conn.Input(packet)
	}
	return true
}

// peerOLT returns peer's olt connection, building it and its accept loop on
// first use.
func (s *Server) peerOLT(peer *peerSession, keys *crypto.KeySet) *olt.Conn {
	dg, ok := s.ln.(transport.PeerDatagramTransport)
	if !ok {
		return nil
	}
	sessionID := peer.sid() // read before peer.olt takes ps.mu
	conn, built := peer.olt(func() *olt.Conn {
		burst := s.ln.Features().MaxPayloadSize
		if burst <= 0 {
			burst = oltBurst
		}
		send := func(p []byte) error { return dg.SendDatagramTo(peer.peerID, p) }
		conn, err := tunnelcore.NewOLT(keys, send, false, sessionID, burst)
		if err != nil {
			logger.Warnf("olt data plane for peer %s: %v", peer.peerID, err)
			return nil
		}
		return conn
	})
	if built {
		logger.Infof("server: olt data plane created peerID=%s", peer.peerID)
		s.goTracked(func() { s.serveLane(peer, tunnelcore.OLTSession{Conn: conn}) })
	}
	return conn
}

// olt returns the peer's olt connection, building it with build when there is
// none; built says this call built it.
func (ps *peerSession) olt(build func() *olt.Conn) (*olt.Conn, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed || ps.oltConn != nil {
		return ps.oltConn, false
	}
	ps.oltConn = build()
	return ps.oltConn, ps.oltConn != nil
}
