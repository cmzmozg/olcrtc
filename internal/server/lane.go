package server

import (
	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// A peer's second data lane carries an smux session of its own, built on the
// lane's first frame from a peer that has a session already: a client opens
// it only after a welcome that announced the lanes. Its streams are served as
// the data session's are, under the same session id. A lane that fails takes
// nothing else down: the client still has its data session.
//
// ai-generated: the whole file (second lane).

// peerFeatures is what the welcome of a peer session announces.
func (s *Server) peerFeatures() []string {
	if _, ok := s.ln.(transport.PeerLanePlane); ok {
		return []string{handshake.FeatureEarlyData, handshake.FeatureLanes}
	}
	return []string{handshake.FeatureEarlyData}
}

func (s *Server) installPeerLanePlane() {
	if plane, ok := s.ln.(transport.PeerLanePlane); ok {
		plane.SetLaneOnPeerData(s.onPeerLaneData)
	}
}

func (s *Server) onPeerLaneData(peerID string, data []byte) {
	s.sessMu.Lock()
	peer := s.peerSessions[peerID]
	s.sessMu.Unlock()
	if peer == nil {
		return
	}
	if conn := s.peerLane(peer); conn != nil {
		conn.Push(data)
	}
}

// peerLane returns the conn of peer's second lane, building the lane and its
// session on first use. The accept loop starts outside peer.mu: goTracked
// takes sessMu, which is taken before peer.mu elsewhere.
func (s *Server) peerLane(peer *peerSession) *muxconn.Conn {
	conn, session := peer.lane(func() (*muxconn.Conn, *smux.Session) {
		conn := muxconn.NewPeerLaneUnboundGrouped(s.ln, peer.group, peer.peerID)
		if conn == nil {
			return nil, nil
		}
		session, err := tunnelcore.NewSession(conn, tunnelcore.ServerRole, runtime.SmuxConfigFor(s.ln))
		if err != nil {
			logger.Warnf("lane smux init failed for peer %s: %v", peer.peerID, err)
			_ = conn.Close()
			return nil, nil
		}
		return conn, session
	})
	if session != nil {
		logger.Infof("server: second lane session created peerID=%s", peer.peerID)
		s.goTracked(func() { s.serveLane(peer, tunnelcore.WrapSmux(session)) })
	}
	return conn
}

// lane returns the peer's lane conn, building it with build when there is
// none; the session is returned only when it was built by this call.
func (ps *peerSession) lane(build func() (*muxconn.Conn, *smux.Session)) (*muxconn.Conn, *smux.Session) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed || ps.laneConn != nil {
		return ps.laneConn, nil
	}
	ps.laneConn, ps.laneSess = build()
	return ps.laneConn, ps.laneSess
}

// serveLane serves the streams of a peer's second lane once its handshake
// has landed, until the lane's session ends.
func (s *Server) serveLane(peer *peerSession, session tunnelcore.StreamSession) {
	if peer.sessionReady != nil {
		select {
		case <-peer.sessionReady:
		case <-s.done:
			return
		}
	}
	sessionID := peer.sid()
	if sessionID == "" {
		return
	}
	ctx := s.streamContext()
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			if !s.stopping() {
				logger.Infof("server: AcceptStream(lane peer=%s) ended: %v", peer.peerID, err)
			}
			return
		}
		s.goTracked(func() { s.handleStream(ctx, stream, sessionID) })
	}
}

// lanePayloadBytes is the payload the peer's second lane has opened, for
// liveness.
func (ps *peerSession) lanePayloadBytes() uint64 {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.laneConn.PayloadBytes()
}
