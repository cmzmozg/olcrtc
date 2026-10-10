package client

import (
	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// The second data lane is an smux session of its own over a KCP session of
// its own, beside the data session. A stream opened there neither queues
// behind the data session's bulk nor waits out a segment lost ahead of it,
// and the other way round. The client opens it once a welcome announces it
// and puts each new stream on the lane that carries less.
//
// ai-generated: the whole file (second lane).

// attachLane gives pair its second lane when the server takes streams there.
func (c *Client) attachLane(pair *tunnelcore.SessionPair, welcome handshake.Welcome) {
	if !welcome.Has(handshake.FeatureLanes) {
		return
	}
	if err := pair.AttachLane(c.ln, tunnelcore.ClientRole); err != nil {
		logger.Warnf("second lane: %v", err)
		return
	}
	if pair.LaneSession != nil {
		logger.Infof("second lane open")
	}
}

// streamSession is the session a new stream opens on: the second lane's,
// when session has one up that carries less than session's own, else session.
func (c *Client) streamSession(session *smux.Session) *smux.Session {
	c.sessMu.RLock()
	pair := c.pair
	c.sessMu.RUnlock()
	if pair == nil || pair.DataSession != session || pair.LaneSession == nil || pair.LaneSession.IsClosed() {
		return session
	}
	lanes, ok := c.ln.(transport.LanePlane)
	if !ok {
		return session
	}
	if data, lane := lanes.LaneLoads(); lane < data {
		return pair.LaneSession
	}
	return session
}

// lanePayloadBytes is the payload the current second lane has opened, for
// liveness: a stream there is as much the peer delivering as one on the data
// session. Callers hold sessMu.
func (c *Client) lanePayloadBytes() uint64 {
	if c.pair == nil {
		return 0
	}
	return c.pair.LaneConn.PayloadBytes()
}
