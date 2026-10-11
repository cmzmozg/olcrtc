package vp8channel

import (
	"encoding/binary"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// The second data lane is a KCP session of its own beside the data plane,
// on the same epochs and track, told apart by its conversation id. A stream
// the client opens on it neither queues behind the data plane's bulk in
// KCP's one send queue nor waits at the peer for a data plane segment lost
// ahead of it: KCP delivers in order, so one lost segment held every stream
// on the plane. A receiver without the lane drops its packets as KCP drops
// any foreign conversation.
//
// ai-generated: the whole file (second lane).
const kcpLaneConvID uint32 = 0xC0FFEE02

var (
	_ transport.LanePlane     = (*streamTransport)(nil)
	_ transport.PeerLanePlane = (*streamTransport)(nil)
)

// newLanePlane is the client's second lane: a data plane on kcpLaneConvID
// whose frames go to the lane callback.
func newLanePlane(tr *streamTransport) *kcpPlane {
	plane := newDataPlane(outboundQueueSizeFor(), func(data []byte) {
		if cb := tr.laneOnData.Load(); cb != nil {
			(*cb)(data)
		}
	})
	plane.sndWnd, plane.conv = tr.sendWindow, kcpLaneConvID
	plane.kick = make(chan struct{}, 1)
	return plane
}

// startLane brings the client's second lane up with the data plane, and its
// writer once. The server keeps a lane per peer instead (laneOf).
func (p *streamTransport) startLane() {
	if p.serverMode || p.lane == nil {
		return
	}
	if _, err := p.lane.start(p.epochHeader()); err != nil {
		logger.Infof("vp8channel: start second lane: %v", err)
		return
	}
	p.laneOnce.Do(func() {
		go p.peerWriterPump(dataLane{
			out: p.lane.out, acks: p.lane.acks, kick: p.lane.kick, name: "lane",
			conn: func() *kcpConn {
				if rt := p.lane.get(); rt != nil {
					return rt.conn
				}
				return nil
			},
			window: func(segments int) {
				if rt := p.lane.get(); rt != nil {
					rt.sess.SetWindowSize(segments, 0)
				}
			},
			srtt: func() int32 {
				if rt := p.lane.get(); rt != nil {
					return rt.sess.GetSRTT()
				}
				return 0
			},
			path: &p.path, side: 1,
		}, nil)
	})
}

// laneRuntime is the client's live second lane, nil while it is down.
func (p *streamTransport) laneRuntime() *kcpRuntime {
	if p.lane == nil {
		return nil
	}
	return p.lane.get()
}

// LaneSend implements transport.LanePlane.
func (p *streamTransport) LaneSend(data []byte) error {
	return p.shaper.Send(func(payload []byte) error { return p.sendVia(p.laneRuntime(), payload) }, data)
}

// SetLaneOnData implements transport.LanePlane.
func (p *streamTransport) SetLaneOnData(cb func([]byte)) { p.laneOnData.Store(&cb) }

// LaneCanSend implements transport.LanePlane.
func (p *streamTransport) LaneCanSend() bool {
	return p.ready(p.laneRuntime(), p.stream.CanSend)
}

// LaneLoads implements transport.LanePlane: what each lane has queued for
// its writer and sent unanswered, and the packets it carried either way in
// about the last second. A download loads the server's queue, which the
// client sees only as what it receives. kcp-go keeps its own queue to itself.
func (p *streamTransport) LaneLoads() (int, int) {
	return planeLoad(p.data), planeLoad(p.lane)
}

func planeLoad(plane *kcpPlane) int {
	if plane == nil {
		return 0
	}
	load := len(plane.out)
	if rt := plane.get(); rt != nil {
		load += rt.conn.inFlight() + rt.conn.traffic.recent()
	}
	return load
}

// LaneSendTo implements transport.PeerLanePlane.
func (p *streamTransport) LaneSendTo(peerID string, data []byte) error {
	return p.shaper.Send(func(payload []byte) error {
		return p.sendToPeer(peerID, payload, p.peerLaneFor)
	}, data)
}

// SetLaneOnPeerData implements transport.PeerLanePlane.
func (p *streamTransport) SetLaneOnPeerData(cb func(peerID string, data []byte)) {
	p.laneOnPeerData.Store(&cb)
}

// LanePeerCanSend implements transport.PeerLanePlane.
func (p *streamTransport) LanePeerCanSend(peerID string) bool {
	epoch, err := parsePeerID(peerID)
	if err != nil {
		return false
	}
	return p.ready(p.peerLaneFor(epoch), p.stream.CanSend)
}

// peerLaneFor is a known peer's second lane, brought up on first use; nil for
// a peer the server does not know.
func (p *streamTransport) peerLaneFor(epoch uint32) *kcpRuntime {
	sess := p.peers.get(epoch)
	if sess == nil {
		return nil
	}
	return p.laneOf(sess)
}

// laneOf returns sess's second lane, creating it and its writer on first
// use: the first lane frame from the peer, or the server's first answer on it.
func (p *streamTransport) laneOf(sess *peerSession) *kcpRuntime {
	sess.controlMu.Lock()
	defer sess.controlMu.Unlock()
	if sess.closed {
		return nil
	}
	if sess.lane != nil {
		return sess.lane
	}
	peerID := formatPeerID(sess.epoch)
	out := make(chan *packetBuffer, outboundQueueSize)
	acks := make(chan *packetBuffer, ackQueueSize)
	kick := make(chan struct{}, 1)
	hdr := buildEpochHeaderTo(p.bindingToken, p.localEpochValue(), sess.epoch)
	lane, err := startKCPAcks(out, acks, kick, func(payload []byte) {
		if cb := p.laneOnPeerData.Load(); cb != nil {
			(*cb)(peerID, payload)
		}
	}, hdr, p.sendWindow, kcpLaneConvID)
	if err != nil {
		logger.Warnf("vp8channel: second lane for peer %s: %v", peerID, err)
		return nil
	}
	sess.lane = lane
	go p.peerWriterPump(dataLane{
		out: out, acks: acks, kick: kick, name: "lane " + peerID,
		conn:   func() *kcpConn { return lane.conn },
		window: func(segments int) { lane.sess.SetWindowSize(segments, 0) },
		srtt:   lane.sess.GetSRTT,
		path:   &sess.path, side: 1,
	}, sess.done)
	logger.Infof("vp8channel: second lane up for peer %s", peerID)
	return lane
}

// deliverLanes hands each KCP packet of payload to its lane: the second
// lane's conversation to lane, when there is one, the rest to data.
func deliverLanes(data *kcpRuntime, lane func() *kcpRuntime, payload []byte) {
	if data == nil || len(payload) == 0 {
		return
	}
	splitKCPPayload(payload, func(packet []byte) {
		if len(packet) >= 4 && binary.LittleEndian.Uint32(packet) == kcpLaneConvID {
			if rt := lane(); rt != nil {
				rt.deliver(packet)
			}
			return
		}
		data.deliver(packet)
	})
}
