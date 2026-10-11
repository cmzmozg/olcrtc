package olt

import "time"

const (
	// maxAckDelay is how long a receiver may hold an acknowledgement.
	maxAckDelay = 5 * time.Millisecond
	// ackDelayCap bounds the ack delay a peer may claim.
	ackDelayCap = 4 * maxAckDelay
	// initialRTT stands in for the round trip until one is measured.
	initialRTT = 100 * time.Millisecond
	// pktThreshold is how many later packets acknowledged make one lost at
	// first; maxReorder caps what spurious losses raise it to.
	pktThreshold = 3
	maxReorder   = 64
	// initLossEighths is the time threshold at first, in eighths of an RTT;
	// maxLossEighths caps what spurious losses raise it to.
	initLossEighths = 9
	maxLossEighths  = 16
	// maxPTOBackoff caps the PTO doubling low: after an outage the tunnel
	// should find the path again within seconds.
	maxPTOBackoff = 4
	// ptoProbes is how many packets a PTO sends past the window.
	ptoProbes = 2
)

// rttStats is the RFC 9002 round trip estimator.
type rttStats struct {
	srtt, rttvar, latest, minRTT time.Duration
	has                          bool
}

// rateSample is one acknowledgement's delivery-rate sample, as in the BBR
// draft, with the bytes it acknowledged.
type rateSample struct {
	has            bool
	priorDelivered int64
	priorTime      time.Duration
	sendElapsed    time.Duration
	appLimited     bool
	acked          int
	valid          bool
	rate           float64 // bytes per second, when valid
}

// update takes in an RTT sample and the ack delay the peer reported.
//
// ai-generated: whole function.
func (r *rttStats) update(sample, ackDelay time.Duration) {
	r.latest = sample
	if !r.has {
		r.srtt, r.rttvar, r.minRTT, r.has = sample, sample/2, sample, true
		return
	}
	r.minRTT = min(r.minRTT, sample)
	adj := sample
	if d := min(ackDelay, ackDelayCap); sample-d >= r.minRTT {
		adj = sample - d
	}
	diff := r.srtt - adj
	if diff < 0 {
		diff = -diff
	}
	r.rttvar = (3*r.rttvar + diff) / 4
	r.srtt = (7*r.srtt + adj) / 8
}

// smoothed is the smoothed RTT, or initialRTT before any sample.
//
// ai-generated: whole function.
func (r *rttStats) smoothed() time.Duration {
	if !r.has {
		return initialRTT
	}
	return r.srtt
}

// pto is the probe timeout before backoff.
//
// ai-generated: whole function.
func (r *rttStats) pto(gran time.Duration) time.Duration {
	if !r.has {
		return 3*initialRTT + maxAckDelay
	}
	return r.srtt + max(4*r.rttvar, gran) + maxAckDelay
}

// onAck takes in an ACK frame: the packets it newly acknowledges, an RTT
// sample, loss detection and the congestion controller's update.
//
// ai-generated: whole function.
func (c *Conn) onAck(now time.Duration, fr *frame, ranges []span) {
	largest := fr.a
	if largest >= c.nextPN {
		return
	}
	var rs rateSample
	var rttSample time.Duration
	base, end := c.sentq.base, c.sentq.end()
	for _, r := range ranges {
		for pn := max(r.lo, base); pn < min(r.hi, end); pn++ {
			p := c.sentq.at(pn)
			if p.done {
				if p.lost && !p.spurious {
					c.spurious(p, largest)
				}
				continue
			}
			if pn == largest {
				rttSample = now - p.sentAt
			}
			c.ackPacket(now, p, &rs)
		}
	}
	if !c.hasAcked || largest > c.largestAcked {
		c.largestAcked, c.hasAcked = largest, true
	}
	if rttSample > 0 {
		c.rtt.update(rttSample, ackDelay(fr.b))
		c.cc.onRTT(now, rttSample)
	}
	c.detectLost(now)
	if rs.acked == 0 {
		return
	}
	c.ptoCount = 0
	c.finishSample(&rs)
	c.cc.onAck(now, &rs, c.inflight, c.delivered)
	c.sentq.popDone(now, c.rtt.smoothed())
}

// ackPacket handles one newly acknowledged packet.
//
// ai-generated: whole function.
func (c *Conn) ackPacket(now time.Duration, p *sentPacket, rs *rateSample) {
	p.done = true
	c.inflight -= p.size
	c.delivered += int64(p.size)
	c.deliveredAt = now
	if !rs.has || p.delivered > rs.priorDelivered {
		rs.has = true
		rs.priorDelivered, rs.priorTime = p.delivered, p.deliveredAt
		rs.sendElapsed, rs.appLimited = p.sentAt-p.firstSentAt, p.appLimited
		c.firstSentAt = p.sentAt
	}
	rs.acked += p.size
	for _, f := range p.frames {
		c.frameAcked(f)
	}
}

// finishSample turns the acknowledged packets into a delivery rate.
//
// ai-generated: whole function.
func (c *Conn) finishSample(rs *rateSample) {
	if c.appLimited != 0 && c.delivered > c.appLimited {
		c.appLimited = 0
	}
	if !rs.has {
		return
	}
	interval := max(rs.sendElapsed, c.deliveredAt-rs.priorTime)
	if interval <= 0 || interval < c.rtt.minRTT {
		return
	}
	rs.rate = float64(c.delivered-rs.priorDelivered) / interval.Seconds()
	rs.valid = true
}

// detectLost declares lost the packets reorder numbers or lossEighths/8 of
// an RTT behind the largest acknowledged, and arms the loss timer for the
// next one that may become lost.
//
// ai-generated: whole function.
func (c *Conn) detectLost(now time.Duration) {
	c.lossAt = 0
	if !c.hasAcked {
		return
	}
	delay := max(max(c.rtt.srtt, c.rtt.latest)*time.Duration(c.lossEighths)/8, c.gran)
	for pn := c.sentq.base; pn < c.largestAcked && pn < c.sentq.end(); pn++ {
		p := c.sentq.at(pn)
		if p.done {
			continue
		}
		if c.largestAcked-pn >= c.reorder || now-p.sentAt >= delay {
			c.lose(now, p)
			continue
		}
		if c.lossAt == 0 {
			c.lossAt = p.sentAt + delay
		}
	}
	c.sentq.popDone(now, c.rtt.smoothed())
}

// lose declares p lost and queues its frames again.
//
// ai-generated: whole function.
func (c *Conn) lose(now time.Duration, p *sentPacket) {
	p.done, p.lost, p.lostAt, p.lostGap = true, true, now, c.largestAcked-p.pn
	c.inflight -= p.size
	c.nLost++
	c.cc.onLost(p.size)
	for _, f := range p.frames {
		c.frameLost(f)
	}
}

// spurious adapts loss detection to a packet acknowledged after it was
// declared lost, as the path reorders: the packet threshold grows past the
// reordering seen, up to maxReorder, and a loss the timer declared stretches
// the time threshold, up to two RTTs. Repairs below olt reorder: a parity
// rebuilds a datagram after the rest of its group, and a retransmitted RTP
// packet completes its frame late.
//
// ai-generated: whole function.
func (c *Conn) spurious(p *sentPacket, largest uint64) {
	p.spurious = true
	c.nSpurious++
	if p.lostGap >= c.reorder {
		c.reorder = min(max(c.reorder, largest-p.pn+1), maxReorder)
		return
	}
	c.lossEighths = min(c.lossEighths+2, maxLossEighths)
}

// ptoAt is when the probe timer fires, zero when nothing is in flight.
//
// ai-generated: whole function.
func (c *Conn) ptoAt() time.Duration {
	if c.inflight == 0 {
		return 0
	}
	return c.lastElicitSent + c.rtt.pto(c.gran)<<c.ptoCount
}

// onPTO queues the frames of the oldest packet in flight again, without
// declaring it lost, and lets probes past the window.
//
// ai-generated: whole function.
func (c *Conn) onPTO() {
	c.ptoCount = min(c.ptoCount+1, maxPTOBackoff)
	c.probes = ptoProbes
	for pn := c.sentq.base; pn < c.sentq.end(); pn++ {
		p := c.sentq.at(pn)
		if p.done || p.probed {
			continue
		}
		p.probed = true
		for _, f := range p.frames {
			c.frameLost(f)
		}
		return
	}
}

// frameAcked acts on an acknowledged frame.
//
// ai-generated: whole function.
func (c *Conn) frameAcked(f sentFrame) {
	s := c.streams[f.id]
	if s == nil {
		return
	}
	switch f.kind {
	case fStream:
		c.streamAcked(s, f)
	case fReset:
		s.resetAcked = true
		c.maybeRetire(s)
	}
}

// streamAcked frees the acknowledged stream data.
//
// ai-generated: whole function.
func (c *Conn) streamAcked(s *Stream, f sentFrame) {
	if s.sndReset {
		return
	}
	base := s.acked.floor
	s.acked.add(f.off, f.end)
	s.acked.advance()
	if f.fin {
		s.finAcked, s.finLost = true, false
	}
	if moved := s.acked.floor - base; moved > 0 {
		s.snd.Drop(moved)
		notify(s.wrCh)
	}
	c.maybeRetire(s)
}

// frameLost queues a lost frame's content again, when still wanted.
//
// ai-generated: whole function.
func (c *Conn) frameLost(f sentFrame) {
	switch f.kind {
	case fMaxData:
		c.needMaxData = true
	case fRefuse:
		c.refuse = append(c.refuse, f.id)
	case fPing:
	default:
		if s := c.streams[f.id]; s != nil {
			c.streamFrameLost(s, f)
		}
	}
}

// streamFrameLost queues a lost frame of s again, when still wanted.
//
// ai-generated: whole function.
func (c *Conn) streamFrameLost(s *Stream, f sentFrame) {
	switch f.kind {
	case fStream:
		if !s.sndReset {
			s.lost.add(f.off, f.end)
			s.finLost = s.finLost || (f.fin && !s.finAcked)
			c.queueRetx(s)
		}
		return
	case fMaxStreamData:
		s.needMaxSD = s.needMaxSD || (!s.finRcvd && !s.rcvReset && !s.stopped)
	case fStop:
		s.needStop = s.needStop || (!s.finRcvd && !s.rcvReset)
	case fReset:
		s.needReset = s.needReset || !s.resetAcked
	}
	c.queueCtrl(s)
}

// ackDelay converts an ACK's delay field, capped at ackDelayCap.
//
// ai-generated: whole function.
func ackDelay(us uint64) time.Duration {
	if us >= uint64(ackDelayCap/time.Microsecond) {
		return ackDelayCap
	}
	return time.Duration(us) * time.Microsecond
}
