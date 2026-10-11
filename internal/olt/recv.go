package olt

import "time"

// Input takes in one packet from the peer. It is safe for concurrent use,
// it does not retain p, and it never waits for more than the connection's
// lock. Malformed, foreign, duplicate and replayed packets do nothing but
// prompt an acknowledgement.
//
// ai-generated: whole function.
func (c *Conn) Input(p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	pk := &c.in
	if err := pk.decode(p, c.cfg.ConnID); err != nil {
		return
	}
	now := c.now()
	c.lastRecv = now
	if c.rcvdPN.contains(pk.pn) {
		c.ackNow = c.ackNow || pk.elicit
		c.kick()
		return
	}
	c.rcvdPN.add(pk.pn, pk.pn+1)
	c.rcvdPN.trim(rcvdRanges)
	for i := range pk.frames {
		c.apply(now, &pk.frames[i])
		if c.err != nil {
			return
		}
	}
	c.noteReceived(now, pk.pn, pk.elicit)
	c.kick()
}

// apply acts on one received frame.
//
// ai-generated: whole function.
func (c *Conn) apply(now time.Duration, fr *frame) {
	switch fr.typ {
	case frAck:
		c.onAck(now, fr, c.in.ranges[fr.r0:fr.r1])
	case frStream, frStreamFin:
		if s := c.stream(fr.id); s != nil {
			c.onStream(s, fr.a, fr.data, fr.typ == frStreamFin)
		}
	case frReset:
		if s := c.stream(fr.id); s != nil {
			c.onReset(s, fr.a, fr.b)
		}
	case frStop:
		if s := c.stream(fr.id); s != nil {
			c.onStop(s, fr.a)
		}
	case frMaxData:
		if fr.a > c.sndMax {
			c.sndMax = fr.a
			for _, s := range c.streams {
				c.queueSend(s)
			}
		}
	case frMaxStreamData:
		if s := c.stream(fr.id); s != nil && fr.a > s.sndMax {
			s.sndMax = fr.a
			c.queueSend(s)
		}
	case frClose:
		c.logf("olt: closed by peer (code %d)", fr.a)
		c.closeWith(ErrPeerClosed, false)
	}
}

// noteReceived schedules the acknowledgement of packet pn: at once for
// every second ack-eliciting packet or one out of order, otherwise within
// maxAckDelay.
//
// ai-generated: whole function.
func (c *Conn) noteReceived(now time.Duration, pn uint64, elicit bool) {
	inOrder := !c.hasRcvd || pn == c.largestRcvd+1
	if !c.hasRcvd || pn > c.largestRcvd {
		c.largestRcvd, c.largestRcvdAt, c.hasRcvd = pn, now, true
	}
	if !elicit {
		return
	}
	c.ackPending++
	if !inOrder || c.ackPending >= 2 {
		c.ackNow = true
	}
	if c.ackAt == 0 {
		c.ackAt = now + maxAckDelay
	}
}
