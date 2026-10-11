package olt

import (
	"slices"
	"time"
)

const (
	// maxBatch is the most packets one loop pass builds under the lock.
	maxBatch = 32
	// maxAckRanges is the most ranges an ACK reports.
	maxAckRanges = 32
	// rcvdRanges bounds the received packet numbers tracked for
	// acknowledgements and duplicate detection.
	rcvdRanges = 128
	// minStreamRoom is the least room worth a STREAM frame.
	minStreamRoom = streamHeaderMax + 16
	// paceTicks is the pacer's bucket, in carrier ticks at the pacing rate;
	// it never holds more than two carrier bursts.
	paceTicks = 2
	// initPaceTokens is the pacer's first bucket, in packets.
	initPaceTokens = 10
	// smallExtra is how far past the window small streams may go.
	smallExtra = 32 << 10
	// minWait keeps a loop that has nothing due from spinning.
	minWait = 100 * time.Microsecond
)

// Why a loop pass stopped building packets.
const (
	stopNone = iota // the batch filled up: go on at once
	stopNoData
	stopCwnd
	stopPacer
)

// pacer is a token bucket at the pacing rate whose releases are at least a
// carrier tick apart.
type pacer struct {
	tokens float64
	at     time.Duration // last refill
	next   time.Duration // no paced packet before this
}

// builder collects one packet and what it carries.
type builder struct {
	b      []byte
	limit  int
	frames []sentFrame
	retx   bool
}

// room is what the packet still holds.
//
// ai-generated: whole function.
func (w *builder) room() int { return w.limit - len(w.b) }

// refill adds the tokens due since the last refill, up to capacity.
//
// ai-generated: whole function.
func (p *pacer) refill(now time.Duration, rate, capacity float64) {
	if now < p.next {
		return
	}
	p.tokens = min(capacity, p.tokens+rate*(now-p.at).Seconds())
	p.at = now
}

// ready reports whether a paced packet may go now.
//
// ai-generated: whole function.
func (p *pacer) ready(now time.Duration) bool { return now >= p.next && p.tokens > 0 }

// spend takes n bytes; an empty bucket holds releases back for at least a
// tick.
//
// ai-generated: whole function.
func (p *pacer) spend(now time.Duration, n int, rate float64, tick time.Duration) {
	p.tokens -= float64(n)
	if p.tokens > 0 {
		return
	}
	wait := time.Duration(-p.tokens / rate * float64(time.Second))
	p.next = now + max(tick, wait)
}

// run is the connection's loop: timers and every Send.
//
// ai-generated: whole function.
func (c *Conn) run() {
	defer close(c.done)
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		wait, ok := c.service()
		if !ok {
			return
		}
		if wait == 0 {
			continue
		}
		t.Reset(wait)
		select {
		case <-c.wake:
		case <-t.C:
		}
	}
}

// service runs the timers, builds a batch of packets under the lock and
// sends them outside it. It returns how long to wait, or false once the
// connection is closed.
//
// ai-generated: whole function.
func (c *Conn) service() (time.Duration, bool) {
	c.mu.Lock()
	now := c.now()
	if c.err == nil {
		c.onTimers(now)
	}
	if c.err != nil {
		p := c.closePacket()
		c.mu.Unlock()
		if p != nil {
			_ = c.cfg.Send(p)
		}
		return 0, false
	}
	n, stop := c.fill(now)
	wait := c.nextWake(now, stop)
	c.mu.Unlock()
	for _, p := range c.batch[:n] {
		_ = c.cfg.Send(p)
	}
	return wait, true
}

// onTimers fires the idle, loss, probe and keepalive timers that are due.
//
// ai-generated: whole function.
func (c *Conn) onTimers(now time.Duration) {
	if now-c.lastRecv >= c.cfg.IdleTimeout {
		c.logf("olt: nothing from the peer for %v, closing", now-c.lastRecv)
		c.closeWith(ErrIdleTimeout, true)
		return
	}
	if c.lossAt != 0 && now >= c.lossAt {
		c.detectLost(now)
	} else if at := c.ptoAt(); at != 0 && now >= at {
		c.onPTO()
	}
	if now-c.lastElicitSent >= c.cfg.KeepAlive {
		c.needPing = true
		c.lastElicitSent = now
	}
}

// nextWake is how long the loop may sleep after a pass that stopped for
// stop.
//
// ai-generated: whole function.
func (c *Conn) nextWake(now time.Duration, stop int) time.Duration {
	if stop == stopNone {
		return 0
	}
	at := min(c.lastRecv+c.cfg.IdleTimeout, c.lastElicitSent+c.cfg.KeepAlive)
	if c.ackPending > 0 || c.ackNow {
		at = min(at, c.ackAt)
	}
	if c.lossAt != 0 {
		at = min(at, c.lossAt)
	} else if p := c.ptoAt(); p != 0 {
		at = min(at, p)
	}
	if stop == stopPacer {
		at = min(at, c.pace.next)
	}
	return max(at-now, minWait)
}

// closePacket builds the CLOSE packet once, if the peer is to be told.
//
// ai-generated: whole function.
func (c *Conn) closePacket() []byte {
	if !c.sendClose {
		return nil
	}
	c.sendClose = false
	b := appendHeader(make([]byte, 0, headerMax+2), c.cfg.ConnID, c.nextPN)
	c.nextPN++
	return append(b, frClose, 0)
}

// fill builds up to a batch of packets: acknowledgements whenever due, and
// everything else as far as the window and the pacer let it.
//
// ai-generated: whole function.
func (c *Conn) fill(now time.Duration) (int, int) {
	rate := c.cc.pacingRate()
	c.pace.refill(now, rate, c.paceCap(rate))
	n := 0
	for n < len(c.batch) {
		stop := c.sendable(now)
		if stop != stopNone && !c.ackDue(now) {
			return n, c.limited(stop)
		}
		b, ok := c.packet(now, c.batch[n][:0], stop == stopNone, rate)
		if !ok {
			return n, c.limited(stopNoData)
		}
		c.batch[n] = b
		n++
	}
	return n, stopNone
}

// sendable is what keeps an ack-eliciting packet from going now, if
// anything.
//
// ai-generated: whole function.
func (c *Conn) sendable(now time.Duration) int {
	c.smallOnly = false
	switch {
	case c.probes > 0:
		return stopNone
	case !c.hasWork():
		return stopNoData
	}
	cwnd := c.cc.cwnd()
	if c.inflight < cwnd && c.pace.ready(now) {
		return stopNone
	}
	if c.inflight < cwnd+smallExtra && c.smallWaiting(now) {
		c.smallOnly = true
		return stopNone
	}
	if c.inflight >= cwnd {
		return stopCwnd
	}
	return stopPacer
}

// smallWaiting reports whether a stream that sent little lately has new
// data. Such data goes past the window and the pacer, up to smallExtra:
// queued behind the bulk it would wait for the next acknowledgements, and
// the carrier's frame queue after olt keeps no order of its own.
//
// ai-generated: whole function.
func (c *Conn) smallWaiting(now time.Duration) bool {
	for _, s := range c.sendq {
		if s.small(now) && c.hasNew(s) {
			return true
		}
	}
	return false
}

// limited records an application-limited sender, which the bandwidth
// filter must not believe when it measures less.
//
// ai-generated: whole function.
func (c *Conn) limited(stop int) int {
	if stop == stopNoData && c.inflight < c.cc.cwnd() {
		c.appLimited = max(c.delivered+int64(c.inflight), 1)
	}
	return stop
}

// hasWork reports whether anything may want an ack-eliciting packet.
//
// ai-generated: whole function.
func (c *Conn) hasWork() bool {
	return len(c.sendq) > 0 || len(c.retxq) > 0 || len(c.ctrlq) > 0 || len(c.refuse) > 0 ||
		c.needMaxData || c.needPing
}

// ackDue reports whether an acknowledgement must go now.
//
// ai-generated: whole function.
func (c *Conn) ackDue(now time.Duration) bool {
	return c.ackNow || (c.ackPending > 0 && now >= c.ackAt)
}

// packet builds one packet into buf: the frames cc allows, then an ACK
// when one is pending. It reports false when there is nothing to send.
//
// ai-generated: whole function.
func (c *Conn) packet(now time.Duration, buf []byte, cc bool, rate float64) ([]byte, bool) {
	w := &c.build
	w.b, w.frames, w.retx = appendHeader(buf, c.cfg.ConnID, c.nextPN), w.frames[:0], false
	ack := c.ackBuf[:0]
	if c.ackPending > 0 || c.ackNow {
		ack = c.encodeAck(ack, now)
		c.ackBuf = ack
		if len(ack) == 0 {
			c.ackPending, c.ackNow, c.ackAt = 0, false, 0
		}
	}
	w.limit = c.cfg.MaxPacket - len(ack)
	if cc {
		if !c.smallOnly {
			c.appendRetx(w)
		}
		c.appendCtrl(w)
		c.appendStreams(now, w)
		if len(w.frames) == 0 && (c.probes > 0 || c.needPing) {
			w.b = append(w.b, frPing)
			w.frames = append(w.frames, sentFrame{kind: fPing})
		}
	}
	elicit := len(w.frames) > 0
	if !elicit && !c.ackDue(now) {
		return buf, false
	}
	if len(ack) > 0 {
		w.b = append(w.b, ack...)
		c.ackPending, c.ackNow, c.ackAt = 0, false, 0
	}
	c.commit(now, w, elicit, rate)
	return w.b, true
}

// commit records the packet just built.
//
// ai-generated: whole function.
func (c *Conn) commit(now time.Duration, w *builder, elicit bool, rate float64) {
	p := c.sentq.push(c.nextPN)
	p.frames = append(p.frames[:0], w.frames...)
	p.pn, p.sentAt, p.size = c.nextPN, now, len(w.b)
	p.done, p.inFlight, p.probed, p.lost, p.spurious = !elicit, elicit, false, false, false
	c.nextPN++
	c.nSent++
	if !elicit {
		c.sentq.popDone(now, c.rtt.smoothed())
		return
	}
	c.needPing = false
	if c.inflight == 0 {
		c.firstSentAt, c.deliveredAt = now, now
	}
	p.delivered, p.deliveredAt, p.firstSentAt = c.delivered, c.deliveredAt, c.firstSentAt
	p.appLimited = c.appLimited != 0
	c.inflight += p.size
	c.lastElicitSent = now
	if c.probes > 0 {
		c.probes--
	} else if !c.smallOnly {
		c.pace.spend(now, p.size, rate, c.cfg.Tick)
	}
	if w.retx {
		c.nRetx++
	}
}

// encodeAck appends an ACK of the newest received ranges.
//
// ai-generated: whole function.
func (c *Conn) encodeAck(b []byte, now time.Duration) []byte {
	sp := c.rcvdPN.spans
	if len(sp) == 0 {
		return b
	}
	last := sp[len(sp)-1]
	largest := last.hi - 1
	delay := max(now-c.largestRcvdAt, 0) / time.Microsecond
	count := min(len(sp)-1, c.ackRanges)
	b = append(b, frAck)
	b = appendVarint(b, largest)
	b = appendVarint(b, uint64(delay))
	b = appendVarint(b, u64(count))
	b = appendVarint(b, largest-last.lo)
	lo := last.lo
	for i := len(sp) - 2; i >= len(sp)-1-count; i-- {
		r := sp[i]
		b = appendVarint(b, lo-r.hi-1)
		b = appendVarint(b, r.hi-1-r.lo)
		lo = r.lo
	}
	return b
}

// appendRetx adds lost stream data, oldest stream first.
//
// ai-generated: whole function.
func (c *Conn) appendRetx(w *builder) {
	for len(c.retxq) > 0 {
		s := c.retxq[0]
		if !c.retxStream(w, s) {
			return
		}
		s.inRetx = false
		c.retxq = slices.Delete(c.retxq, 0, 1)
	}
}

// retxStream adds what s has to send again; false means the packet is full.
//
// ai-generated: whole function.
func (c *Conn) retxStream(w *builder, s *Stream) bool {
	if s.gone || s.sndReset {
		s.lost, s.finLost = rangeSet{}, false
		return true
	}
	for len(s.lost.spans) > 0 {
		sp := s.lost.spans[0]
		lo, hi := s.acked.gap(sp.lo, sp.hi)
		if lo >= hi {
			s.lost.cutFront(sp.hi)
			continue
		}
		room := w.room() - streamOverhead(s.id, lo)
		if room <= 0 {
			return false
		}
		hi = min(hi, lo+u64(room))
		fin := s.finLost && hi == s.sndEnd()
		c.putStream(w, s, lo, hi, fin, true)
		s.finLost = s.finLost && !fin
		s.lost.cutFront(hi)
	}
	if s.finLost {
		end := s.sndEnd()
		if w.room() < streamOverhead(s.id, end) {
			return false
		}
		c.putStream(w, s, end, end, true, true)
		s.finLost = false
	}
	return true
}

// putStream adds a STREAM frame of s's bytes [lo, hi); retx marks resent
// data.
//
// ai-generated: whole function.
func (c *Conn) putStream(w *builder, s *Stream, lo, hi uint64, fin, retx bool) {
	base := s.acked.floor
	w.b = appendStreamFrame(w.b, s.id, lo, s.snd.Bytes()[lo-base:hi-base], fin)
	w.frames = append(w.frames, sentFrame{kind: fStream, fin: fin, id: s.id, off: lo, end: hi})
	w.retx = w.retx || retx
}

// appendCtrl adds the connection's control frames, then the streams'.
//
// ai-generated: whole function.
func (c *Conn) appendCtrl(w *builder) {
	if c.needMaxData {
		if w.room() < 1+varintLen(c.rcvMax) {
			return
		}
		w.b = appendVarint(append(w.b, frMaxData), c.rcvMax)
		w.frames = append(w.frames, sentFrame{kind: fMaxData})
		c.needMaxData = false
	}
	for len(c.refuse) > 0 {
		id := c.refuse[0]
		if w.room() < idFrameLen(id, codeRefused)+idFrameLen(id, codeRefused, 0) {
			return
		}
		w.b = appendIDFrame(w.b, frStop, id, codeRefused)
		w.b = appendIDFrame(w.b, frReset, id, codeRefused, 0)
		w.frames = append(w.frames, sentFrame{kind: fRefuse, id: id})
		c.refuse = slices.Delete(c.refuse, 0, 1)
	}
	for len(c.ctrlq) > 0 {
		s := c.ctrlq[0]
		if !c.ctrlStream(w, s) {
			return
		}
		s.inCtrl = false
		c.ctrlq = slices.Delete(c.ctrlq, 0, 1)
	}
}

// ctrlStream adds s's control frames; false means the packet is full.
//
// ai-generated: whole function.
func (c *Conn) ctrlStream(w *builder, s *Stream) bool {
	if s.gone {
		return true
	}
	if s.needMaxSD && !s.finRcvd && !s.rcvReset && !s.stopped {
		if w.room() < idFrameLen(s.id, s.rcvMax) {
			return false
		}
		w.b = appendIDFrame(w.b, frMaxStreamData, s.id, s.rcvMax)
		w.frames = append(w.frames, sentFrame{kind: fMaxStreamData, id: s.id})
	}
	s.needMaxSD = false
	if s.needStop {
		if w.room() < idFrameLen(s.id, codeStop) {
			return false
		}
		w.b = appendIDFrame(w.b, frStop, s.id, codeStop)
		w.frames = append(w.frames, sentFrame{kind: fStop, id: s.id})
		s.needStop = false
	}
	if s.needReset {
		if w.room() < idFrameLen(s.id, s.resetCode, s.sndNext) {
			return false
		}
		w.b = appendIDFrame(w.b, frReset, s.id, s.resetCode, s.sndNext)
		w.frames = append(w.frames, sentFrame{kind: fReset, id: s.id})
		s.needReset = false
	}
	return true
}

// appendStreams adds new stream data: streams that sent little lately
// first, round robin within each class.
//
// ai-generated: whole function.
func (c *Conn) appendStreams(now time.Duration, w *builder) {
	for w.room() >= minStreamRoom {
		s := c.pickStream(now)
		if s == nil {
			return
		}
		c.sendNew(w, s)
	}
}

// pickStream returns the next stream with new data to send, dropping the
// ones without any from the queue.
//
// ai-generated: whole function.
func (c *Conn) pickStream(now time.Duration) *Stream {
	for len(c.sendq) > 0 {
		i, small := c.nextStream(now)
		if c.smallOnly && !small {
			return nil
		}
		s := c.sendq[i]
		if c.hasNew(s) {
			c.rr = i + 1
			return s
		}
		s.inSend = false
		c.sendq = slices.Delete(c.sendq, i, i+1)
	}
	return nil
}

// nextStream returns the index of the next small stream in round-robin
// order, or of the next stream when none is small.
//
// ai-generated: whole function.
func (c *Conn) nextStream(now time.Duration) (int, bool) {
	n := len(c.sendq)
	start := c.rr % n
	for k := range n {
		if j := (start + k) % n; c.sendq[j].small(now) {
			return j, true
		}
	}
	return start, false
}

// hasNew reports whether s has new data the windows let out, or a FIN.
//
// ai-generated: whole function.
func (c *Conn) hasNew(s *Stream) bool {
	if s.gone || s.sndReset {
		return false
	}
	if s.sndNext < s.sndEnd() {
		return s.sndNext < s.sndMax && c.sent < c.sndMax
	}
	return s.finQueued && !s.finSent
}

// sendNew adds as much of s's new data as fits the packet and the windows.
//
// ai-generated: whole function.
func (c *Conn) sendNew(w *builder, s *Stream) {
	room := w.room() - streamOverhead(s.id, s.sndNext)
	end := s.sndEnd()
	n := min(end-s.sndNext, s.sndMax-s.sndNext, c.sndMax-c.sent, u64(room))
	fin := s.finQueued && !s.finSent && s.sndNext+n == end
	c.putStream(w, s, s.sndNext, s.sndNext+n, fin, false)
	s.sndNext += n
	c.sent += n
	s.recent += n
	s.finSent = s.finSent || fin
}

// paceCap is the pacer's bucket at rate: a few ticks' worth, but no more
// than two carrier bursts, and room for two packets at least.
//
// ai-generated: whole function.
func (c *Conn) paceCap(rate float64) float64 {
	ticks := rate * paceTicks * c.cfg.Tick.Seconds()
	return max(min(ticks, float64(2*c.cfg.BurstBytes)), float64(2*c.cfg.MaxPacket))
}
