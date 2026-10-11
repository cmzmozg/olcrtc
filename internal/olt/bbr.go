package olt

import "time"

// Congestion control: a rate-based controller after BBR. The bottleneck
// bandwidth is the windowed maximum of delivery-rate samples over bwRounds
// round trips, the minimum RTT the minimum over minRTTWin, and the
// connection is paced at a gain over the bandwidth: Startup at 2 until the
// rate stops growing by a quarter for three rounds, Drain at 1/2 until the
// queue it built is gone, then ProbeBW cycling through the probe gains, one
// phase per minimum RTT. When the minimum RTT has not been seen again for
// minRTTWin, ProbeRTT holds the window at half the BDP for probeRTTTime and
// a round trip, so the queues drain and the minimum stays honest: a
// standing queue would otherwise inflate it, and the BDP and the window
// with it.
//
// The window is the BDP (the probing gain while probing) plus the ack
// aggregation: the bytes acknowledged beyond what the bandwidth explains
// within one aggregation epoch, the windowed maximum over aggRounds. The
// carrier sends a frame per tick and the peer acknowledges in its own
// frames, so acknowledgements come in bursts that the minimum RTT does not
// show; measuring the bursts sizes the allowance to the rate, where a fixed
// one is a standing queue on a slow path. The bandwidth runs a little high
// for the same reason, and pacing at it fills whatever the window allows:
// so the smallest RTT of every round is compared with the minimum, and a
// queue above queueTarget trims the allowance (trimQueue) and starts an
// extra drain once per cycle. Everything olt releases queues in the
// carrier's frame queue and at the bottleneck in order, small streams
// included, so a queue there costs every interactive stream its length.
//
// Loss lowers nothing unless a round trip loses more than lossThresh of
// what it carried twice in a row: a lost video frame takes dozens of
// packets at once on this path, and that is not congestion. A round the
// application left short says nothing about the path at all.
const (
	bbrStartup = iota
	bbrDrain
	bbrProbeBW
	bbrProbeRTT
)

const (
	startupGain = 2.0
	drainGain   = 0.5
	// fullBwGrowth and fullBwRounds detect the filled pipe.
	fullBwGrowth = 1.25
	fullBwRounds = 3
	// bwRounds is the bandwidth filter window, in round trips.
	bwRounds = 10
	// minRTTWin is how long a minimum RTT holds without being seen again.
	minRTTWin = 10 * time.Second
	// probeRTTTime is how long ProbeRTT holds the window at probeRTTGain x
	// BDP once in flight has come down to it.
	probeRTTTime = 200 * time.Millisecond
	probeRTTGain = 0.5
	// aggRounds is the ack aggregation filter window, in round trips;
	// aggMaxTime caps the allowance at that long at the bandwidth.
	aggRounds  = 10
	aggMaxTime = 100 * time.Millisecond
	// lossThresh is the share of a round's bytes lost that counts as heavy
	// loss; lossRounds heavy rounds in a row cut the bandwidth by lossCut.
	lossThresh = 0.2
	lossRounds = 2
	lossCut    = 0.85
	// minLossPackets keeps a few lost packets from making a round heavy.
	minLossPackets = 3
	// initCwndPackets is the window before the first bandwidth sample, on
	// top of a burst; minCwndPackets is the smallest window.
	initCwndPackets = 10
	minCwndPackets  = 4
	// minRate is the lowest pacing rate, in bytes per second.
	minRate = 16 << 10
	// probePhases is the length of the ProbeBW gain cycle, drainPhaseMax
	// the most minimum RTTs its draining phase lasts.
	probePhases   = 8
	drainPhaseMax = 4
	// probeStartPhase is where ProbeBW enters its cycle: cruising.
	probeStartPhase = 2
)

// bwSample is one entry of a windowed filter.
type bwSample struct {
	t uint64
	v float64
}

// maxFilter is a windowed maximum over round counts, keeping the best,
// second best and third best samples of the window (Kathleen Nichols'
// algorithm, as in Linux).
type maxFilter struct {
	s [3]bwSample
}

// bbr is the congestion controller state.
type bbr struct {
	mss, burst int
	tick       time.Duration
	logf       func(format string, args ...any)

	state    int
	bw       maxFilter
	minRTT   time.Duration
	minRTTAt time.Duration // when minRTT was last seen
	// rttRound is the smallest RTT of the current round, rttLast of the
	// previous one; redrain allows one queue-driven drain per cycle.
	rttRound, rttLast time.Duration
	redrain           bool
	// trim is what a standing queue took off the aggregation allowance.
	trim float64

	round     uint64
	nextRound int64
	fullBw    float64
	fullCount int
	filled    bool
	phase     int
	phaseAt   time.Duration

	// probeRTTEnd is when ProbeRTT may end, zero until in flight has come
	// down to its window; probeRTTRound is the round it came down in.
	probeRTTEnd   time.Duration
	probeRTTRound uint64

	// aggAt and aggAcked are the start and the bytes acknowledged of the
	// ack aggregation epoch; extra is the filter of the excess.
	aggAt    time.Duration
	aggAcked float64
	extra    maxFilter

	roundAcked, roundLost int
	heavyRounds           int
	rate                  float64 // pacing rate, bytes per second
}

// probeGain is the pacing gain of a ProbeBW phase.
//
// ai-generated: whole function.
func probeGain(phase int) float64 {
	switch phase {
	case 0:
		return 1.25
	case 1:
		return 0.75
	default:
		return 1
	}
}

// get returns the maximum.
//
// ai-generated: whole function.
func (f *maxFilter) get() float64 { return f.s[0].v }

// reset makes v at t the only sample.
//
// ai-generated: whole function.
func (f *maxFilter) reset(t uint64, v float64) {
	f.s = [3]bwSample{{t, v}, {t, v}, {t, v}}
}

// update takes in v at t, with a window of win rounds.
//
// ai-generated: whole function.
func (f *maxFilter) update(t uint64, v float64, win uint64) {
	if v >= f.s[0].v || t-f.s[2].t > win {
		f.reset(t, v)
		return
	}
	nv := bwSample{t, v}
	if v >= f.s[1].v {
		f.s[1], f.s[2] = nv, nv
	} else if v >= f.s[2].v {
		f.s[2] = nv
	}
	dt := t - f.s[0].t
	switch {
	case dt > win:
		f.s[0], f.s[1], f.s[2] = f.s[1], f.s[2], nv
		if t-f.s[0].t > win {
			f.s[0], f.s[1], f.s[2] = f.s[1], f.s[2], nv
		}
	case f.s[1].t == f.s[0].t && dt > win/4:
		f.s[1], f.s[2] = nv, nv
	case f.s[2].t == f.s[1].t && dt > win/2:
		f.s[2] = nv
	}
}

// init sets the controller up for packets of mss and carrier bursts every
// tick.
//
// ai-generated: whole function.
func (b *bbr) init(mss, burst int, tick time.Duration, logf func(string, ...any)) {
	b.mss, b.burst, b.tick, b.logf = mss, burst, tick, logf
	b.state = bbrStartup
	b.rate = startupGain * float64(b.initCwnd()) / initialRTT.Seconds()
}

// onRTT takes in an RTT sample. A minimum not seen again for minRTTWin
// gives way to the sample and sends the controller to ProbeRTT, which finds
// the real one.
//
// ai-generated: whole function.
func (b *bbr) onRTT(now, rtt time.Duration) {
	expired := b.minRTT > 0 && now-b.minRTTAt > minRTTWin
	if b.minRTT == 0 || rtt <= b.minRTT || expired {
		b.minRTT, b.minRTTAt = rtt, now
	}
	if b.rttRound == 0 || rtt < b.rttRound {
		b.rttRound = rtt
	}
	if expired && b.state != bbrProbeRTT {
		b.state, b.probeRTTEnd = bbrProbeRTT, 0
	}
}

// onLost counts lost bytes against the round.
//
// ai-generated: whole function.
func (b *bbr) onLost(n int) { b.roundLost += n }

// onAck takes in an acknowledgement's rate sample.
//
// ai-generated: whole function.
func (b *bbr) onAck(now time.Duration, rs *rateSample, inflight int, delivered int64) {
	b.roundAcked += rs.acked
	roundStart := false
	if rs.has && rs.priorDelivered >= b.nextRound {
		b.nextRound = delivered
		b.round++
		roundStart = true
	}
	if rs.valid && (!rs.appLimited || rs.rate >= b.bw.get()) {
		b.bw.update(b.round, rs.rate, bwRounds)
	}
	b.aggregate(now, rs.acked)
	if roundStart {
		b.rttLast, b.rttRound = b.rttRound, 0
		b.trimQueue()
		b.endRound(rs.appLimited)
	}
	b.advance(now, inflight)
	b.setRate()
}

// aggregate tracks the ack aggregation: within an epoch, the bytes
// acknowledged beyond what the bandwidth delivers in that time. An epoch
// ends once acknowledgements fall behind the bandwidth.
//
// ai-generated: whole function.
func (b *bbr) aggregate(now time.Duration, acked int) {
	bw := b.bw.get()
	if acked <= 0 || bw <= 0 {
		return
	}
	expected := bw * (now - b.aggAt).Seconds()
	if b.aggAcked <= expected {
		b.aggAt, b.aggAcked, expected = now, 0, 0
	}
	b.aggAcked += float64(acked)
	b.extra.update(b.round, b.aggAcked-expected, aggRounds)
}

// aggregation is the window's allowance for ack aggregation: the measured
// excess, capped at aggMaxTime at the bandwidth and at two carrier bursts.
//
// ai-generated: whole function.
func (b *bbr) aggregation() float64 {
	return min(b.extra.get(), b.bw.get()*aggMaxTime.Seconds(), float64(2*b.burst))
}

// endRound checks the round that ended for heavy loss and for a filled
// pipe. A round the application left short says nothing of the path: the
// packets a new connection sends before the peer listens are lost too.
//
// ai-generated: whole function.
func (b *bbr) endRound(appLimited bool) {
	lost, acked := b.roundLost, b.roundAcked
	b.roundLost, b.roundAcked = 0, 0
	if appLimited {
		b.heavyRounds = 0
		return
	}
	if lost >= minLossPackets*b.mss && float64(lost) > lossThresh*float64(lost+acked) {
		b.heavyRounds++
		b.onHeavyLoss(lost, acked)
		return
	}
	b.heavyRounds = 0
	if b.filled {
		return
	}
	if bw := b.bw.get(); bw >= b.fullBw*fullBwGrowth {
		b.fullBw, b.fullCount = bw, 0
		return
	}
	b.fullCount++
	b.filled = b.fullCount >= fullBwRounds
}

// onHeavyLoss ends Startup at the first heavy round, and cuts the
// bandwidth after lossRounds of them in a row.
//
// ai-generated: whole function.
func (b *bbr) onHeavyLoss(lost, acked int) {
	if b.state == bbrStartup {
		b.filled = true
		return
	}
	if b.heavyRounds < lossRounds {
		return
	}
	bw := b.bw.get() * lossCut
	b.bw.reset(b.round, bw)
	if b.logf != nil {
		b.logf("olt: heavy loss (%d of %d bytes), bandwidth cut to %.0f B/s", lost, lost+acked, bw)
	}
}

// advance moves between the states. Drain ends, and so does the draining
// ProbeBW phase, once in flight is down to the BDP and the aggregation or
// the round trips show no queue.
//
// ai-generated: whole function.
func (b *bbr) advance(now time.Duration, inflight int) {
	drained := float64(inflight) <= b.bdp()+b.aggregation() || b.queueGone()
	switch b.state {
	case bbrStartup:
		if b.filled {
			b.state = bbrDrain
		}
	case bbrDrain:
		if drained {
			b.enterProbeBW(now)
		}
	case bbrProbeRTT:
		b.probeRTT(now, inflight)
	default:
		b.cycle(now, drained)
	}
}

// cycle moves through the ProbeBW gain cycle. A phase lasts a minimum RTT,
// two carrier ticks at least, but the draining one goes on, up to
// drainPhaseMax of them, until drained: the bandwidth filter keeps the
// largest sample, acknowledgements in bursts make samples run high, and a
// queue that only the probe's own excess drains would creep up to the
// window. For the same reason a cruising phase that finds the last round
// queued drains again, once per cycle, so a delay that is not a queue can
// never hold the rate down.
//
// ai-generated: whole function.
func (b *bbr) cycle(now time.Duration, drained bool) {
	rtt := max(b.minRTT, 2*b.tick)
	elapsed := now - b.phaseAt
	switch {
	case b.phase == 0:
		b.redrain = true
	case b.phase == 1:
		if !drained && elapsed < drainPhaseMax*rtt {
			return
		}
		if elapsed >= rtt {
			b.rttLast = b.rttRound
			b.phase, b.phaseAt = probeStartPhase, now
		}
		return
	case b.redrain && b.queued():
		b.redrain = false
		b.phase, b.phaseAt = 1, now
		return
	}
	if elapsed >= rtt {
		b.phase = (b.phase + 1) % probePhases
		b.phaseAt = now
	}
}

// queueTarget is the standing queue the round trips may show: a carrier
// tick, 10 ms at least.
//
// ai-generated: whole function.
func (b *bbr) queueTarget() time.Duration { return max(b.tick, 10*time.Millisecond) }

// queued reports whether the last round's smallest RTT stood a queue above
// the minimum.
//
// ai-generated: whole function.
func (b *bbr) queued() bool {
	return b.rttLast > 0 && b.rttLast-b.minRTT > b.queueTarget()
}

// queueGone reports whether the current round has seen an RTT without a
// queue.
//
// ai-generated: whole function.
func (b *bbr) queueGone() bool {
	return b.rttRound > 0 && b.rttRound-b.minRTT <= b.queueTarget()
}

// probeRTT ends ProbeRTT probeRTTTime and a round trip after in flight came
// down to its window; the minimum RTT seen meanwhile is the new one.
//
// ai-generated: whole function.
func (b *bbr) probeRTT(now time.Duration, inflight int) {
	if b.probeRTTEnd == 0 {
		if inflight <= b.cwnd() {
			b.probeRTTEnd, b.probeRTTRound = now+probeRTTTime, b.round
		}
		return
	}
	if now < b.probeRTTEnd || b.round <= b.probeRTTRound {
		return
	}
	b.minRTTAt = now
	if b.filled {
		b.enterProbeBW(now)
		return
	}
	b.state = bbrStartup
}

// enterProbeBW starts the ProbeBW cycle at its cruising phase.
//
// ai-generated: whole function.
func (b *bbr) enterProbeBW(now time.Duration) {
	b.state, b.phase, b.phaseAt = bbrProbeBW, probeStartPhase, now
}

// bdp is the bandwidth-delay product in bytes, zero before samples.
//
// ai-generated: whole function.
func (b *bbr) bdp() float64 {
	return b.bw.get() * b.minRTT.Seconds()
}

// gain is the pacing gain of the current state.
//
// ai-generated: whole function.
func (b *bbr) gain() float64 {
	switch b.state {
	case bbrStartup:
		return startupGain
	case bbrDrain:
		return drainGain
	case bbrProbeRTT:
		return 1
	default:
		return probeGain(b.phase)
	}
}

// setRate sets the pacing rate from the gain and the bandwidth; until the
// pipe is filled it only grows, so an application-limited start does not
// slow a later transfer.
//
// ai-generated: whole function.
func (b *bbr) setRate() {
	if r := b.gain() * b.bw.get(); r > 0 && (b.filled || r > b.rate) {
		b.rate = r
	}
}

// pacingRate is the rate packets are released at, in bytes per second.
//
// ai-generated: whole function.
func (b *bbr) pacingRate() float64 { return max(b.rate, minRate) }

// cwnd is the congestion window in bytes.
//
// ai-generated: whole function.
func (b *bbr) cwnd() int {
	bdp := b.bdp()
	var w int
	switch {
	case b.state == bbrProbeRTT:
		w = int(probeRTTGain * bdp)
	case !b.filled:
		w = max(int(startupGain*bdp), b.initCwnd())
	default:
		w = int(b.cwndGain()*bdp + max(b.aggregation()-b.trim, 0))
	}
	return max(w, minCwndPackets*b.mss)
}

// initCwnd is the window before the pipe is full.
//
// ai-generated: whole function.
func (b *bbr) initCwnd() int { return initCwndPackets*b.mss + b.burst }

// stateName names the state for Stats.
//
// ai-generated: whole function.
func (b *bbr) stateName() string {
	switch b.state {
	case bbrStartup:
		return "startup"
	case bbrDrain:
		return "drain"
	case bbrProbeRTT:
		return "probe_rtt"
	default:
		return "probe_bw"
	}
}

// cwndGain is the window's gain over the BDP once the pipe is full: the
// probing phase's pacing gain while it probes, one otherwise. With the ack
// aggregation on top, one BDP keeps the path full, and the window caps the
// queue a bandwidth estimate a little high would build.
//
// ai-generated: whole function.
func (b *bbr) cwndGain() float64 {
	if b.state == bbrProbeBW {
		return max(probeGain(b.phase), 1)
	}
	return 1
}

// trimQueue sizes the trim at the end of a round: a round whose smallest
// RTT stood more than the queue target above the minimum takes half the
// excess, at the bandwidth, off the aggregation allowance, and a round
// without one gives back half the target's worth. The allowance keeps the
// pipe full through the carrier's bursts; past that it only queues, ahead
// of every small stream behind olt, and the window never goes below the
// BDP for it.
//
// ai-generated: whole function.
func (b *bbr) trimQueue() {
	if b.rttLast == 0 {
		return
	}
	bw, target := b.bw.get(), b.queueTarget()
	if excess := b.rttLast - b.minRTT - target; excess > 0 {
		b.trim += bw * excess.Seconds() / 2
	} else {
		b.trim -= bw * target.Seconds() / 2
	}
	b.trim = max(0, min(b.trim, b.aggregation()))
}
