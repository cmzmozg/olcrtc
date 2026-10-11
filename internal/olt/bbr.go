package olt

import "time"

// Congestion control: a rate-based controller after BBR v1. The bottleneck
// bandwidth is the windowed maximum of delivery-rate samples over bwRounds
// round trips, the minimum RTT the windowed minimum over minRTTWin, and the
// connection is paced at a gain over the bandwidth: Startup at 2 until the
// rate stops growing by a quarter for three rounds, Drain at 1/2 until the
// queue it built is gone, then ProbeBW cycling through probeGains, one phase
// per minimum RTT. The window is 2 x BDP plus two carrier bursts: the
// carrier's frame structure inflates the RTT without any queue, and the
// allowance keeps the window from capping the rate. Loss lowers nothing
// unless a round trip loses more than lossThresh of what it carried twice
// in a row: a lost video frame takes dozens of packets at once on this
// path, and that is not congestion.
const (
	bbrStartup = iota
	bbrDrain
	bbrProbeBW
)

const (
	startupGain = 2.0
	drainGain   = 0.5
	// fullBwGrowth and fullBwRounds detect the filled pipe.
	fullBwGrowth = 1.25
	fullBwRounds = 3
	// bwRounds is the bandwidth filter window, in round trips.
	bwRounds = 10
	// minRTTWin is the minimum RTT filter window.
	minRTTWin = 10 * time.Second
	// lossThresh is the share of a round's bytes lost that counts as heavy
	// loss; lossRounds heavy rounds in a row cut the bandwidth by lossCut.
	lossThresh = 0.2
	lossRounds = 2
	lossCut    = 0.85
	// minLossPackets keeps a few lost packets from making a round heavy.
	minLossPackets = 3
	// initCwndPackets is the window before the first bandwidth sample, on
	// top of the two bursts.
	initCwndPackets = 10
	// minRate is the lowest pacing rate, in bytes per second.
	minRate = 16 << 10
	// probePhases is the length of the ProbeBW gain cycle, drainPhaseMax
	// the most minimum RTTs its draining phase lasts.
	probePhases   = 8
	drainPhaseMax = 4
	// probeStartPhase is where ProbeBW enters its cycle: cruising.
	probeStartPhase = 2
)

// bwSample is one entry of the bandwidth filter.
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

	state  int
	bw     maxFilter
	minRTT time.Duration
	// rttCur and rttPrev are the minimum RTTs of the current and previous
	// half windows, the current one begun at rttAt.
	rttCur, rttPrev time.Duration
	rttAt           time.Duration

	round     uint64
	nextRound int64
	fullBw    float64
	fullCount int
	filled    bool
	phase     int
	phaseAt   time.Duration

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

// onRTT takes in an RTT sample for the windowed minimum: the smaller of the
// current and the previous half window.
//
// ai-generated: whole function.
func (b *bbr) onRTT(now, rtt time.Duration) {
	if now-b.rttAt >= minRTTWin/2 {
		b.rttPrev, b.rttCur, b.rttAt = b.rttCur, 0, now
	}
	if b.rttCur == 0 || rtt < b.rttCur {
		b.rttCur = rtt
	}
	b.minRTT = b.rttCur
	if b.rttPrev > 0 && b.rttPrev < b.minRTT {
		b.minRTT = b.rttPrev
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
	if roundStart {
		b.endRound(rs.appLimited)
	}
	b.advance(now, inflight)
	b.setRate()
}

// endRound checks the round that ended for heavy loss and for a filled
// pipe.
//
// ai-generated: whole function.
func (b *bbr) endRound(appLimited bool) {
	lost, acked := b.roundLost, b.roundAcked
	b.roundLost, b.roundAcked = 0, 0
	if lost >= minLossPackets*b.mss && float64(lost) > lossThresh*float64(lost+acked) {
		b.heavyRounds++
		b.onHeavyLoss(lost, acked)
		return
	}
	b.heavyRounds = 0
	if appLimited || b.filled {
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

// advance moves between the states. A ProbeBW phase lasts a minimum RTT,
// two carrier ticks at least, but the draining one goes on, up to
// drainPhaseMax of them, until the queue is down to the drain target: the
// bandwidth filter keeps the largest sample, ack aggregation makes samples
// run a little high, and a queue that only the probe's own excess drains
// would creep up to the window.
//
// ai-generated: whole function.
func (b *bbr) advance(now time.Duration, inflight int) {
	drained := float64(inflight) <= b.bdp()+float64(b.burst)
	switch b.state {
	case bbrStartup:
		if b.filled {
			b.state = bbrDrain
		}
	case bbrDrain:
		if drained {
			b.state, b.phase, b.phaseAt = bbrProbeBW, probeStartPhase, now
		}
	default:
		rtt := max(b.minRTT, 2*b.tick)
		elapsed := now - b.phaseAt
		if elapsed < rtt || (b.phase == 1 && !drained && elapsed < drainPhaseMax*rtt) {
			return
		}
		b.phase = (b.phase + 1) % probePhases
		b.phaseAt = now
	}
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
	w := int(2*b.bdp()) + 2*b.burst
	if !b.filled {
		w = max(w, b.initCwnd())
	}
	return w
}

// initCwnd is the window before the first bandwidth sample.
//
// ai-generated: whole function.
func (b *bbr) initCwnd() int { return initCwndPackets*b.mss + 2*b.burst }

// stateName names the state for Stats.
//
// ai-generated: whole function.
func (b *bbr) stateName() string {
	switch b.state {
	case bbrStartup:
		return "startup"
	case bbrDrain:
		return "drain"
	default:
		return "probe_bw"
	}
}
