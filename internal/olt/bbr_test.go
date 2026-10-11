package olt

// ai-generated: the whole file (unit cover for the congestion controller:
// ProbeRTT, the queue trim and loss in application-limited rounds).

import (
	"testing"
	"time"
)

// filledBBR returns a controller in ProbeBW at bw bytes per second over a
// path of minRTT, with an aggregation allowance of extra bytes.
//
// ai-generated: whole function.
func filledBBR(bw float64, minRTT time.Duration, extra float64) *bbr {
	b := &bbr{}
	b.init(1100, 60<<10, time.Second/60, nil)
	b.bw.reset(1, bw)
	b.extra.reset(1, extra)
	b.minRTT, b.filled, b.round = minRTT, true, 1
	b.enterProbeBW(0)
	return b
}

// TestProbeRTT checks that a minimum RTT not seen again for minRTTWin
// sends the controller to ProbeRTT at half the BDP, and that it comes back
// with the minimum found there.
//
// ai-generated: whole function.
func TestProbeRTT(t *testing.T) {
	b := filledBBR(1e6, 80*time.Millisecond, 0)
	b.onRTT(time.Second, 80*time.Millisecond)
	now := time.Second + minRTTWin + time.Millisecond
	b.onRTT(now, 200*time.Millisecond)
	if b.state != bbrProbeRTT {
		t.Fatalf("state %s, want probe_rtt", b.stateName())
	}
	if w := b.cwnd(); w != int(probeRTTGain*b.bdp()) {
		t.Fatalf("ProbeRTT window %d, want half of BDP %.0f", w, b.bdp())
	}
	b.advance(now, b.cwnd())
	b.onRTT(now+50*time.Millisecond, 90*time.Millisecond)
	b.round++
	b.advance(now+probeRTTTime+time.Millisecond, 0)
	if b.state != bbrProbeBW || b.minRTT != 90*time.Millisecond {
		t.Fatalf("state %s, minRTT %v; want probe_bw and 90ms", b.stateName(), b.minRTT)
	}
}

// TestQueueTrim checks that rounds whose smallest RTT stands a queue above
// the minimum take the aggregation allowance off the window, never below
// the BDP, and that rounds without one give it back.
//
// ai-generated: whole function.
func TestQueueTrim(t *testing.T) {
	b := filledBBR(1e6, 80*time.Millisecond, 50e3)
	full := b.cwnd()
	for range 10 {
		b.rttLast = 160 * time.Millisecond
		b.trimQueue()
	}
	if w := b.cwnd(); w != int(b.bdp()) {
		t.Fatalf("window %d with a standing queue, want the BDP %.0f (from %d)", w, b.bdp(), full)
	}
	for range 20 {
		b.rttLast = 85 * time.Millisecond
		b.trimQueue()
	}
	if w := b.cwnd(); w != full {
		t.Fatalf("window %d after the queue went, want %d back", w, full)
	}
}

// TestAppLimitedLoss checks that heavy loss in a round the application
// left short neither ends Startup nor cuts the bandwidth.
//
// ai-generated: whole function.
func TestAppLimitedLoss(t *testing.T) {
	b := &bbr{}
	b.init(1100, 60<<10, time.Second/60, nil)
	b.roundLost, b.roundAcked = 30*1100, 10*1100
	b.endRound(true)
	if b.filled {
		t.Fatal("an application-limited lossy round ended Startup")
	}
	b.roundLost, b.roundAcked = 30*1100, 10*1100
	b.endRound(false)
	if !b.filled {
		t.Fatal("a lossy round at full rate did not end Startup")
	}
}
