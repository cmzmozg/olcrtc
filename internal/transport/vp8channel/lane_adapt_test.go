package vp8channel

import (
	"testing"
	"time"
)

// ai-generated: tests for the adaptive send window of an uncapped lane.
func adaptLane(rtt int32) (*dataLane, *int) {
	got := new(int)
	l := &dataLane{
		window: func(segments int) { *got = segments },
		srtt:   func() int32 { return rtt },
		seen:   &kcpConn{},
	}
	return l, got
}

func TestAdaptSizesTheWindowToTwiceThePath(t *testing.T) {
	p := &streamTransport{sendWindow: kcpSndWnd}
	l, got := adaptLane(400)
	now := monoNow() + int64(adaptEvery)
	l.pace.wrote(now, 5<<20) // 5 MiB in the last second
	l.adapt(p, now)
	rate := float64(5 << 20)
	want := int(2*rate*0.4) / kcpMTU
	if *got != want {
		t.Fatalf("window=%d, want %d (twice rate x min rtt)", *got, want)
	}
}

func TestAdaptKeepsTheFloorAndTheCeiling(t *testing.T) {
	p := &streamTransport{sendWindow: kcpSndWnd}
	l, got := adaptLane(250)
	now := monoNow() + int64(adaptEvery)
	l.adapt(p, now) // nothing written yet
	if *got != adaptFloor {
		t.Fatalf("idle window=%d, want the floor %d", *got, adaptFloor)
	}
	l.pace.wrote(now+int64(adaptEvery), 64<<20)
	l.adapt(p, now+int64(adaptEvery))
	if *got != p.sendWindow {
		t.Fatalf("window=%d, want the transport's %d as the ceiling", *got, p.sendWindow)
	}
}

func TestAdaptLeavesCappedLanesAndWaitsItsTurn(t *testing.T) {
	p := &streamTransport{sendWindow: kcpSndWnd}
	l, got := adaptLane(250)
	now := monoNow() + int64(adaptEvery)
	l.pace.rate = 1 << 20
	l.adapt(p, now)
	if *got != 0 {
		t.Fatalf("a capped lane was resized to %d", *got)
	}
	l.pace.rate = 0
	l.adapt(p, now+int64(adaptEvery/2))
	if *got != 0 {
		t.Fatalf("resized %d before adaptEvery passed", *got)
	}
	l.adapt(p, now+int64(adaptEvery))
	if *got != adaptFloor {
		t.Fatalf("window=%d, want %d once uncapped", *got, adaptFloor)
	}
}

func TestAdaptRemembersTheShortestRoundTrip(t *testing.T) {
	p := &streamTransport{sendWindow: kcpSndWnd}
	rtt := int32(200)
	got := 0
	l := &dataLane{
		window: func(segments int) { got = segments },
		srtt:   func() int32 { return rtt },
		seen:   &kcpConn{},
	}
	now := monoNow() + int64(adaptEvery)
	l.adapt(p, now)
	rtt = 900 // a queue behind the session inflates its round trip
	now += int64(adaptEvery)
	l.pace.wrote(now, 4<<20)
	l.seen.acked.Add(3000) // and all of it got through
	l.adapt(p, now)
	rate := float64(4 << 20)
	if want := int(2*rate*0.2) / kcpMTU; got != want {
		t.Fatalf("window=%d, want %d from the 200 ms minimum, not 900", got, want)
	}
}

// ai-generated: the queue check and the delivered rate.
func TestAdaptFollowsWhatIsDeliveredWhileThePathQueues(t *testing.T) {
	p := &streamTransport{sendWindow: kcpSndWnd}
	rtt := int32(150)
	got := new(int)
	conn := &kcpConn{}
	l := &dataLane{
		window: func(segments int) { *got = segments },
		srtt:   func() int32 { return rtt },
		seen:   conn,
	}
	now := monoNow() + int64(adaptEvery)
	step := func(written int, acked uint64) {
		now += int64(adaptEvery)
		l.pace.wrote(now, written)
		conn.acked.Add(acked)
		l.adapt(p, now)
	}
	step(2<<20, 300) // writes 2 MiB/s, the peer acknowledges 300 pushes a second
	if *got != adaptFloor {
		t.Fatalf("window=%d on a clear path, want the floor %d over what the lane writes", *got, adaptFloor)
	}
	delivered := int(2*float64(300*kcpMTU)*0.15) / kcpMTU
	rtt = 150 + int32(queueBudget/time.Millisecond) + 50 // the path holds a queue
	step(2<<20, 300)
	if *got != delivered {
		t.Fatalf("window=%d while the path queues, want %d from what was delivered", *got, delivered)
	}
	rtt = 150 // the queue drained, the lane still busy
	step(2<<20, 300)
	if *got != delivered {
		t.Fatalf("window=%d once the queue drained, want it held at %d while the lane is busy", *got, delivered)
	}
	for range 5 { // quiet: the busiest second of the last four runs out
		step(0, 0)
	}
	if *got != adaptFloor {
		t.Fatalf("window=%d after the lane went quiet, want the floor %d back", *got, adaptFloor)
	}
}

// ai-generated: the queue pace.
func TestQueuePaceFollowsTheBestSecondDeliveredWhileThePathQueues(t *testing.T) {
	l := &dataLane{deliveries: [4]float64{0, 300 << 10, 100 << 10, 0}}
	l.queuePace()
	if l.qpace.rate != 0 {
		t.Fatalf("pace %.0f on a path that does not queue, want none", l.qpace.rate)
	}
	l.queuing = true
	l.queuePace()
	if want := queuePaceGain * float64(300<<10); l.qpace.rate != want {
		t.Fatalf("pace %.0f while queuing, want %.0f over the best second, not the last", l.qpace.rate, want)
	}
	l.deliveries = [4]float64{}
	l.queuePace()
	if l.qpace.rate != minPaceRate {
		t.Fatalf("pace %.0f after four empty seconds, want the floor %d", l.qpace.rate, minPaceRate)
	}
	p := &streamTransport{batchSize: 8}
	now := monoNow()
	l.qpace.credit, l.qpace.creditAt = 0, now
	l.out = make(chan *packetBuffer, 1)
	l.out <- &packetBuffer{}
	if _, ok := l.sampleLimit(p, now); ok || !l.heldBack {
		t.Fatalf("sampleLimit let a sample out with no pace credit left, held=%v", l.heldBack)
	}
}
