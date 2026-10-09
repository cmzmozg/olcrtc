package vp8channel

import (
	"testing"
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
	l.adapt(p, now)
	rate := float64(4 << 20)
	if want := int(2*rate*0.2) / kcpMTU; got != want {
		t.Fatalf("window=%d, want %d from the 200 ms minimum, not 900", got, want)
	}
}
