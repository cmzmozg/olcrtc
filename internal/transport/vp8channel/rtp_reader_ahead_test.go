package vp8channel

import (
	"reflect"
	"testing"

	"github.com/pion/rtp"
)

// vp8Pkt is an RTP packet with a one-byte VP8 payload descriptor, the S bit
// set when start says so.
func vp8Pkt(seq uint16, start, marker bool) *rtp.Packet {
	descriptor := byte(0)
	if start {
		descriptor = 0x10
	}
	return &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: seq, Marker: marker},
		Payload: []byte{descriptor, 'x'},
	}
}

// TestReorderBufferTakesWholeFramesPastAGap checks that a frame held behind
// a lost packet goes out as soon as it is whole, that its packets are not
// delivered again once the gap fills, and that a partial frame still waits.
//
// ai-generated: the whole test (frames past a gap).
func TestReorderBufferTakesWholeFramesPastAGap(t *testing.T) {
	b := newReorderBuffer()
	var ahead [][]uint16
	b.ahead = func(run []*rtp.Packet) {
		seqs := make([]uint16, 0, len(run))
		for _, pkt := range run {
			seqs = append(seqs, pkt.SequenceNumber)
		}
		ahead = append(ahead, seqs)
	}
	steps := []struct {
		pkt       *rtp.Packet
		delivered []uint16
		ahead     [][]uint16
	}{
		{vp8Pkt(10, true, true), []uint16{10}, nil},
		{vp8Pkt(12, true, false), nil, nil},                  // 11 lost, frame 12-13 incomplete
		{vp8Pkt(13, false, true), nil, [][]uint16{{12, 13}}}, // whole: taken past the gap
		{vp8Pkt(15, false, true), nil, nil},                  // 14 missing: waits
		{vp8Pkt(11, true, true), []uint16{11}, nil},          // the repair, 12-13 stepped over
		{vp8Pkt(13, false, true), nil, nil},                  // a late duplicate
		{vp8Pkt(14, true, false), []uint16{14, 15}, nil},     // in order again
		{vp8Pkt(16, true, true), []uint16{16}, nil},
	}
	for i, step := range steps {
		ahead = nil
		got := pushedSeqs(b, step.pkt)
		if !reflect.DeepEqual(got, step.delivered) || !reflect.DeepEqual(ahead, step.ahead) {
			t.Fatalf("step %d (seq %d): delivered %v ahead %v, want %v and %v",
				i, step.pkt.SequenceNumber, got, ahead, step.delivered, step.ahead)
		}
	}
	if b.held() != 0 {
		t.Fatalf("held %d after the gap closed, want 0", b.held())
	}
}

// TestReorderBufferSkipsPastTakenFrames checks that a lost packet given up
// after the hold leaves no taken sequence behind.
//
// ai-generated: the whole test (frames past a gap).
func TestReorderBufferSkipsPastTakenFrames(t *testing.T) {
	b := newReorderBuffer()
	b.ahead = func([]*rtp.Packet) {}
	_ = pushedSeqs(b, vp8Pkt(10, true, true))
	_ = pushedSeqs(b, vp8Pkt(12, true, true)) // 11 lost, 12 taken
	b.skipToOldest()
	if b.nextSeq != 12 {
		t.Fatalf("nextSeq = %d after the skip, want 12, the taken frame", b.nextSeq)
	}
	if got := pushedSeqs(b, vp8Pkt(13, true, true)); !reflect.DeepEqual(got, []uint16{13}) {
		t.Fatalf("delivered %v, want [13] with 12 stepped over", got)
	}
	if b.held() != 0 {
		t.Fatalf("held %d, want 0", b.held())
	}
}
