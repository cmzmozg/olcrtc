package vp8channel

// ai-generated: the whole file (rtp pacing on the server's track).

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4/pkg/media"
)

// pacedSink records the packets a pacer writes and when.
type pacedSink struct {
	mu   sync.Mutex
	pkts []*rtp.Packet
	at   []time.Time
}

func (s *pacedSink) WriteRTP(h *rtp.Header, payload []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pkts = append(s.pkts, &rtp.Packet{Header: h.Clone(), Payload: bytes.Clone(payload)})
	s.at = append(s.at, time.Now())
	return h.MarshalSize() + len(payload), nil
}

func (s *pacedSink) Write(b []byte) (int, error) { return len(b), nil }

func (s *pacedSink) sink(pkt *rtp.Packet) { _, _ = s.WriteRTP(&pkt.Header, pkt.Payload) }

func (s *pacedSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pkts)
}

// waitFor waits until s holds n packets.
func (s *pacedSink) waitFor(tb testing.TB, n int) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.len() < n {
		if time.Now().After(deadline) {
			tb.Fatalf("pacer wrote %d packets, want %d", s.len(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func startPacer(tb testing.TB, rate int64, lag time.Duration) *rtpPacer {
	tb.Helper()
	p := newRTPPacer()
	p.rate, p.lag = rate, lag
	done := make(chan struct{})
	tb.Cleanup(func() { close(done) })
	go p.run(done)
	return p
}

func TestPacedTrackSpreadsASampleAtTheRate(t *testing.T) {
	const rate = 1 << 20
	pacer := startPacer(t, rate, time.Second)
	var paced, plain pacedSink
	track, err := newVP8Track(newVP8Payloader())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (&pacedTrack{TrackLocalStaticSample: track, pacer: pacer}).Bind(
		&captureContext{w: &rtpCapture{sink: paced.sink}}); err != nil {
		t.Fatal(err)
	}
	ref, err := newVP8Track(newVP8Payloader())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ref.Bind(&captureContext{w: &rtpCapture{sink: plain.sink}}); err != nil {
		t.Fatal(err)
	}
	sample := make([]byte, 24<<10)
	for i := range sample {
		sample[i] = byte(rand.Uint32()) //nolint:gosec // test bytes, not a secret
	}
	start := time.Now()
	if err = track.WriteSample(media.Sample{Data: sample, Duration: time.Second / 60}); err != nil {
		t.Fatal(err)
	}
	if paced.len() != 1 {
		t.Fatalf("%d packets went out with the sample, want the first alone", paced.len())
	}
	if err = ref.WriteSample(media.Sample{Data: sample, Duration: time.Second / 60}); err != nil {
		t.Fatal(err)
	}
	n := plain.len()
	paced.waitFor(t, n)

	bytesAfterFirst := 0
	for i, pkt := range paced.pkts {
		if !bytes.Equal(pkt.Payload, plain.pkts[i].Payload) || pkt.SequenceNumber != paced.pkts[0].SequenceNumber+uint16(i) {
			t.Fatalf("packet %d is not the one pion cut, or out of order", i)
		}
		if i > 0 {
			bytesAfterFirst += pkt.MarshalSize()
		}
	}
	want := time.Duration(bytesAfterFirst) * time.Second / rate
	if span := paced.at[n-1].Sub(start); span < want*3/4 {
		t.Fatalf("%d packets went out over %v, want about %v", n, span, want)
	}
}

func TestRTPPacerHoldsNoPacketPastTheLag(t *testing.T) {
	const lag = 20 * time.Millisecond
	pacer := startPacer(t, 1<<10, lag)
	var sink pacedSink
	payload := make([]byte, 1000)
	start := time.Now()
	for seq := range uint16(10) {
		if _, err := pacer.write(&sink, &rtp.Header{SequenceNumber: seq}, payload); err != nil {
			t.Fatal(err)
		}
	}
	sink.waitFor(t, 10)
	for i, pkt := range sink.pkts {
		if int(pkt.SequenceNumber) != i {
			t.Fatalf("packet %d went out as %d", i, pkt.SequenceNumber)
		}
	}
	if late := sink.at[9].Sub(start); late > lag+50*time.Millisecond {
		t.Fatalf("the last packet went out %v after it was written, the lag is %v", late, lag)
	}
}

func TestRTPPacerWritesAtOnceWhenIdleAndCopiesWhatItQueues(t *testing.T) {
	p := newRTPPacer()
	p.rate = 1 << 10 // a 15-byte packet keeps the pace busy for 15 ms
	var sink pacedSink
	payload := []byte{1, 2, 3}
	hdr := rtp.Header{SequenceNumber: 1}
	if _, err := p.write(&sink, &hdr, payload); err != nil || sink.len() != 1 {
		t.Fatalf("an idle pacer queued the packet: %d written, err %v", sink.len(), err)
	}
	hdr.SequenceNumber = 2
	if _, err := p.write(&sink, &hdr, payload); err != nil || sink.len() != 1 {
		t.Fatalf("a packet right behind another went out at once: %d written, err %v", sink.len(), err)
	}
	hdr.SequenceNumber, payload[0] = 9, 9
	if _, ok := p.flush(monoNow() + int64(time.Second)); ok {
		t.Fatal("a packet is still queued after its time")
	}
	if got := sink.pkts[1]; got.SequenceNumber != 2 || got.Payload[0] != 1 {
		t.Fatalf("the queued packet went out as seq %d payload %v", got.SequenceNumber, got.Payload)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := p.write(&sink, &hdr, payload); err != nil || sink.len() != 3 {
		t.Fatalf("a pacer idle again queued the packet: %d written, err %v", sink.len(), err)
	}
}
