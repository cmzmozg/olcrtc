package vp8channel

import (
	"bytes"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// scratchBinding binds one SSRC through Pion's default interceptors and records wire packets.
type scratchBinding struct {
	chain interceptor.Interceptor
	ctx   *scratchContext
	sent  chan *rtp.Packet
}

// scratchContext supplies a distinct binding while keeping the normal VP8 codec parameters.
type scratchContext struct {
	*captureContext
	id     string
	ssrc   webrtc.SSRC
	writer webrtc.TrackLocalWriter
}

// ai-generated: distinguish two bindings of the same sample track.
func (c *scratchContext) ID() string { return c.id }

// ai-generated: route each binding's NACK to its own SSRC.
func (c *scratchContext) SSRC() webrtc.SSRC { return c.ssrc }

// ai-generated: send paced packets through the binding's real interceptor chain.
func (c *scratchContext) WriteStream() webrtc.TrackLocalWriter { return c.writer }

// scratchWriter adapts a default interceptor chain to a track binding.
type scratchWriter struct{ interceptor.RTPWriter }

// ai-generated: preserve the interceptor writer's packet result.
func (w scratchWriter) WriteRTP(h *rtp.Header, payload []byte) (int, error) {
	return w.RTPWriter.Write(h, payload, nil)
}

// ai-generated: satisfy the track writer; the sample track only calls WriteRTP.
func (scratchWriter) Write(b []byte) (int, error) { return len(b), nil }

// ai-generated: build one real default NACK responder behind a packet-owning test sink.
func newScratchBinding(t *testing.T, id string, ssrc webrtc.SSRC) *scratchBinding {
	t.Helper()
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(&webrtc.MediaEngine{}, registry); err != nil {
		t.Fatal(err)
	}
	chain, err := registry.Build(id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chain.Close() })
	b := &scratchBinding{chain: chain, sent: make(chan *rtp.Packet, 256)}
	writer := chain.BindLocalStream(&interceptor.StreamInfo{
		SSRC: uint32(ssrc), ClockRate: 90000, RTCPFeedback: []interceptor.RTCPFeedback{{Type: "nack"}},
	}, interceptor.RTPWriterFunc(func(h *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
		b.sent <- &rtp.Packet{Header: h.Clone(), Payload: bytes.Clone(payload)}
		return len(payload), nil
	}))
	b.ctx = &scratchContext{captureContext: &captureContext{}, id: id, ssrc: ssrc, writer: scratchWriter{writer}}
	return b
}

// ai-generated: read a bounded packet result from the actual interceptor output.
func (b *scratchBinding) receive(t *testing.T) *rtp.Packet {
	t.Helper()
	select {
	case packet := <-b.sent:
		return packet
	case <-time.After(3 * time.Second):
		t.Fatal("RTP packet timed out")
		return nil
	}
}

// ai-generated: trigger a real RTCP NACK read after the payloader's storage was reused.
func (b *scratchBinding) nack(t *testing.T, packet *rtp.Packet) {
	t.Helper()
	wire, err := rtcp.Marshal([]rtcp.Packet{&rtcp.TransportLayerNack{
		SenderSSRC: 9, MediaSSRC: packet.SSRC, Nacks: []rtcp.NackPair{{PacketID: packet.SequenceNumber}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	reader := b.chain.BindRTCPReader(interceptor.RTCPReaderFunc(
		func(buf []byte, attrs interceptor.Attributes) (int, interceptor.Attributes, error) {
			return copy(buf, wire), attrs, nil
		}))
	if _, _, err := reader.Read(make([]byte, 1500), nil); err != nil {
		t.Fatal(err)
	}
	got := b.receive(t)
	if got.SSRC != packet.SSRC || got.SequenceNumber != packet.SequenceNumber || !bytes.Equal(got.Payload, packet.Payload) {
		t.Fatal("NACK retransmitted bytes from a later sample")
	}
}

// ai-generated: preserve old RTP bytes through server scratch reuse, queued pacing, two bindings and real NACKs.
func TestServerVP8ScratchPreservesNACKAcrossBindings(t *testing.T) {
	v := newVP8Payloader()
	v.scratch = &vp8Scratch{}
	first := bytes.Repeat([]byte{0x35}, 4096)
	v.Payload(1188, first) // Warm up the descriptor so both following samples reuse the same buffer.
	track, err := newVP8Track(v)
	if err != nil {
		t.Fatal(err)
	}
	pacer := newRTPPacer()
	pacer.rate = 1 << 10
	paced := &pacedTrack{TrackLocalStaticSample: track, pacer: pacer}
	bindings := []*scratchBinding{newScratchBinding(t, "first", 1), newScratchBinding(t, "second", 2)}
	for _, binding := range bindings {
		if _, err := paced.Bind(binding.ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, data := range [][]byte{first, bytes.Repeat([]byte{0x76}, len(first))} {
		if err := track.WriteSample(media.Sample{Data: data, Duration: time.Second / 60}); err != nil {
			t.Fatal(err)
		}
	}
	if _, waiting := pacer.flush(monoNow() + int64(time.Second)); waiting {
		t.Fatal("pacer kept packets after their deadlines")
	}
	reference := &codecs.VP8Payloader{EnablePictureID: true}
	reference.Payload(1188, first)
	expected := reference.Payload(1188, first)
	for _, binding := range bindings {
		var old *rtp.Packet
		for i := range 2 * len(expected) {
			packet := binding.receive(t)
			if i < len(expected) && !bytes.Equal(packet.Payload, expected[i]) {
				t.Fatal("pacer kept a reference to overwritten scratch")
			}
			if i == len(expected)-1 {
				old = packet
			}
		}
		binding.nack(t, old)
	}
}
