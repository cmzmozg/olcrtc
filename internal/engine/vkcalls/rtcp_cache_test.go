// SPDX-License-Identifier: Apache-2.0

package vkcalls

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// ai-generated: a stats recorder observing decoded packet identity without background work.
type cacheRecorder struct {
	stats.Recorder
	packets []rtcp.Packet
	err     error
	missing bool
}

// ai-generated: observe the packet cache used by each stats recorder.
func (r *cacheRecorder) QueueIncomingRTCP(_ time.Time, buf []byte, attrs interceptor.Attributes) {
	if attrs == nil {
		r.missing = true
		return
	}
	r.packets, r.err = attrs.GetRTCPPackets(buf)
}

// ai-generated: no background recorder work is needed for cache identity tests.
func (*cacheRecorder) Start() {}

// ai-generated: the test recorder owns no background resources.
func (*cacheRecorder) Stop() {}

// ai-generated: build a real Pion stats chain with optional cache sharing for tests and benchmarks.
func cacheStatsChain(tb testing.TB, cached bool, opts ...stats.Option) interceptor.Interceptor {
	tb.Helper()
	factory, err := stats.NewInterceptor(opts...)
	if err != nil {
		tb.Fatal(err)
	}
	registry := &interceptor.Registry{}
	registry.Add(factory)
	if cached {
		registry.Add(rtcpCacheFactory{})
	}
	chain, err := registry.Build("cache-test")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = chain.Close() })
	for ssrc := uint32(1); ssrc <= 4; ssrc++ {
		chain.BindLocalStream(&interceptor.StreamInfo{SSRC: ssrc, ClockRate: 90000},
			interceptor.RTPWriterFunc(func(_ *rtp.Header, buf []byte, _ interceptor.Attributes) (int, error) {
				return len(buf), nil
			}))
	}
	return chain
}

// ai-generated: require one decode across SSRCs, retain caller attributes and refresh on the next packet.
func TestRTCPStatsSharePacketCache(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "supplied"}[supplied], func(t *testing.T) {
			var recorders []*cacheRecorder
			chain := cacheStatsChain(t, true, stats.SetRecorderFactory(func(uint32, float64) stats.Recorder {
				r := &cacheRecorder{}
				recorders = append(recorders, r)
				return r
			}))
			wire, err := rtcp.Marshal([]rtcp.Packet{&rtcp.PictureLossIndication{SenderSSRC: 9, MediaSSRC: 1}})
			if err != nil {
				t.Fatal(err)
			}
			reader := chain.BindRTCPReader(interceptor.RTCPReaderFunc(
				func(buf []byte, attrs interceptor.Attributes) (int, interceptor.Attributes, error) {
					return copy(buf, wire), attrs, nil
				}))
			var previous rtcp.Packet
			for range 2 {
				var attrs interceptor.Attributes
				if supplied {
					attrs = interceptor.Attributes{"caller": true}
				}
				n, returned, err := reader.Read(make([]byte, 1500), attrs)
				if err != nil || n != len(wire) || returned == nil {
					t.Fatalf("read: n=%d, attrs=%v, err=%v", n, returned, err)
				}
				packets, err := returned.GetRTCPPackets(nil)
				if err != nil || len(packets) != 1 || packets[0] == previous {
					t.Fatalf("cache was not refreshed: packets=%v, err=%v", packets, err)
				}
				if supplied && attrs["caller"] != returned["caller"] {
					t.Fatal("caller attributes were lost")
				}
				for _, r := range recorders {
					if r.missing || r.err != nil || len(r.packets) != 1 || r.packets[0] != packets[0] {
						t.Fatal("stats decoded RTCP separately for an SSRC")
					}
				}
				previous = packets[0]
			}
		})
	}
}

// ai-generated: preserve read failures and avoid retaining a cache across failed reads.
func TestRTCPCacheReadError(t *testing.T) {
	var previous interceptor.Attributes
	reader := (&rtcpCache{}).BindRTCPReader(interceptor.RTCPReaderFunc(
		func(_ []byte, attrs interceptor.Attributes) (int, interceptor.Attributes, error) {
			if attrs["previous"] != nil {
				t.Fatal("a previous read's cache leaked")
			}
			attrs["previous"] = true
			previous = attrs
			return 3, attrs, io.ErrUnexpectedEOF
		}))
	for range 2 {
		n, attrs, err := reader.Read(make([]byte, 4), nil)
		if n != 3 || !errors.Is(err, io.ErrUnexpectedEOF) || attrs["previous"] != previous["previous"] {
			t.Fatalf("read result changed: n=%d attrs=%v err=%v", n, attrs, err)
		}
	}
}

// ai-generated: ensure the cache boundary preserves real NACK retransmission through Pion's default chain.
func TestRTCPCacheAnswersNACK(t *testing.T) {
	registry := &interceptor.Registry{}
	media := &webrtc.MediaEngine{}
	if err := webrtc.RegisterDefaultInterceptors(media, registry); err != nil {
		t.Fatal(err)
	}
	registry.Add(rtcpCacheFactory{})
	chain, err := registry.Build("nack-cache-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chain.Close() })
	sent := make(chan uint16, 4)
	writer := chain.BindLocalStream(&interceptor.StreamInfo{
		SSRC: 7, ClockRate: 90000, RTCPFeedback: []interceptor.RTCPFeedback{{Type: "nack"}},
	}, interceptor.RTPWriterFunc(func(h *rtp.Header, buf []byte, _ interceptor.Attributes) (int, error) {
		sent <- h.SequenceNumber
		return len(buf), nil
	}))
	if _, err = writer.Write(&rtp.Header{SSRC: 7, SequenceNumber: 11}, []byte("frame"), nil); err != nil {
		t.Fatal(err)
	}
	<-sent
	wire, err := rtcp.Marshal([]rtcp.Packet{&rtcp.TransportLayerNack{
		SenderSSRC: 1, MediaSSRC: 7, Nacks: []rtcp.NackPair{{PacketID: 11}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	reader := chain.BindRTCPReader(interceptor.RTCPReaderFunc(
		func(buf []byte, attrs interceptor.Attributes) (int, interceptor.Attributes, error) {
			return copy(buf, wire), attrs, nil
		}))
	if _, _, err := reader.Read(make([]byte, 1500), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case seq := <-sent:
		if seq != 11 {
			t.Fatalf("retransmitted seq=%d", seq)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("NACK retransmission timed out")
	}
}

// ai-generated: build a TWCC report with 128 received packet deltas for the hot-path benchmark.
func cacheTWCCWire(tb testing.TB) []byte {
	tb.Helper()
	packet := &rtcp.TransportLayerCC{
		Header:     rtcp.Header{Padding: true, Count: rtcp.FormatTCC, Type: rtcp.TypeTransportSpecificFeedback},
		SenderSSRC: 9, MediaSSRC: 1, PacketStatusCount: 128,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 128,
		}},
		RecvDeltas: make([]*rtcp.RecvDelta, 128),
	}
	for i := range packet.RecvDeltas {
		packet.RecvDeltas[i] = &rtcp.RecvDelta{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 250}
	}
	packet.Header.Length = uint16(packet.MarshalSize()/4 - 1) //nolint:gosec // fixed 128-delta report fits uint16
	wire, err := packet.Marshal()
	if err != nil {
		tb.Fatal(err)
	}
	return wire
}

// ai-generated: compare repeated TWCC decoding with the shared cache across four SSRCs.
func BenchmarkRTCPStatsCache(b *testing.B) {
	for _, cached := range []bool{false, true} {
		b.Run(map[bool]string{false: "separate", true: "shared"}[cached], func(b *testing.B) {
			chain := cacheStatsChain(b, cached)
			wire := cacheTWCCWire(b)
			reader := chain.BindRTCPReader(interceptor.RTCPReaderFunc(
				func(buf []byte, attrs interceptor.Attributes) (int, interceptor.Attributes, error) {
					return copy(buf, wire), attrs, nil
				}))
			buf := make([]byte, 1500)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, _, err := reader.Read(buf, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
