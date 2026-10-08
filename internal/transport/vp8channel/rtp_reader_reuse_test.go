package vp8channel

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/pion/rtp"
)

// ai-generated: verify frame assembly when a reader reuses its RTP header and wire buffer.
func TestReorderWithReusedRTPParser(t *testing.T) {
	frame := make([]byte, 60)
	for i := range frame {
		frame[i] = byte(i)
	}
	packets := make([][]byte, 3)
	for i := range packets {
		packet := rtp.Packet{
			Header: rtp.Header{
				Version: 2, SequenceNumber: uint16(i + 1), Marker: i == 2,
				CSRC: []uint32{uint32(i + 1)},
			},
			Payload: append([]byte{0}, frame[i*20:(i+1)*20]...),
		}
		if i == 0 {
			packet.Payload[0] = 0x10
		}
		if err := packet.SetExtension(3, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		wire, err := packet.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		packets[i] = wire
	}
	buffer := newReorderBuffer()
	var state vp8FrameState
	var parsed rtp.Packet
	wire := make([]byte, rtpBufSize)
	var complete []byte
	for _, i := range []int{0, 2, 1} {
		n := copy(wire, packets[i])
		if err := parsed.Unmarshal(wire[:n]); err != nil {
			t.Fatal(err)
		}
		buffer.push(&parsed, func(ordered *rtp.Packet) {
			if result := state.processRTPPacket(ordered); result != nil {
				complete = bytes.Clone(result)
			}
		})
	}
	if !bytes.Equal(complete, frame) {
		t.Fatalf("reordered frame = %x, want %x", complete, frame)
	}
}

// ai-generated: benchmark RTP parsing with extensions followed by reorder and VP8 assembly.
func BenchmarkRTPParseReorder(b *testing.B) {
	packet := rtp.Packet{
		Header:  rtp.Header{Version: 2, Marker: true},
		Payload: append([]byte{0x10}, make([]byte, 1200)...),
	}
	for _, id := range []uint8{3, 5, 7} {
		if err := packet.SetExtension(id, []byte{1, 2}); err != nil {
			b.Fatal(err)
		}
	}
	wire, err := packet.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	buffer := newReorderBuffer()
	var state vp8FrameState
	var sequence uint16
	var parsed rtp.Packet
	complete := func(ordered *rtp.Packet) {
		if len(state.processRTPPacket(ordered)) != 1200 {
			b.Fatal("incomplete VP8 frame")
		}
	}
	b.ReportAllocs()
	b.SetBytes(1200)
	b.ResetTimer()
	for range b.N {
		binary.BigEndian.PutUint16(wire[2:4], sequence)
		sequence++
		if err := parsed.Unmarshal(wire); err != nil {
			b.Fatal(err)
		}
		buffer.push(&parsed, complete)
	}
}
