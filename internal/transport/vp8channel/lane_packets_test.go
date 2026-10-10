package vp8channel

import (
	"testing"

	"github.com/xtaci/kcp-go/v5"
)

// ai-generated: the whole file (packet frames).

// TestPacketFramesWriteEveryPacketAsAFrame checks that a transport with a
// batch size of 1 writes a flush's packets, answers first, each as a frame
// of its own that fits one RTP packet, instead of one packet a flush.
func TestPacketFramesWriteEveryPacketAsAFrame(t *testing.T) {
	var written [][]byte
	p := &streamTransport{batchSize: 1, sampleWriter: func(data []byte) bool {
		written = append(written, append([]byte(nil), data...))
		return true
	}}
	if p.dataMTU() != packetFrameMTU || epochHdrLen+packetFrameMTU+wireCRCLen > 1188-4 {
		t.Fatalf("dataMTU %d: a full frame of %d bytes does not fit one RTP packet",
			p.dataMTU(), epochHdrLen+p.dataMTU()+wireCRCLen)
	}
	if p.fullPackets() < 40 {
		t.Fatalf("fullPackets %d, want about a sample's worth of frames a flush", p.fullPackets())
	}
	out, acks := make(chan *packetBuffer, 16), make(chan *packetBuffer, 16)
	l := &dataLane{out: out, acks: acks, conn: func() *kcpConn { return nil }}
	hdr := testEpochHdr(1)
	frame := func(cmd byte) *packetBuffer {
		data := append(append([]byte(nil), hdr[:]...), kcpSegments([]byte("d"), cmd)...)
		return &packetBuffer{data: append(data, make([]byte, wireCRCLen)...)}
	}
	for range 3 {
		acks <- frame(kcp.IKCP_CMD_ACK)
	}
	for range 5 {
		out <- frame(kcp.IKCP_CMD_PUSH)
	}
	if !l.flush(p, p.writeSampleLocked) {
		t.Fatal("flush wrote nothing")
	}
	if len(written) != 8 || len(out) != 0 || len(acks) != 0 {
		t.Fatalf("wrote %d frames, %d pushes and %d answers left; want all 8 out, one a frame",
			len(written), len(out), len(acks))
	}
}
