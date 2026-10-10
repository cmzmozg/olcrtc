package vp8channel

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/xtaci/kcp-go/v5"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ai-generated: the whole file (the split answers and the busy check).

func TestAnswerPrefixFindsTheLeadingAnswers(t *testing.T) {
	ack, wins, push := byte(kcp.IKCP_CMD_ACK), byte(kcp.IKCP_CMD_WINS), byte(kcp.IKCP_CMD_PUSH)
	hostile := kcpSegments(nil, ack, push)
	binary.LittleEndian.PutUint32(hostile[kcpLenOff:], 0xffffffff)
	for _, tc := range []struct {
		name string
		segs []byte
		want int
	}{
		{"answers before a push", kcpSegments(nil, ack, wins, push), 2 * kcp.IKCP_OVERHEAD},
		{"answers alone", kcpSegments(nil, ack, ack), 2 * kcp.IKCP_OVERHEAD},
		{"a push first", kcpSegments(nil, push, ack), 0},
		{"a length past the end", hostile, 0},
	} {
		if got := answerPrefix(tc.segs); got != tc.want {
			t.Errorf("%s: answerPrefix = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestQuietLaneKeepsItsFramesGoingDark checks that a lane which wrote next
// to nothing before its spell keeps full frames: what went unanswered was
// the way back.
func TestQuietLaneKeepsItsFramesGoingDark(t *testing.T) {
	tr := newStreamTransport(&fakeVideoStream{canSend: true}, nil,
		transport.Config{ChannelID: "quiet-lane"}, Options{FPS: 50, BatchSize: 64})
	tr.blackoutAfter = 100 * time.Millisecond
	out := make(chan *packetBuffer, 16)
	conn := newKCPConn(out, 16, testEpochHdr(1))
	lane := &dataLane{
		out: out, acks: make(chan *packetBuffer, 8), name: "quiet-lane",
		conn: func() *kcpConn { return conn }, window: func(int) {},
	}
	write := func([]byte) bool { return true }
	_, _ = conn.WriteTo(kcpSegments([]byte("d"), kcp.IKCP_CMD_PUSH), nil)
	lane.flush(tr, write)
	time.Sleep(2 * tr.blackoutAfter)
	lane.flush(tr, write)
	if conn.holdEvery.Load() == 0 {
		t.Fatal("the quiet lane did not go dark")
	}
	if lane.frames.limit != 0 {
		t.Fatalf("frames %d going dark, want a quiet lane's frames left full", lane.frames.limit)
	}
}
