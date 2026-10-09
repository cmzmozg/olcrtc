package vp8channel

import (
	"bytes"
	"testing"

	"github.com/xtaci/kcp-go/v5"
)

// TestDataLaneRepeatsALightTail checks that a light sample carrying pushes
// goes out again once, repeatAfter later, and that acknowledgements alone
// and heavy samples go once.
//
// ai-generated: the whole test (the tail repeat).
func TestDataLaneRepeatsALightTail(t *testing.T) {
	p := &streamTransport{batchSize: 8}
	out := make(chan *packetBuffer, 8)
	l := &dataLane{out: out, conn: func() *kcpConn { return nil }}
	hdr := testEpochHdr(1)
	queue := func(body []byte, cmd byte) {
		frame := append(append([]byte(nil), hdr[:]...), kcpSegments(body, cmd)...)
		out <- &packetBuffer{data: append(frame, make([]byte, wireCRCLen)...)}
	}
	var written [][]byte
	write := func(sample []byte) bool {
		written = append(written, append([]byte(nil), sample...))
		return true
	}
	cases := []struct {
		name  string
		body  []byte
		cmd   byte
		count int
	}{
		{"a light push", []byte("hi"), kcp.IKCP_CMD_PUSH, 2},
		{"acknowledgements alone", nil, kcp.IKCP_CMD_ACK, 1},
		{"a heavy push", make([]byte, repeatMax), kcp.IKCP_CMD_PUSH, 1},
	}
	for _, tc := range cases {
		written = nil
		queue(tc.body, tc.cmd)
		l.flush(p, write)
		l.flush(p, write) // too soon for a repeat
		l.repeatAt -= int64(repeatAfter)
		l.flush(p, write)
		l.flush(p, write) // once only
		if len(written) != tc.count {
			t.Fatalf("%s: wrote %d samples, want %d", tc.name, len(written), tc.count)
		}
		if tc.count == 2 && !bytes.Equal(written[0], written[1]) {
			t.Fatalf("%s: the repeat differs from the original", tc.name)
		}
	}
}
