package vp8channel

import (
	"encoding/binary"
	"testing"

	kcp "github.com/xtaci/kcp-go/v5"
)

// ai-generated: tests for queued duplicate push suppression.
func pushSegment(sn uint32, payload int) []byte {
	seg := make([]byte, kcp.IKCP_OVERHEAD+payload)
	seg[kcpCmdOff] = kcp.IKCP_CMD_PUSH
	binary.LittleEndian.PutUint32(seg[kcpSnOff:], sn)
	binary.LittleEndian.PutUint32(seg[kcpLenOff:], uint32(payload)) //nolint:gosec // test sizes are small
	return seg
}

func TestQueuedDuplicateWhileQueued(t *testing.T) {
	c := &kcpConn{}
	seg := pushSegment(7, 32)
	if c.queuedDuplicate(seg) {
		t.Fatal("the first copy was reported as a duplicate")
	}
	packet := &packetBuffer{}
	c.track(packet, seg)
	if !c.queuedDuplicate(seg) {
		t.Fatal("a resend of a queued push was not caught")
	}
	packet.release()
	if c.queuedDuplicate(seg) {
		t.Fatal("a resend after the copy left the queue was suppressed")
	}
}

func TestQueuedDuplicateNeedsOnlyQueuedPushes(t *testing.T) {
	c := &kcpConn{}
	c.track(&packetBuffer{}, pushSegment(1, 8))
	mixed := make([]byte, kcp.IKCP_OVERHEAD, 2*kcp.IKCP_OVERHEAD+8)
	mixed[kcpCmdOff] = kcp.IKCP_CMD_ACK
	mixed = append(mixed, pushSegment(1, 8)...)
	if c.queuedDuplicate(mixed) {
		t.Fatal("an ack riding with a duplicate was dropped")
	}
	if c.queuedDuplicate(append(pushSegment(1, 8), pushSegment(2, 8)...)) {
		t.Fatal("a new push riding with a duplicate was dropped")
	}
}

func TestQueuedDuplicateCountsCopies(t *testing.T) {
	c := &kcpConn{}
	seg := pushSegment(5, 8)
	first, second := &packetBuffer{}, &packetBuffer{}
	c.track(first, seg)
	c.track(second, seg)
	first.release()
	if !c.queuedDuplicate(seg) {
		t.Fatal("the second queued copy no longer counted")
	}
	second.release()
	if c.queuedDuplicate(seg) {
		t.Fatal("no copy is queued, yet the resend was suppressed")
	}
}

func TestQueuedDuplicateExpires(t *testing.T) {
	c := &kcpConn{}
	seg := pushSegment(3, 8)
	c.track(&packetBuffer{}, seg)
	c.queuedSN[3] = queuedPush{n: 1, at: monoNow() - int64(queuedHold)}
	if c.queuedDuplicate(seg) {
		t.Fatal("a push stuck past queuedHold still silenced its resend")
	}
}
