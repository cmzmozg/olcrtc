package vp8channel

import (
	"encoding/binary"
	"testing"
	"time"

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

// ai-generated: tests for the post-send resend veto.
func ackSegment(sn uint32) []byte {
	seg := make([]byte, kcp.IKCP_OVERHEAD)
	seg[kcpCmdOff] = kcp.IKCP_CMD_ACK
	binary.LittleEndian.PutUint32(seg[kcpSnOff:], sn)
	return seg
}

func TestResendWaitsOutTheMeasuredPath(t *testing.T) {
	c := &kcpConn{}
	seg := pushSegment(9, 8)
	sent := &packetBuffer{}
	c.track(sent, seg)
	sent.releaseSent()
	if c.queuedDuplicate(seg) {
		t.Fatal("a resend was held back before the path had a single sample")
	}
	c.noteAcks(ackSegment(9))
	if c.pathRTT <= 0 {
		t.Fatal("the acknowledgement left no round trip sample")
	}
	c.pathRTT, c.pathVar = int64(200*time.Millisecond), int64(10*time.Millisecond)
	again := &packetBuffer{}
	c.track(again, seg)
	again.releaseSent()
	if !c.queuedDuplicate(seg) {
		t.Fatal("a resend went out before a round trip had passed since the send")
	}
	c.sentAt[9] = monoNow() - int64(300*time.Millisecond)
	if c.queuedDuplicate(seg) {
		t.Fatal("a resend was still held back after the round trip passed")
	}
}

func TestResendVetoIsCappedBySentHold(t *testing.T) {
	c := &kcpConn{}
	c.markSent([]uint32{4})
	c.pathRTT, c.pathVar = int64(10*time.Second), int64(time.Second)
	c.sentAt[4] = monoNow() - int64(sentHold)
	if c.queuedDuplicate(pushSegment(4, 8)) {
		t.Fatal("a slow path held a resend back past sentHold")
	}
}

func parkedConn(t *testing.T) (*kcpConn, []byte) {
	t.Helper()
	c := &kcpConn{out: make(chan *packetBuffer, 4)}
	seg := pushSegment(20, 8)
	sent := &packetBuffer{}
	c.track(sent, seg)
	sent.releaseSent()
	c.pathRTT = int64(time.Second)
	if !c.queuedDuplicate(seg) || len(c.parked) != 1 {
		t.Fatalf("an early resend was not parked: parked=%d", len(c.parked))
	}
	c.flushParked(monoNow())
	if len(c.out) != 0 {
		t.Fatal("a parked resend went before its round trip was up")
	}
	return c, seg
}

func TestParkedResendGoesOnceALaterSendIsAcknowledged(t *testing.T) {
	c, _ := parkedConn(t)
	time.Sleep(time.Millisecond)
	later := &packetBuffer{}
	c.track(later, pushSegment(21, 8))
	later.releaseSent()
	c.noteAcks(ackSegment(21))
	c.flushParked(monoNow())
	if len(c.out) != 1 || len(c.parked) != 0 {
		t.Fatalf("out=%d parked=%d, want the resend of the lost push queued", len(c.out), len(c.parked))
	}
}

func TestParkedResendIsDroppedWhenItsPushIsAcknowledged(t *testing.T) {
	c, _ := parkedConn(t)
	c.noteAcks(ackSegment(20))
	c.flushParked(monoNow() + int64(time.Hour))
	if len(c.out) != 0 || len(c.parked) != 0 {
		t.Fatalf("out=%d parked=%d, want the acknowledged resend dropped", len(c.out), len(c.parked))
	}
}
