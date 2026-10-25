package vp8channel

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

// ai-generated: refresh parked KCP headers and deadlines in owned storage, then release it on ACK.
func TestParkedResendReusesFreshOwnedBytes(t *testing.T) {
	c := &kcpConn{sentAt: map[uint32]int64{7: monoNow()}, pathRTT: int64(time.Second)}
	seg := pushSegment(7, 64)
	if !c.queuedDuplicate(seg) {
		t.Fatal("first early resend was not parked")
	}
	first := c.parked[7]
	binary.LittleEndian.PutUint32(seg[8:12], 42)
	binary.LittleEndian.PutUint32(seg[16:20], 5)
	c.sentAt[7]++
	if !c.queuedDuplicate(seg) {
		t.Fatal("repeated early resend was not parked")
	}
	current := c.parked[7]
	if &first.data[0] != &current.data[0] || current.due != first.due+1 {
		t.Fatal("parked resend did not reuse storage and refresh its deadline")
	}
	if binary.LittleEndian.Uint32(current.data[8:12]) != 42 || binary.LittleEndian.Uint32(current.data[16:20]) != 5 {
		t.Fatal("parked resend kept a stale TS or UNA")
	}
	seg[8] = 99
	if binary.LittleEndian.Uint32(current.data[8:12]) != 42 {
		t.Fatal("parked resend retained caller-owned data")
	}
	c.noteAcks(ackSegment(7))
	if len(c.parked) != 0 {
		t.Fatal("ACK kept the parked payload")
	}
}

// ai-generated: flush the newest parked bytes and remove the map's ownership before queueing.
func TestParkedResendFlushesNewestCopy(t *testing.T) {
	c, seg := parkedConn(t)
	out := make(chan *packetBuffer, 4)
	c.out = out
	binary.LittleEndian.PutUint32(seg[8:12], 42)
	if !c.queuedDuplicate(seg) {
		t.Fatal("repeated early resend was not parked")
	}
	c.flushParked(monoNow() + int64(time.Hour))
	if len(c.out) != 1 || len(c.parked) != 0 {
		t.Fatal("flush kept a parked entry or lost its payload")
	}
	packet := <-out
	defer packet.release()
	if binary.LittleEndian.Uint32(packet.data[epochHdrLen+8:epochHdrLen+12]) != 42 {
		t.Fatal("flush queued a stale KCP header")
	}
}

// ai-generated: quantify first and repeated early-resend parking without changing the KCP timers.
func BenchmarkParkedResendStorage(b *testing.B) {
	for _, calls := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("copies=%d", calls), func(b *testing.B) {
			c := &kcpConn{sentAt: map[uint32]int64{7: 1}, pathRTT: int64(time.Second)}
			seg := pushSegment(7, kcpMTU-24)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				delete(c.parked, 7)
				c.sentAt[7] = monoNow()
				for range calls {
					if !c.queuedDuplicate(seg) {
						b.Fatal("early resend was not parked")
					}
				}
			}
		})
	}
}
