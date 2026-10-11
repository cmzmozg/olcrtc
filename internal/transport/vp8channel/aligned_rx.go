package vp8channel

// ai-generated: the whole file (salvage and fec repair of aligned datagram
// frames, packet by packet).

import (
	"bytes"
	"crypto/subtle"
	"encoding/binary"

	"github.com/pion/rtp"
)

const (
	// alignedRoutes is how many routes a track remembers for chunks whose
	// first packet is lost: a client hears one, a server one per client track.
	alignedRoutes = 4
	// alignedSlots is how many frames a track repairs at once. A frame
	// leaves its slot once all its data is in; an older one waiting on a
	// repair gives its slot up to a new frame, and its late packets are still
	// delivered on their own.
	alignedSlots = 4
)

// alignedRx salvages the aligned datagram frames of one track. The reorder
// buffer hands it every packet before holding it; a chunk it claims is
// delivered at once, gap or not, and marked taken so the in-order drain steps
// over it. Every claimed or rebuilt sequence is taken or behind the drain, so
// a repair or a duplicate is never delivered twice.
type alignedRx struct {
	p       *streamTransport
	reorder *reorderBuffer
	routes  [alignedRoutes]alignedRoute
	frames  [alignedSlots]alignedFrame
	clock   uint64
}

// alignedRoute is a route learned from a first chunk: its tag and epochs.
type alignedRoute struct {
	tag, src, dst uint32
	used          uint64 // clock of the last use, zero for an empty entry
}

// alignedFrame is one frame under repair. acc[s] is the XOR of the bodies of
// group s that are in, so with the parity in and one data chunk missing it is
// that chunk's body.
type alignedFrame struct {
	used          uint64 // clock of the last use, zero for a free slot
	start         uint16 // the sequence number of chunk 0
	tag, src, dst uint32
	n, k, g       int
	have          uint64 // chunk positions received or rebuilt
	dataMask      uint64 // chunk positions holding data
	roles         [maxFramePackets]int8
	dataPos       [maxFramePackets]uint8
	parityPos     [alignedMaxSets]uint8
	missing       [alignedMaxSets]uint8
	acc           [alignedMaxSets][]byte
	accLen        [alignedMaxSets]int
}

// claim takes pkt when it is a chunk of an aligned frame for us and reports
// whether it did; anything else goes the frame assembly way.
func (rx *alignedRx) claim(pkt *rtp.Packet) bool {
	off := vp8DescriptorLen(pkt.Payload)
	if off < 0 || len(pkt.Payload)-off > alignedMaxStride {
		return false
	}
	if vp8PartitionStart(pkt) {
		return rx.claimFirst(pkt.SequenceNumber, pkt.Payload[off:], pkt.Marker)
	}
	return rx.claimNext(pkt.SequenceNumber, pkt.Payload[off:], pkt.Marker)
}

// claimFirst takes chunk 0. One whose length is not the stride it was laid
// out for was cut elsewhere than planned, and goes the frame assembly way to
// handleAlignedFrame.
func (rx *alignedRx) claimFirst(seq uint16, chunk []byte, marker bool) bool {
	n, k, g, ok := alignedFirstShape(chunk, marker)
	if !ok {
		return false
	}
	token, src, dst, valid := parseEpochHeader(chunk)
	if !valid || token != rx.p.bindingToken || !rx.p.admitsDatagram(src, dst) {
		return false
	}
	tag := binary.BigEndian.Uint32(chunk[crcOff:epochHdrLen])
	fresh := rx.learn(tag, src, dst)
	rx.accept(rx.frame(seq, tag, src, dst, n, k), 0, chunk[alignedHead0:])
	if fresh {
		rx.steal(seq, n+g)
	}
	return true
}

// alignedFirstShape reads n, k and the parity count off a first chunk and
// checks that the chunk is what its packet carries: the whole frame when it
// is the only chunk, else exactly the stride.
func alignedFirstShape(chunk []byte, marker bool) (int, int, int, bool) {
	if len(chunk) < alignedHead0 || !bytes.Equal(chunk[epochHdrLen:epochHdrLen+4], alignedMagic[:]) {
		return 0, 0, 0, false
	}
	stride := int(binary.BigEndian.Uint16(chunk[epochHdrLen+4:]))
	n, k := int(chunk[alignedHead0-2]), int(chunk[alignedHead0-1])
	g := parityCount(n, k)
	single := n+g == 1
	ok := alignedShape(n, g) && single == marker && (single || len(chunk) == stride)
	return n, k, g, ok
}

// alignedShape reports whether a frame of n data and g parity chunks is one
// a receiver takes.
func alignedShape(n, g int) bool {
	return n >= 1 && g <= alignedMaxSets && n+g <= maxFramePackets
}

// claimNext takes a chunk past the first when its tag names a route the track
// has learned.
func (rx *alignedRx) claimNext(seq uint16, chunk []byte, marker bool) bool {
	if len(chunk) < alignedHeadN || chunk[0] != alignedChunkMagic {
		return false
	}
	route := rx.route(binary.BigEndian.Uint32(chunk[1:5]))
	pos, n, k := int(chunk[5]), int(chunk[6]), int(chunk[7])
	g := parityCount(n, k)
	if route == nil || !alignedShape(n, g) || pos == 0 || pos >= n+g || (pos == n+g-1) != marker {
		return false
	}
	if !rx.p.admitsDatagram(route.src, route.dst) {
		return false
	}
	start := seq - uint16(pos) //nolint:gosec // pos < maxFramePackets
	rx.accept(rx.frame(start, route.tag, route.src, route.dst, n, k), pos, chunk[alignedHeadN:])
	return true
}

// steal claims the chunks of a frame that arrived before its first one
// taught the track their route, from the packets the reorder buffer holds.
func (rx *alignedRx) steal(start uint16, chunks int) {
	b := rx.reorder
	for pos := 1; pos < chunks; pos++ {
		seq := start + uint16(pos)
		pkt := b.pkts[seq]
		if pkt == nil || !rx.claim(pkt) {
			continue
		}
		delete(b.pkts, seq)
		b.take(seq)
		b.recycle(pkt)
	}
}

// accept delivers a chunk's records, adds its body to its group and rebuilds
// the group's missing data chunk when it can.
func (rx *alignedRx) accept(f *alignedFrame, pos int, body []byte) {
	bit := uint64(1) << pos
	if f.have&bit != 0 {
		return
	}
	f.have |= bit
	role := f.roles[pos]
	set := int(^role)
	if role >= 0 {
		rx.deliver(f, body)
		if f.g == 0 {
			rx.finish(f)
			return
		}
		set = int(role) % f.g
	}
	f.xor(set, body)
	f.missing[set]--
	if f.missing[set] == 1 {
		rx.rebuild(f, set)
	}
	rx.finish(f)
}

// rebuild delivers the one data chunk group set misses, when its parity is in.
func (rx *alignedRx) rebuild(f *alignedFrame, set int) {
	if f.have&(uint64(1)<<f.parityPos[set]) == 0 {
		return
	}
	for d := set; d < f.n; d += f.g {
		pos := f.dataPos[d]
		if f.have&(uint64(1)<<pos) != 0 {
			continue
		}
		f.have |= uint64(1) << pos
		f.missing[set] = 0
		rx.reorder.take(f.start + uint16(pos))
		rx.deliver(f, f.acc[set][:f.accLen[set]])
		return
	}
}

// finish frees f's slot once all its data is in.
func (rx *alignedRx) finish(f *alignedFrame) {
	if f.have&f.dataMask == f.dataMask {
		f.used = 0
	}
}

func (rx *alignedRx) deliver(f *alignedFrame, body []byte) {
	alignedRecords(body, func(record []byte) { rx.p.handleDatagramFrame(f.src, record) })
}

// frame returns the slot of the frame starting at start, taking the least
// recently used one for a new frame.
func (rx *alignedRx) frame(start uint16, tag, src, dst uint32, n, k int) *alignedFrame {
	rx.clock++
	victim := &rx.frames[0]
	for i := range rx.frames {
		f := &rx.frames[i]
		if f.used != 0 && f.start == start && f.tag == tag && f.n == n && f.k == k {
			f.used = rx.clock
			return f
		}
		if f.used < victim.used {
			victim = f
		}
	}
	victim.reset(start, tag, src, dst, n, k)
	victim.used = rx.clock
	return victim
}

// route returns the learned route of tag, nil for none.
func (rx *alignedRx) route(tag uint32) *alignedRoute {
	for i := range rx.routes {
		if r := &rx.routes[i]; r.used != 0 && r.tag == tag {
			rx.clock++
			r.used = rx.clock
			return r
		}
	}
	return nil
}

// learn records a route and reports whether it is new to the track.
func (rx *alignedRx) learn(tag, src, dst uint32) bool {
	if rx.route(tag) != nil {
		return false
	}
	victim := &rx.routes[0]
	for i := range rx.routes {
		if rx.routes[i].used < victim.used {
			victim = &rx.routes[i]
		}
	}
	rx.clock++
	*victim = alignedRoute{tag: tag, src: src, dst: dst, used: rx.clock}
	return true
}

// reset readies the slot for a new frame.
func (f *alignedFrame) reset(start uint16, tag, src, dst uint32, n, k int) {
	for s := range f.g {
		clear(f.acc[s][:f.accLen[s]])
		f.accLen[s] = 0
	}
	f.start, f.tag, f.src, f.dst = start, tag, src, dst
	f.n, f.k, f.g = n, k, parityCount(n, k)
	f.have, f.dataMask = 0, 0
	for pos, role := range alignedRoles(f.roles[:0], n, k, f.g) {
		if role >= 0 {
			f.dataPos[role] = uint8(pos)
			f.dataMask |= uint64(1) << pos
		} else {
			f.parityPos[^role] = uint8(pos)
		}
	}
	for s := range f.g {
		f.missing[s] = uint8((n-s+f.g-1)/f.g + 1) //nolint:gosec // at most maxFramePackets
		if f.acc[s] == nil {
			f.acc[s] = make([]byte, alignedMaxStride)
		}
	}
}

// xor adds a body to group set.
func (f *alignedFrame) xor(set int, body []byte) {
	acc := f.acc[set]
	subtle.XORBytes(acc, acc[:len(body)], body)
	f.accLen[set] = max(f.accLen[set], len(body))
}

// vp8DescriptorLen is the size of the VP8 payload descriptor heading an RTP
// payload (RFC 7741 4.2), -1 when the payload is shorter than it.
func vp8DescriptorLen(payload []byte) int {
	switch {
	case len(payload) == 0:
		return -1
	case payload[0]&0x80 == 0: // no X: no extended control bits
		return 1
	case len(payload) < 2:
		return -1
	}
	ext, n := payload[1], 2
	if ext&0x80 != 0 { // I: picture id, 15 bits with its M bit set
		if len(payload) > n && payload[n]&0x80 != 0 {
			n++
		}
		n++
	}
	if ext&0x40 != 0 { // L: TL0PICIDX
		n++
	}
	if ext&0x30 != 0 { // T or K: TID/KEYIDX
		n++
	}
	if n > len(payload) {
		return -1
	}
	return n
}

// handleAlignedFrame delivers the records of an aligned frame that arrived
// whole through frame assembly: one pion cut elsewhere than its stride, or
// one whose route the track had not learned. Its chunks are cut at the stride
// it was laid out for; the parity is not needed.
func (p *streamTransport) handleAlignedFrame(src uint32, frame []byte) bool {
	if p.legacyWire || len(frame) < alignedHead0 ||
		!bytes.Equal(frame[epochHdrLen:epochHdrLen+4], alignedMagic[:]) {
		return false
	}
	stride := int(binary.BigEndian.Uint16(frame[epochHdrLen+4:]))
	n, k := int(frame[alignedHead0-2]), int(frame[alignedHead0-1])
	g := parityCount(n, k)
	if !alignedShape(n, g) || stride < alignedHead0 {
		return true
	}
	var roles [maxFramePackets]int8
	for pos, role := range alignedRoles(roles[:0], n, k, g) {
		if body := alignedBodyAt(frame, pos, stride); role >= 0 && body != nil {
			alignedRecords(body, func(record []byte) { p.handleDatagramFrame(src, record) })
		}
	}
	return true
}

// alignedBodyAt is the body of chunk pos of a whole frame cut every stride
// bytes, nil when the frame does not hold that chunk.
func alignedBodyAt(frame []byte, pos, stride int) []byte {
	start := pos * stride
	if start >= len(frame) {
		return nil
	}
	chunk := frame[start:min(start+stride, len(frame))]
	if pos == 0 {
		return chunk[alignedHead0:]
	}
	if len(chunk) < alignedHeadN || chunk[0] != alignedChunkMagic || int(chunk[5]) != pos {
		return nil
	}
	return chunk[alignedHeadN:]
}
