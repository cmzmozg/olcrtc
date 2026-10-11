package olt

// Wire format, version 1. Every packet is
//
//	version  1 byte, 0x01
//	conn id  8 bytes, big endian
//	number   varint, the packet number; never reused, one per packet
//	frames   one or more, to the end of the packet
//
// A varint is QUIC's variable-length integer: the top two bits of the first
// byte give its length (1, 2, 4 or 8 bytes), the rest is the value, big
// endian. Every frame starts with a type byte:
//
//	0x00 PADDING          -
//	0x01 PING             -
//	0x02 ACK              largest, delay (us), extra ranges, first range,
//	                      then (gap, range) pairs counted down as in QUIC
//	0x03 RESET            stream id, code, final size
//	0x04 STOP             stream id, code
//	0x05 MAX_DATA         limit
//	0x06 MAX_STREAM_DATA  stream id, limit
//	0x07 CLOSE            code
//	0x08 STREAM           stream id, offset, length, data
//	0x09 STREAM + FIN     the same, and the data ends the stream
//
// Every frame but PADDING and ACK asks for an acknowledgement. A stream
// opens on the first frame that names it. Stream ids fit 32 bits: the
// client opens odd ones, the server even ones.

import (
	"encoding/binary"
	"math"
)

const (
	version = 0x01

	frPadding       = 0x00
	frPing          = 0x01
	frAck           = 0x02
	frReset         = 0x03
	frStop          = 0x04
	frMaxData       = 0x05
	frMaxStreamData = 0x06
	frClose         = 0x07
	frStream        = 0x08
	frStreamFin     = 0x09

	// headerMax is the longest packet header.
	headerMax = 1 + 8 + 8
	// maxVarint is the largest value a varint carries.
	maxVarint = 1<<62 - 1
	// maxAckDecode bounds the ranges one ACK frame may carry.
	maxAckDecode = 256
	// ackFixed and ackPerRange are the worst-case ACK frame sizes, for
	// sizing how many ranges an ACK may report.
	ackFixed    = 1 + 4*8
	ackPerRange = 2 * 8
	// streamHeaderMax is the longest STREAM frame header with a two-byte
	// length.
	streamHeaderMax = 1 + 4 + 8 + 2
)

// frame is one decoded frame. Its fields mean what the frame type says: a
// carries the offset, code or limit, b the final size or the ACK delay.
type frame struct {
	typ    byte
	id     uint32
	a, b   uint64
	data   []byte
	r0, r1 int // the ACK's ranges in inPacket.ranges
}

// inPacket is a decoded packet. Its slices point into the packet and into
// buffers it reuses.
type inPacket struct {
	pn     uint64
	elicit bool
	frames []frame
	ranges []span
}

// reader reads varints off a frame; bad sticks once a read fails.
type reader struct {
	b   []byte
	bad bool
}

// decode parses p into pk. It never panics, whatever p holds.
//
// ai-generated: whole function.
func (pk *inPacket) decode(p []byte, connID uint64) error {
	if len(p) < 1+8+1 || p[0] != version {
		return ErrMalformed
	}
	if binary.BigEndian.Uint64(p[1:9]) != connID {
		return ErrWrongConn
	}
	r := reader{b: p[9:]}
	pk.pn = r.varint()
	pk.elicit = false
	pk.frames = pk.frames[:0]
	pk.ranges = pk.ranges[:0]
	if r.bad || len(r.b) == 0 {
		return ErrMalformed
	}
	for len(r.b) > 0 {
		fr := frame{typ: r.b[0]}
		r.b = r.b[1:]
		if !pk.decodeFrame(&r, &fr) || r.bad {
			return ErrMalformed
		}
		switch fr.typ {
		case frPadding:
			continue
		case frAck:
		default:
			pk.elicit = true
		}
		pk.frames = append(pk.frames, fr)
	}
	return nil
}

// decodeFrame reads the body of fr. It reports false for an unknown type.
//
// ai-generated: whole function.
func (pk *inPacket) decodeFrame(r *reader, fr *frame) bool {
	switch fr.typ {
	case frPadding, frPing:
	case frAck:
		pk.decodeAck(r, fr)
	case frReset:
		fr.id, fr.a, fr.b = r.id(), r.varint(), r.varint()
	case frStop, frMaxStreamData:
		fr.id, fr.a = r.id(), r.varint()
	case frMaxData, frClose:
		fr.a = r.varint()
	case frStream, frStreamFin:
		fr.id, fr.a = r.id(), r.varint()
		fr.data = r.bytes(r.varint())
		if fr.a+u64(len(fr.data)) > maxVarint {
			r.bad = true
		}
	default:
		return false
	}
	return true
}

// decodeAck reads an ACK body into fr and its ranges into pk.ranges, newest
// first, as half-open spans.
//
// ai-generated: whole function.
func (pk *inPacket) decodeAck(r *reader, fr *frame) {
	fr.a, fr.b = r.varint(), r.varint()
	count, first := r.varint(), r.varint()
	if r.bad || count > maxAckDecode || first > fr.a {
		r.bad = true
		return
	}
	fr.r0 = len(pk.ranges)
	lo := fr.a - first
	pk.ranges = append(pk.ranges, span{lo, fr.a + 1})
	for range count {
		gap, n := r.varint(), r.varint()
		if r.bad || lo < gap+2 || lo-gap-2 < n {
			r.bad = true
			return
		}
		hi := lo - gap - 2
		lo = hi - n
		pk.ranges = append(pk.ranges, span{lo, hi + 1})
	}
	fr.r1 = len(pk.ranges)
}

// varint reads one varint.
//
// ai-generated: whole function.
func (r *reader) varint() uint64 {
	if r.bad || len(r.b) == 0 {
		r.bad = true
		return 0
	}
	n := 1 << (r.b[0] >> 6)
	if len(r.b) < n {
		r.bad = true
		return 0
	}
	v := uint64(r.b[0] & 0x3f)
	for _, c := range r.b[1:n] {
		v = v<<8 | uint64(c)
	}
	r.b = r.b[n:]
	return v
}

// id reads a stream id.
//
// ai-generated: whole function.
func (r *reader) id() uint32 {
	v := r.varint()
	if v > math.MaxUint32 {
		r.bad = true
		return 0
	}
	return uint32(v)
}

// bytes reads n bytes.
//
// ai-generated: whole function.
func (r *reader) bytes(n uint64) []byte {
	if r.bad || n > u64(len(r.b)) {
		r.bad = true
		return nil
	}
	d := r.b[:n]
	r.b = r.b[n:]
	return d
}

// varintLen is the encoded size of v.
//
// ai-generated: whole function.
func varintLen(v uint64) int {
	switch {
	case v < 1<<6:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<30:
		return 4
	default:
		return 8
	}
}

// appendVarint appends v, which must not exceed maxVarint.
//
// ai-generated: whole function.
func appendVarint(b []byte, v uint64) []byte {
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], v)
	n := varintLen(v)
	switch n {
	case 2:
		t[6] |= 0x40
	case 4:
		t[4] |= 0x80
	case 8:
		t[0] |= 0xc0
	}
	return append(b, t[8-n:]...)
}

// appendHeader appends a packet header.
//
// ai-generated: whole function.
func appendHeader(b []byte, connID, pn uint64) []byte {
	b = append(b, version)
	b = binary.BigEndian.AppendUint64(b, connID)
	return appendVarint(b, pn)
}

// appendStreamFrame appends a STREAM frame. Its length always takes two
// bytes, as streamOverhead counts.
//
// ai-generated: whole function.
func appendStreamFrame(b []byte, id uint32, off uint64, data []byte, fin bool) []byte {
	typ := byte(frStream)
	if fin {
		typ = frStreamFin
	}
	b = append(b, typ)
	b = appendVarint(b, uint64(id))
	b = appendVarint(b, off)
	b = binary.BigEndian.AppendUint16(b, uint16(len(data))|0x4000) //nolint:gosec // G115: len < maxPacket < 1<<14
	return append(b, data...)
}

// streamOverhead is the size of a STREAM frame header at off.
//
// ai-generated: whole function.
func streamOverhead(id uint32, off uint64) int {
	return 1 + varintLen(uint64(id)) + varintLen(off) + 2
}

// appendIDFrame appends a frame of a type, a stream id and values.
//
// ai-generated: whole function.
func appendIDFrame(b []byte, typ byte, id uint32, vals ...uint64) []byte {
	b = append(b, typ)
	b = appendVarint(b, uint64(id))
	for _, v := range vals {
		b = appendVarint(b, v)
	}
	return b
}

// idFrameLen is the size appendIDFrame gives the same arguments.
//
// ai-generated: whole function.
func idFrameLen(id uint32, vals ...uint64) int {
	n := 1 + varintLen(uint64(id))
	for _, v := range vals {
		n += varintLen(v)
	}
	return n
}
