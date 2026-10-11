package vp8channel

import (
	"strings"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport/common"
)

// reorderWindow bounds how many out-of-order RTP packets the reorder buffer
// holds while waiting for a gap to fill. Real SFUs reorder within a handful of
// packets; once this many newer packets pile up behind a hole, the missing
// sequence is treated as genuinely lost and we advance, so a truly dropped
// packet cannot stall delivery indefinitely.
const reorderWindow = 256

// reorderHold bounds how long a gap may hold back the packets behind it.
// The count bound alone is reached in a fraction of a second at full rate,
// but a stream down to keepalives and probes - an idle peer, or one whose
// lane is waiting out a dark path - brings a dozen packets a second, and one
// lost packet held control and data behind it for twenty seconds, long
// enough for the sender to count the path dark (issue #12).
//
// It has to outlast a repair, not just reordering: on every provider but
// Jitsi the engine registers pion's default interceptors, whose generator
// asks for a lost packet every 100 ms, so a repaired packet lands a NACK
// interval plus a round trip late. A gap given up sooner than that throws
// the repair away and loses the frame anyway: modelled at 1000 packets a
// second in frames of 17, with 95% of losses repaired, a 100 ms hold put
// frames through 93.9% of the time against 99.2% for the count bound at 1%
// loss and a 40 ms round trip, and 89.4% against 98.3% at 2%. Three hundred
// covers the generator's interval and a round trip of 100 ms with room to
// spare, and still ends the twenty-second holds it was added for.
//
// ai-generated: the time bound.
const reorderHold = 300 * time.Millisecond

// Reordered RTP packets normally carry an MTU-sized payload. Larger buffers
// are not retained after delivery so one malformed packet cannot pin a large
// allocation in the reorder buffer's local free list.
const maxRetainedRTPPayloadCap = 2 * 1024

// maxFramePackets bounds how far takeFrame walks to find a frame's ends: a
// full sample is defaultMaxPayloadSize over RTP packets of about 1.2 KB.
const maxFramePackets = 64

// maxAssembledFrameSize bounds one reassembled VP8 frame. The buffer only
// resets on a start-of-partition bit, a sequence gap or a marker bit, so a
// contiguous run of packets that never sets the marker grows it without limit
// - roughly 1.2 KB per packet, for as long as the peer keeps sending. The cap
// sits well above defaultMaxPayloadSize plus the batching overhead, so no
// legitimate frame reaches it.
const maxAssembledFrameSize = 4 * defaultMaxPayloadSize

// seqLess reports whether RTP sequence a precedes b using wrap-around aware
// comparison (RFC 1982 serial arithmetic on uint16).
func seqLess(a, b uint16) bool {
	// bit15 of the wrap-around difference is the serial sign bit: set means a
	// precedes b. Avoids a signed conversion gosec flags as overflow.
	return (a-b)&0x8000 != 0
}

// reorderBuffer restores RTP sequence order before frame assembly. The SFU may
// deliver packets out of order or drop them; feeding that stream straight into
// the strict contiguity check in processRTPPacket made every reorder look like
// loss and discarded whole frames (issue #95: ~80-90% of VP8 frames dropped on
// a live SFU). Buffering by sequence number and draining in order means only
// genuine loss produces a gap.
type reorderBuffer struct {
	pkts    map[uint16]*rtp.Packet
	free    []*rtp.Packet
	nextSeq uint16
	started bool
	// gapSince is when the packets now held started waiting on a gap, zero
	// while none waits (ai-generated: issue #12).
	gapSince time.Time
	// ahead, when set, takes a whole frame held behind a gap at once, in
	// order; taken marks its sequence numbers for drain to step over, and
	// run is takeFrame's scratch. KCP orders its segments itself, so a lost
	// packet costs only its own frame, not a hold on every frame behind it:
	// the acks and the repeats that repair the loss among them.
	//
	// ai-generated: ahead, taken and run (frames past a gap).
	ahead func([]*rtp.Packet)
	taken map[uint16]struct{}
	run   []*rtp.Packet
	// claim, when set, sees every packet before it is held and takes the
	// chunks of aligned datagram frames, which it delivers itself; drain
	// steps over them (ai-generated: aligned frames).
	claim func(*rtp.Packet) bool
}

func newReorderBuffer() *reorderBuffer {
	return &reorderBuffer{
		pkts:  make(map[uint16]*rtp.Packet, reorderWindow),
		taken: make(map[uint16]struct{}),
	}
}

// push adds pkt and synchronously delivers packets now available in strict
// sequence order. The caller reuses its read buffer, so buffered payloads are
// copied into reorder-owned storage and recycled immediately after deliver
// returns. deliver must not retain the packet.
//
// ai-generated: hold, which lets claim take aligned chunks.
func (b *reorderBuffer) push(pkt *rtp.Packet, deliver func(*rtp.Packet)) {
	if !b.started {
		b.started = true
		b.nextSeq = pkt.SequenceNumber
	}
	// Drop packets older than our current position: already delivered, or
	// skipped past as lost.
	if seqLess(pkt.SequenceNumber, b.nextSeq) {
		return
	}
	if _, done := b.taken[pkt.SequenceNumber]; done {
		return // a repair of a frame already delivered ahead
	}
	b.hold(pkt)

	// Holding a full window behind a hole, or holding anything behind it for
	// longer than reorderHold, means the head sequence is genuinely lost:
	// skip forward to the oldest buffered packet.
	now := time.Now()
	if b.held() > reorderWindow || (!b.gapSince.IsZero() && now.Sub(b.gapSince) > reorderHold) {
		b.skipToOldest()
		b.gapSince = time.Time{}
	}
	b.drain(deliver)
	if _, waiting := b.pkts[pkt.SequenceNumber]; waiting && b.ahead != nil {
		if run := b.takeFrame(pkt.SequenceNumber); run != nil {
			b.ahead(run)
			for _, taken := range run {
				b.recycle(taken)
			}
		}
	}
	switch {
	case b.held() == 0:
		b.gapSince = time.Time{}
	case b.gapSince.IsZero():
		b.gapSince = now
	}
}

// hold stores pkt for drain, or marks it taken when claim delivered it.
//
// ai-generated: the whole function (aligned frames).
func (b *reorderBuffer) hold(pkt *rtp.Packet) {
	seq := pkt.SequenceNumber
	if old := b.pkts[seq]; old != nil {
		delete(b.pkts, seq)
		b.recycle(old)
	}
	if b.claim != nil && b.claim(pkt) {
		b.take(seq)
		return
	}
	b.pkts[seq] = b.clone(pkt)
}

// take marks seq delivered out of band, for drain to step over; a copy held
// for drain is dropped. A sequence behind the drain needs no mark.
//
// ai-generated: the whole function (aligned frames).
func (b *reorderBuffer) take(seq uint16) {
	if seqLess(seq, b.nextSeq) {
		return
	}
	if held := b.pkts[seq]; held != nil {
		delete(b.pkts, seq)
		b.recycle(held)
	}
	if seq == b.nextSeq { // in order: drain goes on from the next one
		b.nextSeq++
		return
	}
	b.taken[seq] = struct{}{}
}

// held counts what waits on the gap, frames already taken ahead included.
func (b *reorderBuffer) held() int {
	return len(b.pkts) + len(b.taken)
}

// takeFrame returns, in order, the packets of the frame seq belongs to when
// all of them are held, from the start of its partition to the marker, and
// marks them taken; nil while any is missing.
//
// ai-generated: the whole function (frames past a gap).
func (b *reorderBuffer) takeFrame(seq uint16) []*rtp.Packet {
	start := seq
	for n := 0; !vp8PartitionStart(b.pkts[start]); n++ {
		if n >= maxFramePackets || b.pkts[start-1] == nil {
			return nil
		}
		start--
	}
	b.run = b.run[:0]
	for next := start; ; next++ {
		pkt := b.pkts[next]
		if pkt == nil || len(b.run) >= maxFramePackets {
			return nil
		}
		b.run = append(b.run, pkt)
		if pkt.Marker {
			break
		}
	}
	for _, pkt := range b.run {
		delete(b.pkts, pkt.SequenceNumber)
		b.taken[pkt.SequenceNumber] = struct{}{}
	}
	return b.run
}

// vp8PartitionStart reports whether pkt carries the S bit of its VP8
// payload descriptor: the first packet of a frame this transport sends.
func vp8PartitionStart(pkt *rtp.Packet) bool {
	return pkt != nil && len(pkt.Payload) > 0 && pkt.Payload[0]&0x10 != 0
}

// drain pops contiguous packets starting at nextSeq, stepping over those
// already delivered ahead.
func (b *reorderBuffer) drain(deliver func(*rtp.Packet)) {
	for {
		if _, done := b.taken[b.nextSeq]; done {
			delete(b.taken, b.nextSeq)
			b.nextSeq++
			continue
		}
		pkt, ok := b.pkts[b.nextSeq]
		if !ok {
			return
		}
		delete(b.pkts, b.nextSeq)
		b.nextSeq++
		deliver(pkt)
		b.recycle(pkt)
	}
}

// ai-generated: retain only the RTP fields needed for VP8 assembly, without parser-owned slices.
func (b *reorderBuffer) clone(pkt *rtp.Packet) *rtp.Packet {
	var clone *rtp.Packet
	if last := len(b.free) - 1; last >= 0 {
		clone = b.free[last]
		b.free = b.free[:last]
	} else {
		clone = &rtp.Packet{}
	}
	clone.Header = rtp.Header{SequenceNumber: pkt.SequenceNumber, Marker: pkt.Marker}
	if cap(clone.Payload) < len(pkt.Payload) {
		clone.Payload = make([]byte, len(pkt.Payload))
	} else {
		clone.Payload = clone.Payload[:len(pkt.Payload)]
	}
	copy(clone.Payload, pkt.Payload)
	return clone
}

func (b *reorderBuffer) recycle(pkt *rtp.Packet) {
	pkt.Header = rtp.Header{}
	if cap(pkt.Payload) > maxRetainedRTPPayloadCap {
		pkt.Payload = nil
	} else {
		pkt.Payload = pkt.Payload[:0]
	}
	b.free = append(b.free, pkt)
}

// skipToOldest advances nextSeq to the lowest buffered sequence, abandoning a
// lost packet so drain can make progress. Taken sequences count, so none is
// skipped past and left behind.
func (b *reorderBuffer) skipToOldest() {
	first := true
	var oldest uint16
	for seq := range b.pkts {
		if first || seqLess(seq, oldest) {
			oldest = seq
			first = false
		}
	}
	for seq := range b.taken {
		if first || seqLess(seq, oldest) {
			oldest = seq
			first = false
		}
	}
	b.nextSeq = oldest
}

type vp8FrameState struct {
	vp8Pkt      codecs.VP8Packet
	frameBuf    []byte
	lastSeq     uint16
	haveLastSeq bool
	frameValid  bool
}

// processRTPPacket returns a complete VP8 frame payload when fully assembled,
// nil otherwise. The result remains valid until the next call. Detects packet
// loss/reordering to avoid silently corrupting fragmented VP8 frames.
func (s *vp8FrameState) processRTPPacket(pkt *rtp.Packet) []byte {
	if s.haveLastSeq && pkt.SequenceNumber != s.lastSeq+1 {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
	}
	s.lastSeq = pkt.SequenceNumber
	s.haveLastSeq = true

	vp8Payload, err := s.vp8Pkt.Unmarshal(pkt.Payload)
	if err != nil {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
		return nil
	}

	if s.vp8Pkt.S == 1 {
		s.frameBuf = s.frameBuf[:0]
		s.frameValid = true
	}

	if !s.frameValid {
		return nil
	}

	if len(s.frameBuf)+len(vp8Payload) > maxAssembledFrameSize {
		// A frame this large is not something the writer can produce, so the
		// marker bit is never coming. Drop what we have and wait for the next
		// start-of-partition rather than growing forever.
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
		return nil
	}

	s.frameBuf = append(s.frameBuf, vp8Payload...)

	if !pkt.Marker {
		return nil
	}

	frame := s.frameBuf
	s.frameBuf = s.frameBuf[:0]
	s.frameValid = false
	if len(frame) >= epochHdrLen {
		return frame
	}
	return nil
}

func (p *streamTransport) handleRemoteTrack(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	switch {
	case track.Codec().MimeType == webrtc.MimeTypeVP8:
	case strings.EqualFold(track.Codec().MimeType, mimeTypeRED):
		// A VK Calls SFU forwards camera video wrapped in RFC 2198 RED even
		// when the publisher sent no redundancy; the primary block is the
		// VP8 packet this transport assembles.
	default:
		go p.drainTrack(track)
		return
	}

	// We don't reset KCP here. Peer restarts are detected by the epoch
	// header on incoming frames, which works even when the SFU keeps
	// forwarding the same track across our restarts.
	go p.readVP8Track(track)
}

func (p *streamTransport) drainTrack(track *webrtc.TrackRemote) {
	buf := make([]byte, rtpBufSize)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
	}
}

// mimeTypeRED is the RFC 2198 wrapper a track can arrive in; see
// common.REDPrimary.
const mimeTypeRED = "video/red"

// trackReader is what one remote track's reader keeps: the reorder buffer,
// frame assembly in order and past a gap, and the aligned datagram frames it
// salvages packet by packet.
//
// ai-generated: the whole type, split out of readVP8Track (aligned frames).
type trackReader struct {
	p            *streamTransport
	reorder      *reorderBuffer
	state, ahead vp8FrameState
	aligned      alignedRx
	frames       int
	ordered      func(*rtp.Packet)
}

// newTrackReader builds p's reader state for one track.
//
// ai-generated: the whole function.
func newTrackReader(p *streamTransport) *trackReader {
	r := &trackReader{p: p, reorder: newReorderBuffer()}
	r.reorder.ahead = r.deliverAhead
	r.ordered = r.deliverOrdered
	if !p.legacyWire {
		r.aligned = alignedRx{p: p, reorder: r.reorder}
		r.reorder.claim = r.aligned.claim
	}
	return r
}

// handleRTP takes one RTP packet of the track. Sequence order is restored
// before assembly so SFU reordering is not mistaken for loss.
//
// ai-generated: the whole function.
func (r *trackReader) handleRTP(pkt *rtp.Packet) {
	r.reorder.push(pkt, r.ordered)
}

// deliverOrdered assembles frames from the packets drain hands on in order.
//
// ai-generated: the whole function, split out of readVP8Track.
func (r *trackReader) deliverOrdered(pkt *rtp.Packet) {
	if frame := r.state.processRTPPacket(pkt); frame != nil {
		r.frames++
		r.p.handleIncomingFrame(frame)
	}
}

// deliverAhead assembles a whole frame taken past a gap.
//
// ai-generated: the whole function, split out of readVP8Track.
func (r *trackReader) deliverAhead(run []*rtp.Packet) {
	for _, pkt := range run {
		if frame := r.ahead.processRTPPacket(pkt); frame != nil {
			r.p.handleIncomingFrame(frame)
		}
	}
}

// ai-generated: reuse the RTP parser and its header extension storage between
// reads; the per-track state moved to trackReader.
func (p *streamTransport) readVP8Track(track *webrtc.TrackRemote) {
	red := strings.EqualFold(track.Codec().MimeType, mimeTypeRED)
	reader := newTrackReader(p)
	var pkt rtp.Packet
	buf := make([]byte, rtpBufSize)
	var rtpCount int

	for {
		n, _, err := track.Read(buf)
		if err != nil {
			logger.Infof("vp8channel: readVP8Track closed track=%s rtp=%d frames=%d err=%v",
				track.ID(), rtpCount, reader.frames, err)
			return
		}
		rtpCount++

		if pkt.Unmarshal(buf[:n]) != nil {
			continue
		}
		if red {
			payloadType, data, unwrapErr := common.REDPrimary(pkt.Payload)
			if unwrapErr != nil {
				continue
			}
			pkt.PayloadType = payloadType
			pkt.Payload = data
		}
		reader.handleRTP(&pkt)
	}
}
