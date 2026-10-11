package vp8channel

// ai-generated: the whole file (rtp-aligned datagram frames with xor fec).

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// rtpCapture is the write stream of a track bound without a PeerConnection:
// every RTP packet pion's packetizer cuts goes to sink.
type rtpCapture struct{ sink func(*rtp.Packet) }

func (c *rtpCapture) WriteRTP(h *rtp.Header, payload []byte) (int, error) {
	c.sink(&rtp.Packet{Header: *h, Payload: append([]byte(nil), payload...)})
	return len(payload), nil
}

func (c *rtpCapture) Write(b []byte) (int, error) { return len(b), nil }

// captureContext binds a track the way an RTPSender does, for VP8 only.
type captureContext struct{ w *rtpCapture }

func (c *captureContext) CodecParameters() []webrtc.RTPCodecParameters {
	return []webrtc.RTPCodecParameters{{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		PayloadType:        96,
	}}
}
func (c *captureContext) HeaderExtensions() []webrtc.RTPHeaderExtensionParameter { return nil }
func (c *captureContext) SSRC() webrtc.SSRC                                      { return 1 }
func (c *captureContext) SSRCRetransmission() webrtc.SSRC                        { return 0 }
func (c *captureContext) SSRCForwardErrorCorrection() webrtc.SSRC                { return 0 }
func (c *captureContext) WriteStream() webrtc.TrackLocalWriter                   { return c.w }
func (c *captureContext) ID() string                                             { return "capture" }
func (c *captureContext) RTCPReader() interceptor.RTCPReader                     { return nil }

// bindCapture gives tr the track New builds, bound so that pion's own
// packetizer cuts every sample tr writes into RTP packets for sink.
func bindCapture(tb testing.TB, tr *streamTransport, sink func(*rtp.Packet)) {
	tb.Helper()
	vp8 := newVP8Payloader()
	track, err := newVP8Track(vp8)
	if err != nil {
		tb.Fatalf("newVP8Track: %v", err)
	}
	if _, err := track.Bind(&captureContext{w: &rtpCapture{sink: sink}}); err != nil {
		tb.Fatalf("Bind: %v", err)
	}
	tr.track, tr.vp8, tr.sampleWriter = track, vp8, nil
}

// pictureID reads the picture id off a VP8 descriptor pion wrote; one with
// no picture id is pion's id 0.
func pictureID(payload []byte) uint32 {
	if payload[0]&0x80 == 0 || payload[1]&0x80 == 0 {
		return 0
	}
	if payload[2]&0x80 == 0 {
		return uint32(payload[2])
	}
	return uint32(payload[2]&0x7f)<<8 | uint32(payload[3])
}

// alignedLink is a transport writing aligned frames to its peer's track
// reader through pion's real packetizer.
type alignedLink struct {
	tx, rx *streamTransport
	w      *writerState
	reader *trackReader
	pkts   []*rtp.Packet // what the last writes cut, in order
	mu     sync.Mutex
	got    map[uint32]int // deliveries per record id
	bad    int            // deliveries whose bytes were not the record's
	count  bool           // whether deliveries are counted
}

func newAlignedLink(tb testing.TB) *alignedLink {
	tb.Helper()
	l := &alignedLink{got: map[uint32]int{}, count: true}
	l.tx = newDatagramTestTransport(transport.Config{}, Options{BatchSize: 64})
	l.rx = newDatagramTestTransport(transport.Config{
		OnDatagram: func(data []byte) {
			if !l.count {
				return
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			id := binary.BigEndian.Uint32(data)
			l.got[id]++
			if !bytes.Equal(data, record(id, len(data))) {
				l.bad++
			}
		},
	}, Options{})
	l.rx.localEpoch = 0x200
	l.rx.peerEpoch.Store(0x100)
	l.rx.peerConfirmed.Store(true)
	l.tx.peerEpoch.Store(0x200)
	l.tx.alignedPeer.Store(0x200)
	bindCapture(tb, l.tx, func(pkt *rtp.Packet) { l.pkts = append(l.pkts, pkt) })
	l.w = newWriterState(l.tx)
	l.reader = newTrackReader(l.rx)
	return l
}

// record is datagram id of size bytes: the id, then bytes that follow from it.
func record(id uint32, size int) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b, id)
	for i := 4; i < size; i++ {
		b[i] = byte(id*31 + uint32(i)) //nolint:gosec // test pattern
	}
	return b
}

// send queues the records and lets the writer cut them into samples.
func (l *alignedLink) send(tb testing.TB, ids []uint32, sizes []int) {
	tb.Helper()
	l.pkts = l.pkts[:0]
	for i, id := range ids {
		if err := l.tx.SendDatagram(record(id, sizes[i])); err != nil {
			tb.Fatalf("SendDatagram: %v", err)
		}
	}
	if !l.w.drainDatagram() {
		tb.Fatal("drainDatagram did not write every sample")
	}
}

// deliveries returns and clears what the receiver got.
func (l *alignedLink) deliveries() map[uint32]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	got := l.got
	l.got = map[uint32]int{}
	return got
}

// sentFrame is one aligned sample as pion cut it.
type sentFrame struct {
	pkts  []*rtp.Packet
	n, g  int
	roles []int8
	ids   [][]uint32 // record ids per chunk position
}

// frames splits the last writes into samples and reads each chunk on its own,
// checking that every RTP payload is exactly one whole chunk.
func (l *alignedLink) frames(tb testing.TB) []sentFrame {
	tb.Helper()
	var out []sentFrame
	start := 0
	for i, pkt := range l.pkts {
		if pkt.Marker {
			out = append(out, readSentFrame(tb, l.pkts[start:i+1]))
			start = i + 1
		}
	}
	return out
}

func readSentFrame(tb testing.TB, pkts []*rtp.Packet) sentFrame {
	tb.Helper()
	first := pkts[0].Payload[vp8DescriptorLen(pkts[0].Payload):]
	if !bytes.Equal(first[epochHdrLen:epochHdrLen+4], alignedMagic[:]) {
		tb.Fatalf("sample is not an aligned frame: %q", first[epochHdrLen:epochHdrLen+4])
	}
	f := sentFrame{pkts: pkts, n: int(first[alignedHead0-2])}
	k := int(first[alignedHead0-1])
	f.g = parityCount(f.n, k)
	f.roles = alignedRoles(nil, f.n, k, f.g)
	if len(pkts) != len(f.roles) {
		tb.Fatalf("pion cut %d packets, the frame has %d chunks", len(pkts), len(f.roles))
	}
	stride := int(binary.BigEndian.Uint16(first[epochHdrLen+4:]))
	for pos, pkt := range pkts {
		chunk := pkt.Payload[vp8DescriptorLen(pkt.Payload):]
		if pos < len(pkts)-1 && len(chunk) != stride {
			tb.Fatalf("chunk %d is %d bytes, want the stride %d", pos, len(chunk), stride)
		}
		head := alignedHead0
		if pos > 0 {
			head = alignedHeadN
			if chunk[0] != alignedChunkMagic || int(chunk[5]) != pos || int(chunk[6]) != f.n {
				tb.Fatalf("chunk %d head % x is not its own", pos, chunk[:alignedHeadN])
			}
		}
		var ids []uint32 // a parity body holds no records
		if body := chunk[head:]; f.roles[pos] >= 0 {
			alignedRecords(body, func(rec []byte) { ids = append(ids, binary.BigEndian.Uint32(rec)) })
			assertOnlyRecords(tb, pos, body)
		}
		f.ids = append(f.ids, ids)
	}
	return f
}

// assertOnlyRecords checks that a data body is records, then zeros: nothing
// of a record spills past its packet.
func assertOnlyRecords(tb testing.TB, pos int, body []byte) {
	tb.Helper()
	used := 0
	alignedRecords(body, func(rec []byte) { used += batchLenLen + len(rec) })
	for i, c := range body[used:] {
		if c != 0 {
			tb.Fatalf("chunk %d: byte %d past its records is %#x, a record straddles its packet", pos, used+i, c)
		}
	}
}

// recoverable reports whether the data chunk at pos comes through when the
// packets in lost are lost: it arrives, or it is its group's only lost data
// chunk and the group's parity arrives.
func (f *sentFrame) recoverable(pos int, lost map[int]bool) bool {
	if !lost[pos] {
		return true
	}
	if f.g == 0 {
		return false
	}
	set := int(f.roles[pos]) % f.g
	for p, role := range f.roles {
		if p == pos || !lost[p] {
			continue
		}
		if (role >= 0 && int(role)%f.g == set) || (role < 0 && int(^role) == set) {
			return false
		}
	}
	return true
}

// TestAlignedLayoutMatchesPionPacketizer writes aligned frames through the
// track New builds, bound to pion's own packetizer, from picture ids next to
// every descriptor size change: 0, 1 to 127, 128 on, and the wrap at 0x8000.
// Each write cuts three or more samples, so the stride changes between the
// samples of one write too. Every RTP payload must be exactly one chunk, no
// record may cross one, and the payloader's count must match the picture id
// and the descriptor pion wrote on every packet.
func TestAlignedLayoutMatchesPionPacketizer(t *testing.T) {
	l := newAlignedLink(t)
	var mismatches int
	aligned := map[uint32]bool{} // picture ids aligned samples went out at
	bindCapture(t, l.tx, func(pkt *rtp.Packet) {
		used := (l.tx.vp8.next.Load() + 0x7fff) & 0x7fff
		desc := vp8DescriptorLen(pkt.Payload)
		if chunk := pkt.Payload[desc:]; vp8PartitionStart(pkt) && len(chunk) > alignedHead0 &&
			bytes.Equal(chunk[epochHdrLen:epochHdrLen+4], alignedMagic[:]) {
			aligned[used] = true
		}
		if pictureID(pkt.Payload) != used || desc != rtpPacketMTU-rtpHeaderLen-strideAt(used) {
			mismatches++
		}
		l.pkts = append(l.pkts, pkt)
	})
	rng := rand.New(rand.NewPCG(7, 7)) //nolint:gosec // reproducible sizes
	id := uint32(0)
	for _, at := range []uint32{0, 126, 1000, 32765, 127} {
		for l.tx.vp8.next.Load() != at {
			l.tx.writeKeepalive()
		}
		count := 150 + rng.IntN(40)
		sent, sizes := make([]uint32, count), make([]int, count)
		for i := range sent {
			id++
			sent[i], sizes[i] = id, 600+rng.IntN(MaxAlignedDatagramSize-599)
			if rng.IntN(4) == 0 {
				sizes[i] = 4 + rng.IntN(60)
			}
		}
		l.send(t, sent, sizes)
		frames := l.frames(t)
		var carried []uint32
		for _, f := range frames {
			for _, chunk := range f.ids {
				carried = append(carried, chunk...)
			}
		}
		slices.Sort(carried)
		if len(frames) < 3 || !slices.Equal(carried, sent) {
			t.Fatalf("picture id %d: %d frames carry %d records, want each of %d once", at, len(frames), len(carried), count)
		}
	}
	for _, used := range []uint32{0, 1, 2, 126, 127, 128, 32766, 32767} {
		if !aligned[used] {
			t.Fatalf("no aligned sample went out at picture id %d", used)
		}
	}
	if mismatches != 0 {
		t.Fatalf("%d packets disagree with the payloader's picture id or descriptor size", mismatches)
	}
}

// strideAt is the stride of a sample cut at picture id used.
func strideAt(used uint32) int {
	v := &vp8Payloader{}
	v.next.Store(used)
	return v.stride()
}

// TestAlignedConstantsFit checks the frame bounds against each other.
func TestAlignedConstantsFit(t *testing.T) {
	g := parityCount(alignedMaxData, alignedFECGroup)
	if chunks := alignedMaxData + g; chunks > maxFramePackets || chunks*alignedMaxStride > defaultMaxPayloadSize {
		t.Fatalf("a full frame is %d chunks, %d bytes", chunks, chunks*alignedMaxStride)
	}
	if g > alignedMaxSets {
		t.Fatalf("a full frame has %d parity chunks, a receiver takes %d", g, alignedMaxSets)
	}
	if alignedHead0+batchLenLen+MaxAlignedDatagramSize != alignedMinStride {
		t.Fatalf("the largest datagram does not fill the first chunk at the smallest stride")
	}
	for n := 1; n <= alignedMaxData; n++ {
		roles := alignedRoles(nil, n, alignedFECGroup, parityCount(n, alignedFECGroup))
		if roles[0] != 0 || roles[len(roles)-1] != int8(n-1) {
			t.Fatalf("n=%d: roles %v, want data 0 first and the last data chunk last", n, roles)
		}
	}
}

// deliver feeds the receiver the packets of frames, in a shuffled order,
// except those lost says, and returns the record ids that must come through.
func (l *alignedLink) deliver(rng *rand.Rand, frames []sentFrame, lose func(f *sentFrame) map[int]bool) []uint32 {
	var want []uint32
	for i := range frames {
		f := &frames[i]
		lost := lose(f)
		for pos, ids := range f.ids {
			if f.roles[pos] >= 0 && f.recoverable(pos, lost) {
				want = append(want, ids...)
			}
		}
		order := rng.Perm(len(f.pkts))
		for _, pos := range order {
			if !lost[pos] {
				l.reader.handleRTP(f.pkts[pos])
			}
		}
		l.giveUpGaps()
	}
	slices.Sort(want)
	return want
}

// warmUp sends a frame in order, so the reader starts its sequence on it and
// learns the route.
func (l *alignedLink) warmUp(t *testing.T, id *uint32) {
	t.Helper()
	*id++
	l.send(t, []uint32{*id}, []int{100})
	for _, pkt := range l.pkts {
		l.reader.handleRTP(pkt)
	}
	l.assertDelivered(t, []uint32{*id})
}

// giveUpGaps does what the reorder buffer's hold does once it runs out: it
// skips every lost packet, so the next frame starts on an empty buffer.
func (l *alignedLink) giveUpGaps() {
	b := l.reader.reorder
	for b.held() > 0 {
		b.skipToOldest()
		b.drain(l.reader.ordered)
	}
	b.gapSince = time.Time{}
}

// assertDelivered checks that exactly want came through, each once and byte
// exact.
func (l *alignedLink) assertDelivered(t *testing.T, want []uint32) {
	t.Helper()
	got := l.deliveries()
	for _, id := range want {
		if got[id] != 1 {
			t.Fatalf("record %d delivered %d times, want once", id, got[id])
		}
		delete(got, id)
	}
	if len(got) != 0 {
		t.Fatalf("%d records delivered that could not come through: %v", len(got), got)
	}
	if l.bad != 0 {
		t.Fatalf("%d deliveries were not their record byte for byte", l.bad)
	}
}

// sendRandom sends count frames' worth of records of random sizes.
func (l *alignedLink) sendRandom(t *testing.T, rng *rand.Rand, id *uint32, count int) []sentFrame {
	t.Helper()
	ids, sizes := make([]uint32, count), make([]int, count)
	for i := range ids {
		*id++
		ids[i], sizes[i] = *id, 4+rng.IntN(MaxAlignedDatagramSize-3)
		if rng.IntN(4) == 0 {
			sizes[i] = 4 + rng.IntN(100)
		}
	}
	l.send(t, ids, sizes)
	return l.frames(t)
}

// TestAlignedSalvageDeliversWhatArrives drops random RTP packets of aligned
// frames and checks that every record whose packet arrived, or whose packet
// the parity rebuilds, is delivered exactly once, and nothing else is.
func TestAlignedSalvageDeliversWhatArrives(t *testing.T) {
	l := newAlignedLink(t)
	rng := rand.New(rand.NewPCG(11, 11)) //nolint:gosec // reproducible loss
	var id uint32
	l.warmUp(t, &id)
	for range 150 {
		frames := l.sendRandom(t, rng, &id, 1+rng.IntN(64))
		want := l.deliver(rng, frames, func(f *sentFrame) map[int]bool {
			lost := map[int]bool{}
			for pos := range f.pkts {
				if rng.Float64() < 0.08 {
					lost[pos] = true
				}
			}
			return lost
		})
		l.assertDelivered(t, want)
	}
}

// TestAlignedFECRebuildsOneLossPerGroup loses one data packet of every group
// of a full frame and then only its parity packets, and checks that every
// record comes through byte exact either way, even with the first packet,
// the one with the epoch header, among the lost.
func TestAlignedFECRebuildsOneLossPerGroup(t *testing.T) {
	l := newAlignedLink(t)
	rng := rand.New(rand.NewPCG(3, 3)) //nolint:gosec // reproducible loss
	var id uint32
	l.warmUp(t, &id)
	for _, lose := range []func(f *sentFrame) map[int]bool{
		func(f *sentFrame) map[int]bool { // one data packet a group, chunk 0 among them
			lost := map[int]bool{}
			for set := range f.g {
				for pos, role := range f.roles {
					if role >= 0 && int(role)%f.g == set && (set == 0 || rng.IntN(3) == 0) {
						lost[pos] = true
						break
					}
				}
			}
			return lost
		},
		func(f *sentFrame) map[int]bool { // every parity packet
			lost := map[int]bool{}
			for pos, role := range f.roles {
				lost[pos] = role < 0
			}
			return lost
		},
	} {
		for range 20 {
			frames := l.sendRandom(t, rng, &id, 46)
			if frames[0].g == 0 {
				t.Fatal("a full frame has no parity")
			}
			want := l.deliver(rng, frames, lose)
			sent := 0
			for _, f := range frames {
				for pos, ids := range f.ids {
					if f.roles[pos] >= 0 {
						sent += len(ids)
					}
				}
			}
			if len(want) != sent {
				t.Fatalf("the model expects %d of %d records", len(want), sent)
			}
			l.assertDelivered(t, want)
		}
	}
}

// TestAlignedFECTwoLossesSalvageTheRest loses two data packets of one group:
// the group cannot be rebuilt, the rest of the frame is delivered, and a late
// repair of one of the two rebuilds the other.
func TestAlignedFECTwoLossesSalvageTheRest(t *testing.T) {
	l := newAlignedLink(t)
	var id uint32
	l.warmUp(t, &id)
	ids, sizes := make([]uint32, 46), make([]int, 46)
	for i := range ids {
		id++
		ids[i], sizes[i] = id, MaxAlignedDatagramSize
	}
	l.send(t, ids, sizes)
	f := l.frames(t)[0]
	var pair []int
	for pos, role := range f.roles {
		if role >= 0 && int(role)%f.g == 1 && len(pair) < 2 {
			pair = append(pair, pos)
		}
	}
	var want []uint32
	for pos, pkt := range f.pkts {
		if pos != pair[0] && pos != pair[1] {
			l.reader.handleRTP(pkt)
			want = append(want, f.ids[pos]...)
		}
	}
	if len(want) != len(ids)-2 {
		t.Fatalf("%d records arrived, want all but the two lost", len(want))
	}
	l.assertDelivered(t, want)

	l.reader.handleRTP(f.pkts[pair[0]]) // the relay's repair, late
	l.assertDelivered(t, append(slices.Clone(f.ids[pair[0]]), f.ids[pair[1]]...))
	l.reader.handleRTP(f.pkts[pair[1]]) // and the other one, after it was rebuilt
	l.assertDelivered(t, nil)
}

// TestAlignedFrameRepairsPastAGap holds a gap in the reorder buffer with a
// lost packet of a legacy frame, then rebuilds an aligned frame behind it at
// once: neither the hold nor the repair of the gap is waited for, and the
// late original of the rebuilt packet is not delivered again.
func TestAlignedFrameRepairsPastAGap(t *testing.T) {
	l := newAlignedLink(t)
	var id uint32
	l.warmUp(t, &id)

	l.tx.alignedPeer.Store(0) // a legacy frame of three packets
	l.send(t, []uint32{id + 1}, []int{3000})
	legacy := slices.Clone(l.pkts)
	l.tx.alignedPeer.Store(0x200)
	l.reader.handleRTP(legacy[0])
	l.reader.handleRTP(legacy[2]) // legacy[1] is lost: a gap

	ids, sizes := []uint32{id + 2, id + 3, id + 4, id + 5}, []int{1100, 1100, 1100, 1100}
	l.send(t, ids, sizes)
	f := l.frames(t)[0]
	if f.g != 1 {
		t.Fatalf("frame of %d data chunks has %d parity chunks, want 1", f.n, f.g)
	}
	for pos := range f.pkts {
		if pos != 1 {
			l.reader.handleRTP(f.pkts[pos])
		}
	}
	if l.reader.reorder.held() == 0 {
		t.Fatal("the gap closed; the test needs it open")
	}
	l.assertDelivered(t, ids)
	l.reader.handleRTP(f.pkts[1])
	l.reader.handleRTP(legacy[1])
	got := l.deliveries()
	if len(got) != 1 || got[id+1] != 1 {
		t.Fatalf("after the repairs got %v, want only the legacy record", got)
	}
}

// TestLegacyFramesStillNeedEveryPacket checks that a legacy datagram frame
// with a lost packet is dropped whole, as before, and delivered whole when
// nothing is lost.
func TestLegacyFramesStillNeedEveryPacket(t *testing.T) {
	l := newAlignedLink(t)
	l.tx.alignedPeer.Store(0)
	l.send(t, []uint32{1, 2, 3}, []int{1100, 1100, 1100})
	if len(l.pkts) < 3 {
		t.Fatalf("legacy frame of %d packets", len(l.pkts))
	}
	for i, pkt := range l.pkts {
		if i != 1 {
			l.reader.handleRTP(pkt)
		}
	}
	if got := l.deliveries(); len(got) != 0 {
		t.Fatalf("a legacy frame with a lost packet delivered %v", got)
	}
	l.send(t, []uint32{4, 5, 6}, []int{1100, 1100, 1100})
	for _, pkt := range l.pkts {
		l.reader.handleRTP(pkt)
	}
	l.assertDelivered(t, []uint32{4, 5, 6})
}

// TestAlignedFrameCutElsewhereArrivesWhole lays a frame out for a stride
// pion does not use: its first packet is not claimed, and the frame is
// delivered whole through assembly, or dropped whole like a legacy frame.
func TestAlignedFrameCutElsewhereArrivesWhole(t *testing.T) {
	l := newAlignedLink(t)
	var id uint32
	l.warmUp(t, &id)
	write := func(ids []uint32) {
		tx := &l.w.aligned
		hdr := buildEpochHeaderTo(l.tx.bindingToken, l.tx.localEpoch, 0x200)
		tx.reset(hdr[:])
		for _, id := range ids {
			tx.add(record(id, 1000))
		}
		tx.seal()
		l.pkts = l.pkts[:0]
		if !l.tx.writeSampleLocked(tx.layout(alignedMinStride - 1)) {
			t.Fatal("write failed")
		}
	}
	write([]uint32{100, 101, 102})
	for _, pkt := range l.pkts {
		l.reader.handleRTP(pkt)
	}
	l.assertDelivered(t, []uint32{100, 101, 102})
	write([]uint32{103, 104, 105})
	for i, pkt := range l.pkts {
		if i != 2 {
			l.reader.handleRTP(pkt)
		}
	}
	if got := l.deliveries(); len(got) != 0 {
		t.Fatalf("a frame cut elsewhere with a lost packet delivered %v", got)
	}
}

// TestAlignedChunksBeforeTheirRouteAreStolen delivers a frame on a route the
// reader has not learned with its first packet last: the chunks held before
// it are claimed once it teaches the route. A frame on an unknown route whose
// first packet is lost cannot be attributed, and is dropped.
func TestAlignedChunksBeforeTheirRouteAreStolen(t *testing.T) {
	l := newAlignedLink(t)
	var id uint32
	l.warmUp(t, &id)

	l.tx.localEpoch, id = 0x101, 100 // a new route
	l.rx.peerEpoch.Store(0x101)
	ids, sizes := []uint32{101, 102, 103, 104, 105}, []int{1100, 1100, 1100, 1100, 1100}
	l.send(t, ids, sizes)
	f := l.frames(t)[0]
	for pos := len(f.pkts) - 1; pos >= 0; pos-- {
		l.reader.handleRTP(f.pkts[pos])
	}
	l.assertDelivered(t, ids)

	l.tx.localEpoch = 0x102 // another one, its first packet lost
	l.rx.peerEpoch.Store(0x102)
	l.send(t, []uint32{106, 107, 108, 109}, []int{1100, 1100, 1100, 1100})
	for _, pkt := range l.pkts[1:] {
		l.reader.handleRTP(pkt)
	}
	if got := l.deliveries(); len(got) != 0 {
		t.Fatalf("chunks of an unknown route were delivered: %v", got)
	}
}

// BenchmarkAlignedLayout packs, seals and lays out a full aligned frame of
// datagrams of the largest size.
func BenchmarkAlignedLayout(b *testing.B) {
	datagram := make([]byte, MaxAlignedDatagramSize)
	hdr := buildEpochHeaderTo(1, 2, 3)
	var tx alignedTx
	b.ReportAllocs()
	b.SetBytes(int64(alignedMaxData * len(datagram)))
	for range b.N {
		tx.reset(hdr[:])
		for tx.add(datagram) {
		}
		tx.seal()
		if len(tx.layout(alignedMinStride)) == 0 {
			b.Fatal("empty sample")
		}
	}
}

// BenchmarkAlignedReceive feeds a track reader the packets of full aligned
// frames, cut by pion's packetizer; lossy loses one data packet a group,
// which the parity rebuilds.
func BenchmarkAlignedReceive(b *testing.B) {
	for _, lossy := range []bool{false, true} {
		name := "clean"
		if lossy {
			name = "one_loss_a_group"
		}
		b.Run(name, func(b *testing.B) {
			l := newAlignedLink(b)
			l.count = false
			ids, sizes := make([]uint32, alignedMaxData), make([]int, alignedMaxData)
			for i := range ids {
				ids[i], sizes[i] = uint32(i), MaxAlignedDatagramSize
			}
			l.send(b, ids, sizes)
			f := l.frames(b)[0]
			for _, pkt := range f.pkts { // the route, learned
				l.reader.handleRTP(pkt)
			}
			lost := map[int]bool{}
			for set := range f.g {
				if lossy {
					lost[f.dataPosOf(set)] = true
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(alignedMaxData * MaxAlignedDatagramSize))
			base := f.pkts[0].SequenceNumber + uint16(len(f.pkts)) //nolint:gosec // at most maxFramePackets
			for range b.N {
				for pos, pkt := range f.pkts {
					pkt.SequenceNumber = base + uint16(pos)
					if !lost[pos] {
						l.reader.handleRTP(pkt)
					}
				}
				base += uint16(len(f.pkts)) //nolint:gosec // RTP sequence wrap is intentional
			}
		})
	}
}

// dataPosOf is the position of the first data chunk of group set.
func (f *sentFrame) dataPosOf(set int) int {
	for pos, role := range f.roles {
		if role >= 0 && int(role)%f.g == set {
			return pos
		}
	}
	return -1
}
