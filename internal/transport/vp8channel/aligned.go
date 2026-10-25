package vp8channel

// ai-generated: the whole file (rtp-aligned datagram frames with xor fec).

import (
	"crypto/subtle"
	"encoding/binary"
	"sync/atomic"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
)

// RTP-aligned datagram frames. A frame reaches the receiver only when every
// RTP packet of it does, so one lost packet in a frame of forty used to cost
// every datagram in it. An aligned frame is laid out in chunks that pion's
// VP8 payloader cuts into RTP payloads one to one, and no datagram crosses a
// chunk, so a lost packet costs only the datagrams it carried; one parity
// chunk per group of data chunks rebuilds one lost chunk of the group.
//
// pion packetizes a sample at outboundMTU (1200) less the 12-byte RTP header
// and cuts it into fragments of that less the VP8 descriptor: one byte while
// the picture id is 0, three below 128, four from there; the id starts at 0,
// rises by one a sample and wraps at 0x8000. vp8Payloader counts the samples
// pion fragments, so the writer knows the stride of the next one.
//
//	chunk 0:  [epoch header 36][OLUA][stride 2][n][k][records]
//	chunk p:  [0xA1][tag 4][p][n][k][records, or a parity body]
//	record:   [length 2][datagram]; a zero length or the chunk's end ends them
//
// Every chunk but the last is exactly stride bytes, zero padded. n counts the
// data chunks and k is the FEC group, zero for none; parityCount gives the
// parity chunks and alignedRoles where each sits. The tag is the epoch
// header's CRC: it names the route (token, src, dst), so a chunk whose first
// packet was lost is still delivered to the route that packet carried on an
// earlier frame of the track, and to no other; so is a rebuilt first chunk,
// whose head the parity leaves out. A body is what follows a chunk's head;
// parity r is the XOR of the data bodies d with d%G == r, zero extended to
// the longest, so it rebuilds any one of them, and a burst of up to G lost
// packets in a row hits G different groups.
//
// A peer gets aligned frames only once its keepalives carry OLUC, which a
// build without them neither sends nor parses (it drops it as a bad KCP
// packet); everything else still gets OLUB/OLUD, which every build parses.
const (
	// rtpPacketMTU is the MTU pion's TrackLocalStaticSample packetizes with
	// (webrtc outboundMTU), rtpHeaderLen the RTP header the packetizer takes
	// off it before the payloader cuts the sample.
	rtpPacketMTU = 1200
	rtpHeaderLen = 12
	// alignedMaxStride and alignedMinStride are the sample bytes one RTP
	// packet carries behind the one-byte VP8 descriptor of picture id 0 and
	// behind the four-byte one of picture ids from 128 on.
	alignedMaxStride = rtpPacketMTU - rtpHeaderLen - 1
	alignedMinStride = rtpPacketMTU - rtpHeaderLen - 4
	// alignedHead0 is the first chunk's head: the epoch header, OLUA, the
	// stride, n and k. alignedHeadN is every other chunk's: the chunk magic,
	// the tag, the position, n and k.
	alignedHead0 = epochHdrLen + 4 + 4
	alignedHeadN = 8
	// alignedBodyCap is the most a chunk past the first carries behind its
	// head at the smallest stride. A parity body is never longer, so a parity
	// chunk fits a packet too.
	alignedBodyCap = alignedMinStride - alignedHeadN
	// MaxAlignedDatagramSize is the largest datagram an aligned frame
	// carries: it fits the first chunk, the smallest, at the smallest stride.
	// A larger one goes out in a legacy frame, whole or not at all.
	MaxAlignedDatagramSize = alignedMinStride - alignedHead0 - batchLenLen
	// alignedFECGroup is k, the data chunks a parity chunk is added for. At
	// the 0.3% of RTP packets an LTE phone loses after the relay's repairs, a
	// frame of 46 data chunks loses one 13% of the time; with a parity per 8
	// a datagram is lost only when a second packet of its group is too, about
	// k*p*p, some 40 times less often, for 1/k = 12.5% more bytes. Twice the
	// group halves the overhead and doubles what is left of the loss; the
	// tunnel favours low loss over throughput.
	alignedFECGroup = 8
	// alignedMaxData is the most data chunks a frame carries: with its parity
	// it stays within defaultMaxPayloadSize at the largest stride.
	alignedMaxData = 46
	// alignedMaxSets bounds the parity chunks of a frame a receiver takes.
	alignedMaxSets = 16
	// alignedChunkMagic heads every chunk past the first.
	alignedChunkMagic byte = 0xA1
)

var (
	alignedMagic    = [4]byte{'O', 'L', 'U', 'A'} //nolint:gochecknoglobals // wire marker
	alignedCapMagic = [4]byte{'O', 'L', 'U', 'C'} //nolint:gochecknoglobals // wire marker
)

// vp8Payloader is the VP8 payloader pion builds for a VP8 track
// (payloaderForCodec: EnablePictureID), counting the samples it cuts. The
// picture id, and so the descriptor and the stride of the next sample, follow
// from that count alone. Counting in writeSampleLocked would not do: until the
// track is bound pion drops samples before they reach the payloader.
type vp8Payloader struct {
	codecs.VP8Payloader
	next atomic.Uint32 // the picture id the next sample gets
	// ai-generated: server-owned fragment storage, reused under writeMu.
	scratch *vp8Scratch
}

func newVP8Payloader() *vp8Payloader {
	return &vp8Payloader{VP8Payloader: codecs.VP8Payloader{EnablePictureID: true}}
}

// Payload implements rtp.Payloader. pion moves the picture id on only for a
// sample it cuts into at least one fragment.
// ai-generated: retain Pion on clients and reuse fragment storage on servers.
func (v *vp8Payloader) Payload(mtu uint16, payload []byte) [][]byte {
	var out [][]byte
	if v.scratch == nil {
		out = v.VP8Payloader.Payload(mtu, payload)
	} else {
		out = v.scratch.payload(mtu, payload, v.next.Load(), v.EnablePictureID)
	}
	if len(out) > 0 {
		v.next.Store((v.next.Load() + 1) & 0x7fff)
	}
	return out
}

// payloader hands pion this payloader for the track (webrtc.WithPayloader).
func (v *vp8Payloader) payloader(webrtc.RTPCodecCapability) (rtp.Payloader, error) {
	return v, nil
}

// stride is the sample bytes each RTP packet of the next sample carries.
func (v *vp8Payloader) stride() int {
	switch id := v.next.Load(); {
	case id == 0:
		return alignedMaxStride
	case id < 128:
		return alignedMaxStride - 2
	default:
		return alignedMinStride
	}
}

// parityCount is how many parity chunks a frame of n data chunks gets for a
// group of k: one per k, and one for a frame of at least k/2. Below that a
// lost chunk costs no more than a lost packet ever did, and a parity chunk
// would double a frame that is mostly padding.
func parityCount(n, k int) int {
	switch {
	case k <= 0:
		return 0
	case n >= k:
		return n / k
	case 2*n >= k:
		return 1
	default:
		return 0
	}
}

// alignedRoles appends what each chunk position of a frame carries to roles:
// data chunk d as d, parity r as ^r. Parity r goes in front of data chunk
// min((r+1)k, n-1), never the first: the frame ends with its last data chunk,
// the one first-fit leaves emptiest, unpadded.
func alignedRoles(roles []int8, n, k, g int) []int8 {
	r := 0
	for d := range n {
		for r < g && d == max(1, min((r+1)*k, n-1)) {
			roles = append(roles, int8(^r)) //nolint:gosec // r < alignedMaxSets
			r++
		}
		roles = append(roles, int8(d))
	}
	for ; r < g; r++ {
		roles = append(roles, int8(^r)) //nolint:gosec // r < alignedMaxSets
	}
	return roles
}

// alignedRecords calls yield with each record of a chunk body, up to a zero
// length, the padding, or the end of the body.
func alignedRecords(body []byte, yield func([]byte)) {
	for len(body) >= batchLenLen {
		size := int(binary.BigEndian.Uint16(body))
		if size == 0 || size > len(body)-batchLenLen {
			return
		}
		yield(body[batchLenLen : batchLenLen+size])
		body = body[batchLenLen+size:]
	}
}

// alignedTx packs one aligned frame: datagrams first-fit into data chunk
// bodies, then the parity bodies, then, under the write lock, the layout at
// the stride the next sample gets. Bodies are sized for the smallest stride,
// so packing does not depend on which one that is.
type alignedTx struct {
	hdr     [epochHdrLen]byte
	bodies  []byte // alignedMaxData data bodies, then alignedMaxSets parity bodies
	fill    [alignedMaxData + alignedMaxSets]int
	n, g    int
	records int
	roles   []int8
	sample  []byte
	ready   bool // packed and not yet written
}

// reset starts a frame for the route of hdr.
func (tx *alignedTx) reset(hdr []byte) {
	if tx.bodies == nil {
		tx.bodies = make([]byte, (alignedMaxData+alignedMaxSets)*alignedBodyCap)
		tx.roles = make([]int8, 0, maxFramePackets)
		tx.sample = make([]byte, 0, defaultMaxPayloadSize)
	}
	copy(tx.hdr[:], hdr)
	tx.n, tx.g, tx.records, tx.ready = 0, 0, 0, false
}

func (tx *alignedTx) body(i int) []byte {
	return tx.bodies[i*alignedBodyCap : (i+1)*alignedBodyCap]
}

// room is what data chunk d still takes; the first carries the frame's head.
func (tx *alignedTx) room(d int) int {
	room := alignedBodyCap - tx.fill[d]
	if d == 0 {
		room -= alignedHead0 - alignedHeadN
	}
	return room
}

// add puts a datagram of 1 to MaxAlignedDatagramSize bytes in the first data
// chunk with room, or a new one, and reports false when the frame is full.
func (tx *alignedTx) add(datagram []byte) bool {
	need := batchLenLen + len(datagram)
	d := 0
	for d < tx.n && tx.room(d) < need {
		d++
	}
	if d == tx.n {
		if tx.n == alignedMaxData {
			return false
		}
		tx.fill[d] = 0
		tx.n++
	}
	body := tx.body(d)[tx.fill[d]:]
	binary.BigEndian.PutUint16(body, uint16(len(datagram))) //nolint:gosec // at most MaxAlignedDatagramSize
	copy(body[batchLenLen:], datagram)
	tx.fill[d] += need
	tx.records++
	return true
}

// seal computes the parity bodies and the chunk order.
func (tx *alignedTx) seal() {
	tx.g = parityCount(tx.n, alignedFECGroup)
	for r := range tx.g {
		parity := tx.body(alignedMaxData + r)
		size := 0
		for d := r; d < tx.n; d += tx.g {
			body := tx.body(d)[:tx.fill[d]]
			if len(body) > size {
				clear(parity[size:len(body)])
				size = len(body)
			}
			subtle.XORBytes(parity, parity[:len(body)], body)
		}
		tx.fill[alignedMaxData+r] = size
	}
	tx.roles = alignedRoles(tx.roles[:0], tx.n, alignedFECGroup, tx.g)
	tx.ready = true
}

// layout writes the frame out for samples cut every stride bytes.
func (tx *alignedTx) layout(stride int) []byte {
	sample := tx.sample[:0]
	last := len(tx.roles) - 1
	for pos, role := range tx.roles {
		end := len(sample) + stride
		if pos == 0 {
			sample = append(sample, tx.hdr[:]...)
			sample = append(sample, alignedMagic[:]...)
			sample = binary.BigEndian.AppendUint16(sample, uint16(stride)) //nolint:gosec // at most alignedMaxStride
		} else {
			sample = append(sample, alignedChunkMagic)
			sample = append(sample, tx.hdr[crcOff:epochHdrLen]...)
			sample = append(sample, byte(pos))
		}
		sample = append(sample, byte(tx.n), alignedFECGroup) //nolint:gosec // n <= alignedMaxData
		i := int(role)
		if role < 0 {
			i = alignedMaxData + int(^role)
		}
		sample = append(sample, tx.body(i)[:tx.fill[i]]...)
		if pos < last {
			sample = append(sample, make([]byte, end-len(sample))...)
		}
	}
	tx.sample = sample
	return sample
}
