package vp8channel

import (
	"encoding/binary"
	"hash/crc32"
	"net"
	"sync"
	"sync/atomic"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// wireCRCLen is the size of the CRC32 trailer appended to every KCP packet
// on the wire. KCP is handed to kcp-go with block=nil (no FEC, no checksum),
// so the vp8channel provider - a video stream an SFU may transcode or reorder -
// has no integrity protection at all. A real UDP datagram carries a checksum
// and is dropped on mismatch; without an equivalent, a single flipped byte
// rides through KCP as valid in-order data and corrupts the encrypted muxconn
// stream above it, tripping "chacha20poly1305: message authentication failed"
// (issue #109). The CRC restores UDP-equivalent semantics: a corrupted packet
// is dropped so KCP retransmits it.
const wireCRCLen = 4

// warnInterval rate-limits the drop/truncation warnings.
const warnInterval = 5 * time.Second

// crcTable uses the Castagnoli polynomial for hardware-accelerated checksums
// (SSE4.2 on amd64) on this throughput hot path.
var crcTable = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // shared read-only CRC table

func fakeUDPAddr() *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

// kcpConn is a net.PacketConn implementation that bridges kcp-go on top of
// the vp8channel byte-message provider.
//
//	kcp.UDPSession  ──Write──▶  WriteTo  ──▶ outbound chan  ──▶ VP8 wire
//	kcp.UDPSession  ◀──Read──   ReadFrom  ◀── inbound (deliver) ◀── VP8 wire
//
// All packet boundaries are preserved by the underlying transport, which is
// exactly what KCP expects from a UDP-like conn.
type kcpConn struct {
	out chan<- *packetBuffer
	// acks, when set, takes the packets that only acknowledge the peer, so
	// the lane writes them ahead of its queued data: an acknowledgement
	// behind seconds of pushes tells the peer the path is dark.
	// ai-generated: issue #12.
	acks      chan<- *packetBuffer
	in        chan *packetBuffer
	inPools   [4]sync.Pool
	outPools  [4]sync.Pool
	addr      *net.UDPAddr
	closed    chan struct{}
	closeOnce sync.Once

	// epochHdr is prepended to every outgoing KCP packet so that the peer
	// can detect a session restart on our side (see transport.go for the
	// layout). The dst portion can be re-pointed via setHeader once the
	// remote peer's epoch is learned, so downlink/uplink can be addressed
	// to a specific participant instead of broadcast; a server's per-peer
	// session has its src re-pointed too when the server's epoch rotates
	// (readdressPeers). Guarded by hdrMu.
	hdrMu    sync.RWMutex
	epochHdr [epochHdrLen]byte

	// dropped counts packets discarded because the inbound queue was full,
	// corrupt counts packets rejected by the CRC check, and truncated counts
	// reads whose destination buffer was too small for the packet. All three
	// are otherwise-invisible failure modes, so they are reported through
	// warnRateLimitedf.
	dropped      atomic.Uint64
	corrupt      atomic.Uint64
	truncated    atomic.Uint64
	lastWarnNano atomic.Int64

	// lastAck is when the peer last answered a push or a window probe, on
	// monoNow's clock. The data lane reads it to tell a dark path from a
	// slow one. holdEvery is non-zero while the lane holds pushes back:
	// WriteTo then drops the pushes KCP resends but one per holdEvery, the
	// next due at probeAt, and lets through whatever answers the peer.
	// ai-generated: issue #12.
	lastAck atomic.Int64
	// traffic counts the packets that pass either way, for the client's
	// pick between its lanes. ai-generated (second lane).
	traffic   trafficMeter
	holdEvery atomic.Int64
	probeAt   atomic.Int64
	// acked counts the pushes answered for the first time, the delivery
	// rate the data lane sizes its window by while the path queues.
	// ai-generated: the delivered rate.
	acked atomic.Uint64
	// probeMu guards probeBuf, the newest push held back, which probe()
	// sends again when the next one is due. probeOut is probe()'s copy of
	// it, touched by the lane's writer alone.
	// ai-generated: issue #12.
	probeMu  sync.Mutex
	probeBuf []byte
	probeOut []byte

	// delivery counts the packets with a push the data lane writes and the
	// acknowledgements that come back for them, for the lane's frame cap.
	// ai-generated: issue #12.
	delivery deliveryTrack

	// queuedMu guards queuedSN, the pushes waiting in out by sequence number.
	// spurious counts packets queuedDuplicate kept out of the queue.
	// ai-generated: spurious RTO retransmissions.
	// kick is told when out goes from empty to not, so an idle writer sends
	// that first packet now instead of at its next frame tick: the tick wait
	// was up to a whole frame interval per direction on every request after
	// a pause. Nil for none. ai-generated: the idle kick.
	kick chan struct{}

	queuedMu sync.Mutex
	queuedSN map[uint32]queuedPush
	spurious atomic.Uint64
	// sentAt is when each push last went out in a sample, and pathRTT and
	// pathVar the round trip from there to its acknowledgement, which is
	// what a resend has to wait out. ai-generated: post-send resend veto.
	sentAt  map[uint32]int64
	pathRTT int64
	pathVar int64
	// ai-generated: resent excludes ambiguous acknowledgements from RTT samples.
	// Timing a lost push from its first send includes retransmission delays,
	// which makes a lossy path look congested even when its queue is empty.
	resent map[uint32]struct{}
	// ackedSent is the latest send among the acknowledged pushes: a push
	// sent before it and still unacknowledged was lost, and its resend goes
	// out at once.
	ackedSent int64
	// parked holds the resends that came due before their round trip.
	parked map[uint32]parkedPush

	mu        sync.Mutex
	rDeadline time.Time
	wDeadline time.Time
}

type packetBuffer struct {
	data []byte
	pool *sync.Pool
	// queued is when WriteTo queued the packet, on monoNow's clock; zero on
	// the inbound side. ai-generated: issue #12, a capped lane drops pushes
	// that waited too long.
	queued int64
	// owner and sns are the conn and the push sequence numbers this packet
	// holds in its queue, cleared on release. ai-generated: spurious RTO
	// retransmissions, see queuedDuplicate.
	owner *kcpConn
	sns   []uint32
}

func packetBufferClass(size int) (int, int, bool) {
	switch {
	case size <= 256:
		return 0, 256, true
	case size <= 512:
		return 1, 512, true
	case size <= 1024:
		return 2, 1024, true
	case size <= 1536:
		return 3, 1536, true
	default:
		return 0, size, false
	}
}

func acquirePacketBuffer(pools *[4]sync.Pool, size int) *packetBuffer {
	class, capacity, pooled := packetBufferClass(size)
	if !pooled {
		return &packetBuffer{data: make([]byte, size)}
	}
	pool := &pools[class]
	if value := pool.Get(); value != nil {
		packet, ok := value.(*packetBuffer)
		if ok {
			packet.data = packet.data[:size]
			packet.pool = pool
			return packet
		}
	}
	return &packetBuffer{data: make([]byte, size, capacity), pool: pool}
}

func (p *packetBuffer) release() {
	if p == nil {
		return
	}
	if p.owner != nil { // ai-generated: the pushes leave the queue with the packet.
		p.owner.untrack(p.sns)
		p.owner, p.sns = nil, p.sns[:0]
	}
	if p.pool == nil {
		return
	}
	pool := p.pool
	p.pool = nil
	p.data = p.data[:0]
	pool.Put(p)
}

// releaseSent releases a packet that went out in a sample, noting when its
// pushes left. ai-generated: post-send resend veto.
func (p *packetBuffer) releaseSent() {
	if p != nil && p.owner != nil {
		p.owner.markSent(p.sns)
	}
	p.release()
}

// setHeader re-points the outgoing frame header (used to update the dst epoch
// after the peer is latched). Safe for concurrent use with WriteTo.
func (c *kcpConn) setHeader(hdr [epochHdrLen]byte) {
	c.hdrMu.Lock()
	c.epochHdr = hdr
	c.hdrMu.Unlock()
}

func newKCPConn(out chan<- *packetBuffer, inboundCap int, epochHdr [epochHdrLen]byte) *kcpConn {
	if inboundCap <= 0 {
		inboundCap = 1024
	}
	conn := &kcpConn{
		out:      out,
		in:       make(chan *packetBuffer, inboundCap),
		addr:     fakeUDPAddr(),
		closed:   make(chan struct{}),
		epochHdr: epochHdr,
	}
	return conn
}

// deliver hands an incoming wire payload to the KCP read loop. The trailing
// CRC32 is verified and stripped first: a mismatch means the provider corrupted
// the packet, so we drop it (KCP retransmits via SACK) instead of feeding
// garbage into KCP and, ultimately, the muxconn AEAD (issue #109). Drops on
// overflow are intentional - KCP will detect the loss via SACK and retransmit -
// but they are counted and reported, because a queue that keeps overflowing is
// the difference between "KCP recovered a packet" and "the reader cannot keep
// up and throughput is quietly capped".
func (c *kcpConn) deliver(payload []byte) {
	if len(payload) < wireCRCLen {
		c.corrupt.Add(1)
		return
	}
	body := payload[:len(payload)-wireCRCLen]
	want := binary.BigEndian.Uint32(payload[len(payload)-wireCRCLen:])
	if crc32.Checksum(body, crcTable) != want {
		c.corrupt.Add(1)
		return
	}
	c.traffic.add()
	if answers(body) { // ai-generated: issue #12
		c.lastAck.Store(monoNow())
	}
	// ai-generated: post-send resend veto.
	c.noteAcks(body)
	if st := stampsOf(body); st.acked { // ai-generated: issue #12
		c.delivery.count(st.ack, 0, 1, 0)
	}
	packet := acquirePacketBuffer(&c.inPools, len(body))
	copy(packet.data, body)
	select {
	case c.in <- packet:
	case <-c.closed:
		packet.release()
	default:
		packet.release()
		c.warnRateLimitedf("inbound queue full, dropped %d packet(s) (corrupt=%d)",
			c.dropped.Add(1), c.corrupt.Load())
	}
}

// warnRateLimitedf emits at most one warning per warnInterval so a sustained
// drop storm cannot itself become the problem.
func (c *kcpConn) warnRateLimitedf(format string, args ...any) {
	now := time.Now().UnixNano()
	last := c.lastWarnNano.Load()
	if now-last < int64(warnInterval) {
		return
	}
	if !c.lastWarnNano.CompareAndSwap(last, now) {
		return
	}

	logger.Warnf("vp8channel: "+format, args...)
}

func (c *kcpConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	deadline := c.rDeadline
	c.mu.Unlock()

	var timerC <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, nil, TimeoutError{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timerC = t.C
	}

	select {
	case packet := <-c.in:
		n := copy(p, packet.data)
		packetLen := len(packet.data)
		packet.release()
		if n < packetLen {
			// KCP always reads with a full-MTU buffer, so this means the
			// packet exceeded the MTU: the tail is lost and KCP will see a
			// malformed segment. Never silently.
			c.warnRateLimitedf("read buffer too small: %d of %d bytes, %d truncated read(s)",
				n, packetLen, c.truncated.Add(1))
		}
		return n, c.addr, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	case <-timerC:
		return 0, nil, TimeoutError{}
	}
}

// hold has WriteTo drop pushes but one per every; zero lets them all through.
//
// ai-generated: issue #12.
func (c *kcpConn) hold(every time.Duration) {
	c.probeAt.Store(0)
	c.holdEvery.Store(int64(every))
}

// withheld reports whether WriteTo drops p: a push, while pushes are held
// back. What answers the peer always goes. The newest push held back is kept
// for probe() to send.
//
// ai-generated: issue #12.
func (c *kcpConn) withheld(p []byte) bool {
	if c.holdEvery.Load() == 0 || answers(p) || !pushes(p) {
		return false
	}
	c.keepProbe(p)
	return true
}

// keepProbe remembers a push that was dropped rather than sent, as the one
// probe() sends while the lane is dark.
//
// ai-generated: issue #12.
func (c *kcpConn) keepProbe(p []byte) {
	c.probeMu.Lock()
	c.probeBuf = append(c.probeBuf[:0], p...)
	c.probeMu.Unlock()
}

// probe sends the newest push held back, once per hold interval, so the peer
// has something to answer while the lane is dark. Leaving that to whatever
// KCP happens to resend does not work: kcp-go backs each segment's timeout
// off every time it goes unanswered, so the next resend can be seconds away
// and the lane never learns the path is back.
//
// ai-generated: the whole function (issue #12).
func (c *kcpConn) probe() {
	every := c.holdEvery.Load()
	if every == 0 {
		return
	}
	now := monoNow()
	at := c.probeAt.Load()
	if now < at || !c.probeAt.CompareAndSwap(at, now+every) {
		return
	}
	c.probeMu.Lock()
	c.probeOut = append(c.probeOut[:0], c.probeBuf...)
	c.probeMu.Unlock()
	if len(c.probeOut) == 0 {
		return
	}
	// The lane's own writer calls this, and it is what drains the queue, so
	// a probe waits for no room: with none there is plenty on its way out
	// already, and the next interval brings another probe.
	packet := c.wrap(c.probeOut)
	select {
	case c.out <- packet:
	default:
		packet.release()
	}
}

func (c *kcpConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	size := len(p)
	c.traffic.add()
	// ai-generated: the split answers. kcp-go writes its acknowledgements
	// ahead of the pushes in one packet, and a packet with a push waits on
	// out behind everything queued: while this side sends bulk, the peer's
	// pushes went unanswered for as long as the queue took to drain, seconds
	// on a slow uplink, and the peer's lane counted its path dark. The
	// answers go on acks instead, which the lane writes ahead of the data.
	if c.acks != nil {
		if n := answerPrefix(p); n > 0 && n < len(p) && c.queueAnswers(p[:n]) {
			p = p[n:]
		}
	}
	if c.withheld(p) { // ai-generated: issue #12, as a full path would drop it
		return size, nil
	}
	if _, err := c.enqueue(p); err != nil {
		return 0, err
	}
	return size, nil
}

// wrap builds the wire packet for a KCP packet. Layout: [epoch header][KCP
// packet p][CRC32(p)]. The receiver strips the epoch header before
// deliver(), which then verifies and strips the CRC.
func (c *kcpConn) wrap(p []byte) *packetBuffer {
	packet := acquirePacketBuffer(&c.outPools, epochHdrLen+len(p)+wireCRCLen)
	packet.queued = monoNow()
	buf := packet.data
	c.hdrMu.RLock()
	copy(buf, c.epochHdr[:])
	c.hdrMu.RUnlock()
	copy(buf[epochHdrLen:], p)
	binary.BigEndian.PutUint32(buf[epochHdrLen+len(p):], crc32.Checksum(p, crcTable))
	return packet
}

// enqueue wraps a KCP packet for the wire and queues it for the lane.
func (c *kcpConn) enqueue(p []byte) (int, error) {
	size := len(p)
	if c.queuedDuplicate(p) { // ai-generated: spurious RTO retransmissions.
		c.spurious.Add(1)
		return size, nil
	}
	packet := c.wrap(p)
	if c.acks != nil && answers(p) && !pushes(p) { // ai-generated: issue #12
		select {
		case c.acks <- packet:
			return size, nil
		default: // full: it waits with the data
		}
	}

	c.mu.Lock()
	deadline := c.wDeadline
	c.mu.Unlock()

	var timerC <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, TimeoutError{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timerC = t.C
	}

	c.track(packet, p)
	select {
	case c.out <- packet:
		if len(c.out) == 1 { // ai-generated: the idle kick.
			select {
			case c.kick <- struct{}{}:
			default:
			}
		}
		return size, nil
	case <-c.closed:
		packet.release()
		return 0, net.ErrClosed
	case <-timerC:
		packet.release()
		return 0, TimeoutError{}
	}
}

// queueAnswers puts a packet of answers on acks, reporting false when acks
// is full. ai-generated: the split answers.
func (c *kcpConn) queueAnswers(p []byte) bool {
	packet := c.wrap(p)
	select {
	case c.acks <- packet:
		return true
	default:
		packet.release()
		return false
	}
}

// answerPrefix is the length of the answers a KCP packet starts with, zero
// when it starts with something else or a length runs past its end.
// ai-generated: the split answers.
func answerPrefix(segs []byte) int {
	rest := segs
	for len(rest) >= kcp.IKCP_OVERHEAD && answers(rest) {
		size := binary.LittleEndian.Uint32(rest[kcpLenOff:])
		body := rest[kcp.IKCP_OVERHEAD:]
		if uint64(size) > uint64(len(body)) {
			return 0
		}
		rest = body[size:]
	}
	return len(segs) - len(rest)
}

// ai-generated: spurious RTO retransmission suppression (queuedDuplicate,
// track, untrack and their constants).
const (
	kcpSnOff = 12
	// queuedHold bounds how long a queued push keeps its resends out: past
	// it a resend goes through, so a packet that somehow never left the
	// queue cannot silence its segment for good.
	queuedHold = 2 * time.Second
	// sentHold caps how long a push that went out keeps its resends off the
	// wire, and sentAtMax how many send times a conn keeps before it drops
	// the old ones.
	sentHold  = 1500 * time.Millisecond
	sentAtMax = 8192
)

// queuedPush is how many copies of a push wait in out, and since when.
type queuedPush struct {
	n  int32
	at int64
}

// parkedPush is a resend held back until due, its round trip from the send.
type parkedPush struct {
	data []byte
	due  int64
}

// What queuedDuplicate makes of a resend: send it, drop it because a copy
// still waits in out, or hold it while the copy that went out may still be
// answered.
const (
	resendGo = iota
	resendQueued
	resendEarly
)

// queuedDuplicate reports whether p is only pushes whose earlier copies still
// wait in out. KCP stamps a resend deadline when it hands a segment over, but
// the paced writer drains a batch a frame, so the tail of a window burst can
// outwait its RTO in this queue; queueing the resend as well only doubles the
// wait and the wire cost of what is already on its way.
// ai-generated: reuse an existing parked sequence's payload while refreshing its header.
func (c *kcpConn) queuedDuplicate(p []byte) bool {
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	if len(c.queuedSN) == 0 && len(c.sentAt) == 0 {
		return false
	}
	now := monoNow()
	orig, pushes, early := p, 0, false
	var sn uint32
	for len(p) >= kcp.IKCP_OVERHEAD {
		size := binary.LittleEndian.Uint32(p[kcpLenOff:])
		rest := p[kcp.IKCP_OVERHEAD:]
		if p[kcpCmdOff] != kcp.IKCP_CMD_PUSH || uint64(size) > uint64(len(rest)) {
			return false
		}
		sn = binary.LittleEndian.Uint32(p[kcpSnOff:])
		switch c.resendState(sn, now) {
		case resendGo:
			return false
		case resendEarly:
			early = true
		}
		pushes++
		p = rest[size:]
	}
	if pushes == 0 || len(p) != 0 || (early && pushes > 1) {
		return false
	}
	if early {
		// ai-generated: a resend due before its round trip is up waits for
		// it instead of being dropped, which would leave a lost segment to
		// KCP's next, backed-off timeout.
		if c.parked == nil {
			c.parked = make(map[uint32]parkedPush)
		}
		previous := c.parked[sn]
		c.parked[sn] = parkedPush{data: append(previous.data[:0], orig...), due: c.sentAt[sn] + c.sentWait()}
	}
	return true
}

// track records the pushes a queued packet carries.
func (c *kcpConn) track(packet *packetBuffer, p []byte) {
	now := monoNow()
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	for len(p) >= kcp.IKCP_OVERHEAD {
		size := binary.LittleEndian.Uint32(p[kcpLenOff:])
		rest := p[kcp.IKCP_OVERHEAD:]
		if uint64(size) > uint64(len(rest)) {
			break
		}
		if p[kcpCmdOff] == kcp.IKCP_CMD_PUSH {
			if c.queuedSN == nil {
				c.queuedSN = make(map[uint32]queuedPush)
			}
			sn := binary.LittleEndian.Uint32(p[kcpSnOff:])
			c.queuedSN[sn] = queuedPush{n: c.queuedSN[sn].n + 1, at: now}
			packet.sns = append(packet.sns, sn)
		}
		p = rest[size:]
	}
	if len(packet.sns) > 0 {
		packet.owner = c
	}
}

// resendState says what to do with a resend: drop it while an earlier copy
// waits in out, hold it while the copy that went out less than a round trip
// ago (as this path measures it from the send) may still be answered, or go.
// KCP times a resend from when it handed the segment over, so a segment that
// waited for its frame comes due before its acknowledgement can be back: on
// a real path a third of what arrived at the far end was such a duplicate.
// The caller holds queuedMu. ai-generated: post-send resend veto.
func (c *kcpConn) resendState(sn uint32, now int64) int {
	if q, ok := c.queuedSN[sn]; ok && now-q.at < int64(queuedHold) {
		return resendQueued
	}
	at, ok := c.sentAt[sn]
	if !ok || c.pathRTT == 0 || now-at >= c.sentWait() || c.ackedSent > at {
		// No earlier copy, no measure of the path, the round trip is up, or
		// a later send was acknowledged first, so this one was lost.
		return resendGo
	}
	return resendEarly
}

// inFlight is how many pushes went out unanswered, as far as sentAt keeps
// them. ai-generated: the second lane's load.
func (c *kcpConn) inFlight() int {
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	return len(c.sentAt)
}

// sentWait is how long a sent push waits for its acknowledgement before a
// resend may follow it: at least two round trips, since the spread alone
// read 9 ms on a VK path whose late acknowledgements still came after
// srtt + 4 rttvar and made a sixth of what a desktop received duplicates;
// a lost segment mostly goes on the loss evidence long before this. The
// caller holds queuedMu. ai-generated: the 2 x pathRTT floor.
func (c *kcpConn) sentWait() int64 {
	return min(max(c.pathRTT+4*c.pathVar, 2*c.pathRTT), int64(sentHold))
}

// flushParked queues the parked resends whose round trip is up or whose
// segment a later acknowledgement showed lost; an acknowledged one was
// dropped from parked when its acknowledgement came. It never blocks: the
// lane that calls it is what drains out, and a resend that finds no room
// is left to KCP's next timeout. ai-generated: post-send resend veto.
func (c *kcpConn) flushParked(now int64) {
	c.queuedMu.Lock()
	var due [][]byte
	for sn, pp := range c.parked {
		at, sent := c.sentAt[sn]
		if !sent || now >= pp.due || c.ackedSent > at {
			due = append(due, pp.data)
			delete(c.parked, sn)
		}
	}
	c.queuedMu.Unlock()
	for _, data := range due {
		packet := c.wrap(data)
		c.track(packet, data)
		select {
		case c.out <- packet:
		default:
			packet.release()
		}
	}
}

// markSent notes when pushes went out in a sample, dropping send times older
// than sentHold once there are sentAtMax of them.
// ai-generated: post-send resend veto.
func (c *kcpConn) markSent(sns []uint32) {
	now := monoNow()
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	if c.sentAt == nil {
		c.sentAt = make(map[uint32]int64)
		c.resent = make(map[uint32]struct{})
	}
	if len(c.sentAt) >= sentAtMax {
		for sn, at := range c.sentAt {
			if now-at >= int64(sentHold) {
				delete(c.sentAt, sn)
				delete(c.resent, sn)
			}
		}
	}
	for _, sn := range sns {
		if _, again := c.sentAt[sn]; again {
			c.resent[sn] = struct{}{}
		}
		c.sentAt[sn] = now
	}
}

// noteAcks counts acknowledgements and samples RTT only for pushes sent once.
// ai-generated: exclude retransmissions from RTT measurements (Karn's algorithm).
func (c *kcpConn) noteAcks(body []byte) {
	now := monoNow()
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	if len(c.sentAt) == 0 {
		return
	}
	for len(body) >= kcp.IKCP_OVERHEAD {
		size := binary.LittleEndian.Uint32(body[kcpLenOff:])
		rest := body[kcp.IKCP_OVERHEAD:]
		if uint64(size) > uint64(len(rest)) {
			return
		}
		if body[kcpCmdOff] == kcp.IKCP_CMD_ACK {
			sn := binary.LittleEndian.Uint32(body[kcpSnOff:])
			delete(c.parked, sn)
			if at, ok := c.sentAt[sn]; ok {
				delete(c.sentAt, sn)
				c.acked.Add(1)
				if _, again := c.resent[sn]; again {
					delete(c.resent, sn)
				} else {
					c.ackedSent = max(c.ackedSent, at)
					c.sampleRTT(now - at)
				}
			}
		}
		body = rest[size:]
	}
}

// sentRTT is pathRTT in milliseconds, zero before the first sample. Unlike
// KCP's own round trip it starts when a push goes out in a sample, not when
// KCP hands it to the queue, so a lane holding its queue back does not read
// its own hold as a path that queues. ai-generated: the queue pace.
func (c *kcpConn) sentRTT() int32 {
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	return int32(c.pathRTT / int64(time.Millisecond)) //nolint:gosec // G115: a round trip in ms fits
}

// sampleRTT folds one round trip into pathRTT and pathVar the way KCP folds
// its own. ai-generated: post-send resend veto.
func (c *kcpConn) sampleRTT(rtt int64) {
	if c.pathRTT == 0 {
		c.pathRTT, c.pathVar = rtt, rtt/2
		return
	}
	delta := rtt - c.pathRTT
	c.pathRTT += delta / 8
	c.pathVar += (max(delta, -delta) - c.pathVar) / 4
}

// untrack forgets the pushes of a packet that left the queue.
func (c *kcpConn) untrack(sns []uint32) {
	c.queuedMu.Lock()
	defer c.queuedMu.Unlock()
	for _, sn := range sns {
		if q := c.queuedSN[sn]; q.n > 1 {
			c.queuedSN[sn] = queuedPush{n: q.n - 1, at: q.at}
		} else {
			delete(c.queuedSN, sn)
		}
	}
}

func (c *kcpConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *kcpConn) LocalAddr() net.Addr { return c.addr }

func (c *kcpConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	_ = c.SetWriteDeadline(t)
	return nil
}

func (c *kcpConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *kcpConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.wDeadline = t
	c.mu.Unlock()
	return nil
}

// TimeoutError is a net.Error indicating a deadline exceeded.
type TimeoutError struct{}

// kcp-go and everything else on this path classify read/write failures with
// errors.As(err, &net.Error). That interface still lists the deprecated
// Temporary method, so dropping it would silently stop TimeoutError being
// recognised as a timeout at all.
var _ net.Error = TimeoutError{}

func (TimeoutError) Error() string { return "i/o timeout" }

// Timeout reports that this error is a timeout.
func (TimeoutError) Timeout() bool { return true }

// Temporary reports that this error is temporary.
func (TimeoutError) Temporary() bool { return true }

// trafficBucket is the span of one bucket of a trafficMeter.
const trafficBucket = int64(500 * time.Millisecond)

// trafficMeter counts packets over the bucket filling now and the one before
// it: about the last second of a lane's traffic, both ways.
//
// ai-generated: the whole type (second lane).
type trafficMeter struct {
	mu        sync.Mutex
	at        int64 // bucket of cur
	cur, prev int
}

func (m *trafficMeter) roll(now int64) {
	switch b := now / trafficBucket; b {
	case m.at:
		return
	case m.at + 1:
		m.prev, m.cur = m.cur, 0
	default:
		m.prev, m.cur = 0, 0
	}
	m.at = now / trafficBucket
}

func (m *trafficMeter) add() {
	now := monoNow()
	m.mu.Lock()
	m.roll(now)
	m.cur++
	m.mu.Unlock()
}

func (m *trafficMeter) recent() int {
	now := monoNow()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.roll(now)
	return m.prev + m.cur
}
