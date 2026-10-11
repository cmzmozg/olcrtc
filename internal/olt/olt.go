// Package olt is a multi-stream reliable transport over a lossy packet
// carrier. Every stream is ordered on its own, so a loss on one stalls no
// other; a rate-based congestion controller in the spirit of BBR paces the
// connection and takes neither isolated nor bursty loss for congestion; and
// the scheduler serves streams that have sent little before bulk ones.
//
// olt does not encrypt: the caller seals and opens packets around Send and
// Input. Lost, reordered, duplicated and replayed packets are harmless.
//
// ai-generated: the whole package.
package olt

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Defaults for the zero fields of Config.
const (
	defaultMaxPacket   = 1100
	defaultStreamWin   = 1 << 20
	defaultConnWin     = 8 << 20
	defaultTick        = 5 * time.Millisecond
	defaultBurst       = 64 << 10
	defaultKeepAlive   = 10 * time.Second
	defaultIdleTimeout = 30 * time.Second
	defaultMaxStreams  = 1024
	// minPacket and maxPacket bound Config.MaxPacket: a packet must hold a
	// header, an ACK and a useful STREAM frame, and a frame length must fit
	// a two-byte varint.
	minPacket = 200
	maxPacket = 16383
	// closeWait bounds how long Close waits for the CLOSE packet to leave.
	closeWait = time.Second
)

// Sentinel errors.
var (
	// ErrClosed reports an operation on a connection or stream closed here.
	ErrClosed = fmt.Errorf("olt: closed: %w", net.ErrClosed)
	// ErrPeerClosed reports that the peer closed the connection.
	ErrPeerClosed = errors.New("olt: closed by peer")
	// ErrIdleTimeout reports that nothing came from the peer for IdleTimeout.
	ErrIdleTimeout = errors.New("olt: idle timeout")
	// ErrTooManyStreams reports that MaxStreams streams are already open.
	ErrTooManyStreams = errors.New("olt: too many streams")
	// ErrStreamReset reports that the peer abandoned the stream.
	ErrStreamReset = errors.New("olt: stream reset by peer")
	// ErrStreamRefused reports that the peer refused the stream (its
	// MaxStreams was reached).
	ErrStreamRefused = errors.New("olt: stream refused by peer")
	// ErrWriteClosed reports a Write after CloseWrite.
	ErrWriteClosed = errors.New("olt: write after close write")
	// ErrConfig reports an unusable Config.
	ErrConfig = errors.New("olt: invalid config")
	// ErrMalformed reports a packet that does not parse.
	ErrMalformed = errors.New("olt: malformed packet")
	// ErrWrongConn reports a packet tagged with another connection id.
	ErrWrongConn = errors.New("olt: packet of another connection")
)

// Config configures a Conn.
type Config struct {
	// Send transmits one packet to the peer. It must not retain p after it
	// returns. A returned error counts as a lost packet. Packets may be lost,
	// reordered and duplicated by the path. olt calls Send from one goroutine
	// at a time, never while holding its lock.
	Send func(p []byte) error
	// MaxPacket is the largest packet Send may carry, in bytes (use 1100 when 0).
	MaxPacket int
	// Client selects the stream id space: the client opens odd ids, the server even.
	Client bool
	// ConnID tags every packet; Input drops packets carrying another id.
	ConnID uint64
	// StreamWindow and ConnWindow are the receive windows in bytes
	// (defaults 1 MiB and 8 MiB). Values below the protocol's initial
	// windows (256 KiB and 1 MiB) are raised to them.
	StreamWindow, ConnWindow int
	// Tick is the carrier's frame interval: what olt releases is written at
	// most this often, in bursts. Default 5 ms. The pacer is never finer.
	Tick time.Duration
	// BurstBytes is the most the carrier writes in one frame (default 64 KiB);
	// the congestion window allows for two such bursts.
	BurstBytes int
	// KeepAlive (default 10 s) and IdleTimeout (default 30 s).
	KeepAlive, IdleTimeout time.Duration
	// MaxStreams bounds concurrently open streams the peer may open (default
	// 1024); OpenStream holds this side to it as well.
	MaxStreams int
	// Logf, when set, receives rare diagnostic lines. It may be called with
	// the connection locked, so it must not call back into the Conn.
	Logf func(format string, args ...any)
}

// Stats is a snapshot of a connection's transport state.
type Stats struct {
	// SRTT is the smoothed round trip time, MinRTT the congestion
	// controller's windowed minimum.
	SRTT, MinRTT time.Duration
	// BtlBw is the bottleneck bandwidth estimate and PacingRate the rate
	// packets are released at, both in bytes per second.
	BtlBw, PacingRate float64
	// Cwnd and BytesInFlight are in bytes.
	Cwnd, BytesInFlight int
	// PacketsRetransmitted counts packets that carried resent stream data.
	PacketsSent, PacketsLost, PacketsRetransmitted uint64
	// State is the congestion controller state: startup, drain or probe_bw.
	State string
}

// Conn is one olt connection: the streams carried over one packet path.
type Conn struct {
	cfg       Config
	epoch     time.Time
	streamWin uint64
	connWin   uint64
	gran      time.Duration

	wake     chan struct{}
	acceptCh chan struct{}
	closedCh chan struct{}
	done     chan struct{}

	// batch holds the packets one loop pass sends; the loop alone uses it.
	batch [][]byte

	mu sync.Mutex
	// Everything below is guarded by mu.
	err       error
	sendClose bool

	streams   map[uint32]*Stream
	nextID    uint32
	localOpen int
	peerOpen  int
	peerSeen  rangeSet // peer stream ids seen, as id/2
	acceptq   []*Stream
	sendq     []*Stream // streams that may have new data
	retxq     []*Stream // streams with data to send again
	ctrlq     []*Stream // streams with control frames to send
	refuse    []uint32  // peer streams to refuse
	rr        int

	// Connection flow control: rcvd counts the highest offsets received,
	// read what the app consumed or olt discarded; sent counts new bytes.
	rcvMax, rcvd, read uint64
	sndMax, sent       uint64
	needMaxData        bool

	in            inPacket
	rcvdPN        rangeSet
	hasRcvd       bool
	largestRcvd   uint64
	largestRcvdAt time.Duration
	ackPending    int
	ackNow        bool
	ackAt         time.Duration
	ackBuf        []byte
	ackRanges     int
	lastRecv      time.Duration

	nextPN         uint64
	sentq          sentRing
	inflight       int
	hasAcked       bool
	largestAcked   uint64
	lossAt         time.Duration
	ptoCount       int
	probes         int
	lastElicitSent time.Duration
	needPing       bool
	rtt            rttStats
	cc             bbr
	pace           pacer
	delivered      int64
	deliveredAt    time.Duration
	firstSentAt    time.Duration
	appLimited     int64
	build          builder

	nSent, nLost, nRetx uint64
}

// New starts a connection. It sends nothing until there is something to
// send, and it closes on its own after IdleTimeout without packets.
//
// ai-generated: whole function.
func New(cfg Config) (*Conn, error) {
	if cfg.Send == nil {
		return nil, fmt.Errorf("%w: Send is nil", ErrConfig)
	}
	cfg = withDefaults(cfg)
	if cfg.MaxPacket < minPacket || cfg.MaxPacket > maxPacket {
		return nil, fmt.Errorf("%w: MaxPacket %d is outside %d..%d", ErrConfig, cfg.MaxPacket, minPacket, maxPacket)
	}
	c := &Conn{
		cfg:       cfg,
		epoch:     time.Now(),
		streamWin: u64(max(cfg.StreamWindow, initStreamWin)),
		gran:      max(cfg.Tick, time.Millisecond),
		wake:      make(chan struct{}, 1),
		acceptCh:  make(chan struct{}, 1),
		closedCh:  make(chan struct{}),
		done:      make(chan struct{}),
		batch:     make([][]byte, maxBatch),
		streams:   make(map[uint32]*Stream),
		nextID:    2,
		sndMax:    initConnWin,
		ackBuf:    make([]byte, 0, cfg.MaxPacket),
		ackRanges: min(maxAckRanges, (cfg.MaxPacket-headerMax-ackFixed)/ackPerRange),
	}
	c.connWin = max(u64(cfg.ConnWindow), c.streamWin, initConnWin)
	c.rcvMax = c.connWin
	c.needMaxData = c.connWin > initConnWin
	if cfg.Client {
		c.nextID = 1
	}
	for i := range c.batch {
		c.batch[i] = make([]byte, 0, cfg.MaxPacket)
	}
	c.cc.init(cfg.MaxPacket, cfg.BurstBytes, cfg.Tick, cfg.Logf)
	c.pace.tokens = float64(initPaceTokens * cfg.MaxPacket)
	go c.run()
	return c, nil
}

// withDefaults fills the zero and negative fields of cfg.
//
// ai-generated: whole function.
func withDefaults(cfg Config) Config {
	pick := func(v, def int) int {
		if v <= 0 {
			return def
		}
		return v
	}
	pickDur := func(v, def time.Duration) time.Duration {
		if v <= 0 {
			return def
		}
		return v
	}
	cfg.MaxPacket = pick(cfg.MaxPacket, defaultMaxPacket)
	cfg.StreamWindow = pick(cfg.StreamWindow, defaultStreamWin)
	cfg.ConnWindow = pick(cfg.ConnWindow, defaultConnWin)
	cfg.BurstBytes = pick(cfg.BurstBytes, defaultBurst)
	cfg.MaxStreams = pick(cfg.MaxStreams, defaultMaxStreams)
	cfg.Tick = pickDur(cfg.Tick, defaultTick)
	cfg.KeepAlive = pickDur(cfg.KeepAlive, defaultKeepAlive)
	cfg.IdleTimeout = pickDur(cfg.IdleTimeout, defaultIdleTimeout)
	return cfg
}

// OpenStream opens a stream. The peer learns of it at once, with its
// receive window, and accepts it before any data arrives.
//
// ai-generated: whole function.
func (c *Conn) OpenStream() (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	if c.localOpen >= c.cfg.MaxStreams || c.nextID > maxStreamID {
		return nil, ErrTooManyStreams
	}
	s := c.newStream(c.nextID)
	c.nextID += 2
	c.localOpen++
	c.kick()
	return s, nil
}

// AcceptStream waits for the next stream the peer opened.
//
// ai-generated: whole function.
func (c *Conn) AcceptStream() (*Stream, error) {
	for {
		c.mu.Lock()
		if len(c.acceptq) > 0 {
			s := c.acceptq[0]
			c.acceptq[0] = nil
			c.acceptq = c.acceptq[1:]
			if len(c.acceptq) > 0 {
				notify(c.acceptCh)
			}
			c.mu.Unlock()
			return s, nil
		}
		err := c.err
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		select {
		case <-c.acceptCh:
		case <-c.closedCh:
		}
	}
}

// NumStreams reports the streams not yet finished both ways.
//
// ai-generated: whole function.
func (c *Conn) NumStreams() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.streams)
}

// IsClosed reports whether the connection is closed.
//
// ai-generated: whole function.
func (c *Conn) IsClosed() bool {
	select {
	case <-c.closedCh:
		return true
	default:
		return false
	}
}

// Close closes the connection: a best-effort CLOSE goes to the peer and
// every stream fails. It is safe to call more than once.
//
// ai-generated: whole function.
func (c *Conn) Close() error {
	c.mu.Lock()
	c.closeWith(ErrClosed, true)
	c.mu.Unlock()
	t := time.NewTimer(closeWait)
	defer t.Stop()
	select {
	case <-c.done:
	case <-t.C:
	}
	return nil
}

// Stats returns a snapshot of the transport state.
//
// ai-generated: whole function.
func (c *Conn) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		SRTT:                 c.rtt.smoothed(),
		MinRTT:               c.cc.minRTT,
		BtlBw:                c.cc.bw.get(),
		PacingRate:           c.cc.pacingRate(),
		Cwnd:                 c.cc.cwnd(),
		BytesInFlight:        c.inflight,
		PacketsSent:          c.nSent,
		PacketsLost:          c.nLost,
		PacketsRetransmitted: c.nRetx,
		State:                c.cc.stateName(),
	}
}

// closeWith closes the connection with err; notifyPeer queues a CLOSE.
//
// ai-generated: whole function.
func (c *Conn) closeWith(err error, notifyPeer bool) {
	if c.err != nil {
		return
	}
	c.err = err
	c.sendClose = notifyPeer
	close(c.closedCh)
	c.kick()
}

// now is the time since the connection started.
//
// ai-generated: whole function.
func (c *Conn) now() time.Duration {
	return time.Since(c.epoch)
}

// kick wakes the loop.
//
// ai-generated: whole function.
func (c *Conn) kick() {
	notify(c.wake)
}

// logf passes a diagnostic line to Config.Logf.
//
// ai-generated: whole function.
func (c *Conn) logf(format string, args ...any) {
	if c.cfg.Logf != nil {
		c.cfg.Logf(format, args...)
	}
}

// notify signals ch without blocking.
//
// ai-generated: whole function.
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// u64 converts a length or count, never negative, to uint64.
//
// ai-generated: whole function.
func u64(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}
