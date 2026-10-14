package vp8channel

// ai-generated: the whole file (rtp pacing on the server's track).

import (
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	// rtpPaceRate is the rate, in bytes a second, the server's track lets
	// RTP packets out at. pion writes a sample's packets back to back, some
	// fifty for a full one, and they leave the VPS at line rate for the
	// relay to forward onto a phone's downlink of a few MB/s. Paced, a full
	// sample goes out over about ten milliseconds, still faster than an LTE
	// downlink drains it.
	rtpPaceRate = 6 << 20
	// rtpPaceLag is the longest a packet waits for its turn. A writer
	// faster than the pace, peers' lanes writing at once, gets its packets
	// out at most this late, back to back as before; it stays under the
	// frame interval, so the queue is empty again by the next tick.
	rtpPaceLag = 12 * time.Millisecond
	// rtpPaceSlack is how early a packet may go: the pacer writes what is
	// due within it in one wake rather than sleeping once a packet.
	rtpPaceSlack = 500 * time.Microsecond
)

// pacedTrack is the server's video track with its RTP packets paced: pion
// binds it as it does the sample track, whose writes then go through the
// pacer to the writer pion handed over.
type pacedTrack struct {
	*webrtc.TrackLocalStaticSample
	pacer *rtpPacer
}

// Bind implements webrtc.TrackLocal.
func (t *pacedTrack) Bind(ctx webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	paced := pacedContext{TrackLocalContext: ctx, w: pacedWriter{p: t.pacer, w: ctx.WriteStream()}}
	return t.TrackLocalStaticSample.Bind(paced) //nolint:wrapcheck // pion's own error, as the sample track returns it
}

// pacedContext is a binding context whose write stream is paced.
type pacedContext struct {
	webrtc.TrackLocalContext
	w webrtc.TrackLocalWriter
}

// WriteStream implements webrtc.TrackLocalContext.
func (c pacedContext) WriteStream() webrtc.TrackLocalWriter { return c.w }

// pacedWriter queues one binding's packets on the pacer.
type pacedWriter struct {
	p *rtpPacer
	w webrtc.TrackLocalWriter
}

// WriteRTP implements webrtc.TrackLocalWriter.
func (w pacedWriter) WriteRTP(header *rtp.Header, payload []byte) (int, error) {
	return w.p.write(w.w, header, payload)
}

// Write implements webrtc.TrackLocalWriter. The sample track never calls
// it; a whole packet goes out as it comes.
func (w pacedWriter) Write(b []byte) (int, error) {
	return w.w.Write(b) //nolint:wrapcheck // pion's own error, unpaced
}

// pacedPacket is a queued packet and when it is due, a monoNow value.
type pacedPacket struct {
	w       webrtc.TrackLocalWriter
	header  rtp.Header
	payload []byte
	at      int64
}

// rtpPacer lets RTP packets out at rate, each no later than lag after it
// was written, in the order they were. A packet that finds nothing queued
// and the pace idle goes out at once, on the writer's goroutine; the rest
// wait for run.
type rtpPacer struct {
	rate int64
	lag  time.Duration

	mu    sync.Mutex
	queue []pacedPacket
	head  int
	// free is when the pace is idle again, a monoNow value.
	free  int64
	spare [][]byte
	wake  chan struct{}
}

func newRTPPacer() *rtpPacer {
	return &rtpPacer{rate: rtpPaceRate, lag: rtpPaceLag, wake: make(chan struct{}, 1)}
}

// write paces one packet to w. The error is w's for a packet written at
// once, nil for a queued one.
func (p *rtpPacer) write(w webrtc.TrackLocalWriter, header *rtp.Header, payload []byte) (int, error) {
	size := header.MarshalSize() + len(payload)
	now := monoNow()
	p.mu.Lock()
	defer p.mu.Unlock()
	at := min(max(now, p.free), now+int64(p.lag))
	p.free = at + int64(size)*int64(time.Second)/p.rate
	if p.head == len(p.queue) && at <= now {
		return w.WriteRTP(header, payload) //nolint:wrapcheck // pion's own error, as the sample track returns it
	}
	p.queue = append(p.queue, pacedPacket{w: w, header: header.Clone(), payload: append(p.buf(), payload...), at: at})
	if p.head == len(p.queue)-1 {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
	return size, nil
}

// buf is a spare payload buffer, or a new one.
func (p *rtpPacer) buf() []byte {
	if n := len(p.spare); n > 0 {
		b := p.spare[n-1]
		p.spare = p.spare[:n-1]
		return b
	}
	return make([]byte, 0, rtpPacketMTU)
}

// flush writes the packets due by now and reports how long until the next
// one is, false when none waits.
func (p *rtpPacer) flush(now int64) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ; p.head < len(p.queue); p.head++ {
		pkt := &p.queue[p.head]
		if pkt.at > now+int64(rtpPaceSlack) {
			return time.Duration(pkt.at - now), true
		}
		_, _ = pkt.w.WriteRTP(&pkt.header, pkt.payload)
		p.spare = append(p.spare, pkt.payload[:0])
		*pkt = pacedPacket{}
	}
	p.queue, p.head = p.queue[:0], 0
	return 0, false
}

// run writes queued packets as they fall due, until done.
func (p *rtpPacer) run(done <-chan struct{}) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		var due <-chan time.Time
		if wait, ok := p.flush(monoNow()); ok {
			timer.Reset(wait)
			due = timer.C
		}
		select {
		case <-done:
			return
		case <-p.wake:
		case <-due:
		}
	}
}
