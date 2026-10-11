package olt

import (
	"bytes"
	"math"
	"slices"
	"sort"
)

const (
	// initStreamWin and initConnWin are the windows a sender may fill
	// before the receiver's first update: part of the protocol.
	initStreamWin = 256 << 10
	initConnWin   = 1 << 20
	// smallStream is what a stream may send lately and still go before the
	// bulk ones.
	smallStream = 64 << 10
	// peerSeenRanges bounds the record of peer stream ids seen.
	peerSeenRanges = 1024
	// maxStreamID is the last id OpenStream hands out.
	maxStreamID = math.MaxUint32 - 2
	// Stream reset codes.
	codeStop    = 0
	codeRefused = 1
)

// newStream creates stream id with a full receive window and queues the
// MAX_STREAM_DATA that announces it.
//
// ai-generated: whole function.
func (c *Conn) newStream(id uint32) *Stream {
	s := &Stream{
		c:         c,
		id:        id,
		rdCh:      make(chan struct{}, 1),
		wrCh:      make(chan struct{}, 1),
		sndMax:    initStreamWin,
		rcvMax:    c.streamWin,
		needMaxSD: true,
	}
	c.streams[id] = s
	c.queueCtrl(s)
	return s
}

// stream returns stream id, opening it when the peer names a new one. It
// returns nil for a finished stream, a local one never opened, or one
// refused for MaxStreams.
//
// ai-generated: whole function.
func (c *Conn) stream(id uint32) *Stream {
	if s := c.streams[id]; s != nil {
		return s
	}
	if c.isLocal(id) {
		return nil
	}
	idx := uint64(id >> 1)
	if c.peerSeen.contains(idx) {
		return nil
	}
	c.peerSeen.add(idx, idx+1)
	c.peerSeen.trim(peerSeenRanges)
	if c.peerOpen >= c.cfg.MaxStreams {
		c.refuse = append(c.refuse, id)
		c.logf("olt: refusing stream %d: %d open", id, c.peerOpen)
		return nil
	}
	s := c.newStream(id)
	c.peerOpen++
	c.acceptq = append(c.acceptq, s)
	notify(c.acceptCh)
	return s
}

// isLocal reports whether this side opens stream id.
//
// ai-generated: whole function.
func (c *Conn) isLocal(id uint32) bool { return (id&1 == 1) == c.cfg.Client }

// sendBufMax bounds a stream's unacknowledged bytes.
//
// ai-generated: whole function.
func (c *Conn) sendBufMax() int { return int(c.streamWin) } //nolint:gosec // G115: from an int

// queueSend marks s as having new data or a FIN to send.
//
// ai-generated: whole function.
func (c *Conn) queueSend(s *Stream) {
	if !s.inSend && !s.gone {
		s.inSend = true
		c.sendq = append(c.sendq, s)
	}
}

// queueRetx marks s as having data to send again.
//
// ai-generated: whole function.
func (c *Conn) queueRetx(s *Stream) {
	if !s.inRetx && !s.gone {
		s.inRetx = true
		c.retxq = append(c.retxq, s)
	}
}

// queueCtrl marks s as having control frames to send.
//
// ai-generated: whole function.
func (c *Conn) queueCtrl(s *Stream) {
	if !s.inCtrl && !s.gone {
		s.inCtrl = true
		c.ctrlq = append(c.ctrlq, s)
	}
}

// maybeRetire drops s from the stream map once it is finished both ways.
//
// ai-generated: whole function.
func (c *Conn) maybeRetire(s *Stream) {
	if s.gone || !s.sendDone() || !s.recvDone() {
		return
	}
	s.gone = true
	delete(c.streams, s.id)
	if c.isLocal(s.id) {
		c.localOpen--
	} else {
		c.peerOpen--
	}
	s.snd, s.lost, s.ooo = byteQueue{}, rangeSet{}, nil
}

// stopReading discards what s holds and will receive, and asks the peer to
// stop when it has not finished.
//
// ai-generated: whole function.
func (c *Conn) stopReading(s *Stream) {
	if s.stopped || s.rcvReset {
		return
	}
	s.stopped, s.needMaxSD = true, false
	c.release(s.rcvHigh - s.readOff)
	s.readOff = s.rcvHigh
	s.rcv, s.ooo = byteQueue{}, nil
	if !s.finRcvd {
		s.needStop = true
		c.queueCtrl(s)
	}
}

// consume takes n read bytes off s and opens the windows when half used.
//
// ai-generated: whole function.
func (c *Conn) consume(s *Stream, n int) {
	s.rcv.Drop(u64(n))
	s.readOff += u64(n)
	c.release(u64(n))
	if !s.finRcvd && s.readOff+c.streamWin/2 > s.rcvMax {
		s.rcvMax = s.readOff + c.streamWin
		s.needMaxSD = true
		c.queueCtrl(s)
		c.kick()
	}
	c.maybeRetire(s)
}

// release counts n bytes consumed against the connection window.
//
// ai-generated: whole function.
func (c *Conn) release(n uint64) {
	c.read += n
	if c.read+c.connWin/2 > c.rcvMax {
		c.rcvMax = c.read + c.connWin
		c.needMaxData = true
		c.kick()
	}
}

// onStream takes in a STREAM frame. Data past either window is dropped.
//
// ai-generated: whole function.
func (c *Conn) onStream(s *Stream, off uint64, data []byte, fin bool) {
	if s.rcvReset {
		return
	}
	end := off + u64(len(data))
	if fin {
		if (s.finRcvd && end != s.finalSize) || end < s.rcvHigh {
			return
		}
		s.finRcvd, s.finalSize, s.needMaxSD = true, end, false
	}
	if (s.finRcvd && end > s.finalSize) || end > s.rcvMax {
		return
	}
	if end > s.rcvHigh {
		if c.rcvd+end-s.rcvHigh > c.rcvMax {
			return
		}
		c.rcvd += end - s.rcvHigh
		s.rcvHigh = end
	}
	if s.stopped {
		c.release(s.rcvHigh - s.readOff)
		s.readOff = s.rcvHigh
	} else {
		s.insert(off, data)
		if s.rcv.Len() > 0 || s.finRcvd {
			notify(s.rdCh)
		}
	}
	c.maybeRetire(s)
}

// onReset takes in the peer's RESET: the peer sends no more, and what s
// holds is discarded.
//
// ai-generated: whole function.
func (c *Conn) onReset(s *Stream, code, finalSize uint64) {
	if s.rcvReset {
		return
	}
	if finalSize > s.rcvHigh {
		c.rcvd += finalSize - s.rcvHigh
		s.rcvHigh = finalSize
	}
	c.release(s.rcvHigh - s.readOff)
	s.readOff = s.rcvHigh
	s.rcvReset, s.rcvCode = true, code
	s.needMaxSD, s.needStop = false, false
	s.rcv, s.ooo = byteQueue{}, nil
	notify(s.rdCh)
	c.maybeRetire(s)
}

// onStop takes in the peer's STOP: the send side is abandoned and a RESET
// tells the peer so.
//
// ai-generated: whole function.
func (c *Conn) onStop(s *Stream, code uint64) {
	if s.sndReset || s.sendDone() {
		return
	}
	s.sndReset, s.needReset, s.resetCode = true, true, code
	s.snd, s.lost, s.finLost = byteQueue{}, rangeSet{}, false
	c.queueCtrl(s)
	notify(s.wrCh)
}

// insert puts stream data in place: readable when it continues rcvOff,
// kept aside when it comes early.
//
// ai-generated: whole function.
func (s *Stream) insert(off uint64, data []byte) {
	end := off + u64(len(data))
	if end <= s.rcvOff {
		return
	}
	if off > s.rcvOff {
		s.stash(off, data)
		return
	}
	s.rcv.Push(data[s.rcvOff-off:])
	s.rcvOff = end
	k := 0
	for k < len(s.ooo) && s.ooo[k].off <= s.rcvOff {
		if ch := s.ooo[k]; ch.end() > s.rcvOff {
			s.rcv.Push(ch.data[s.rcvOff-ch.off:])
			s.rcvOff = ch.end()
		}
		k++
	}
	if k > 0 {
		s.ooo = slices.Delete(s.ooo, 0, k)
	}
}

// stash keeps a copy of early data, unless a kept chunk covers it.
//
// ai-generated: whole function.
func (s *Stream) stash(off uint64, data []byte) {
	end := off + u64(len(data))
	i := sort.Search(len(s.ooo), func(k int) bool { return s.ooo[k].off >= off })
	if i < len(s.ooo) && s.ooo[i].off == off && s.ooo[i].end() >= end {
		return
	}
	if i > 0 && s.ooo[i-1].end() >= end {
		return
	}
	s.ooo = slices.Insert(s.ooo, i, chunk{off: off, data: bytes.Clone(data)})
}
