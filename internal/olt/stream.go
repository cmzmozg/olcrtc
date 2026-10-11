package olt

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// Stream is one bidirectional byte stream of a Conn. It implements net.Conn.
type Stream struct {
	c    *Conn
	id   uint32
	rdCh chan struct{}
	wrCh chan struct{}

	// Everything below is guarded by c.mu.
	rdl, wdl time.Time
	closed   bool // the app called Close
	gone     bool // finished both ways and dropped from the stream map

	// Send side. acked.floor is the first unacknowledged offset, where snd
	// starts.
	snd       byteQueue
	acked     rangeSet
	lost      rangeSet
	sndNext   uint64 // the next offset never sent
	sndMax    uint64 // the peer's limit
	recent    uint64 // bytes sent lately, halved every second
	recentAt  time.Duration
	finQueued bool
	finSent   bool
	finLost   bool
	finAcked  bool
	// sndReset: the send side was abandoned at the peer's STOP; needReset
	// queues the RESET that says so, resetAcked ends the send side.
	sndReset   bool
	needReset  bool
	resetAcked bool
	resetCode  uint64
	inSend     bool
	inRetx     bool
	inCtrl     bool

	// Receive side. rcv holds the readable bytes from readOff to rcvOff;
	// ooo holds the data past rcvOff that came early.
	rcv       byteQueue
	ooo       []chunk
	readOff   uint64
	rcvOff    uint64
	rcvHigh   uint64
	rcvMax    uint64
	finalSize uint64
	finRcvd   bool
	rcvReset  bool
	rcvCode   uint64
	stopped   bool // incoming data is discarded
	needStop  bool
	needMaxSD bool
}

// timeoutError is the error of an operation past its deadline.
type timeoutError struct{}

// Error describes the timeout.
//
// ai-generated: whole function.
func (timeoutError) Error() string { return "olt: i/o timeout" }

// Timeout reports true, as net.Error asks.
//
// ai-generated: whole function.
func (timeoutError) Timeout() bool { return true }

// Temporary reports true, as net.Error asks.
//
// ai-generated: whole function.
func (timeoutError) Temporary() bool { return true }

// Unwrap makes the error match os.ErrDeadlineExceeded.
//
// ai-generated: whole function.
func (timeoutError) Unwrap() error { return os.ErrDeadlineExceeded }

// addr is the fixed address of every stream.
type addr struct{}

// Network names the transport.
//
// ai-generated: whole function.
func (addr) Network() string { return "olt" }

// String names the transport.
//
// ai-generated: whole function.
func (addr) String() string { return "olt" }

// ID returns the stream id.
//
// ai-generated: whole function.
func (s *Stream) ID() uint32 { return s.id }

// LocalAddr returns a fixed placeholder address.
//
// ai-generated: whole function.
func (s *Stream) LocalAddr() net.Addr { return addr{} }

// RemoteAddr returns a fixed placeholder address.
//
// ai-generated: whole function.
func (s *Stream) RemoteAddr() net.Addr { return addr{} }

// Read reads the stream's data in order; io.EOF follows the peer's FIN.
//
// ai-generated: whole function.
func (s *Stream) Read(b []byte) (int, error) {
	c := s.c
	for {
		c.mu.Lock()
		n, done, err := s.readLocked(b)
		dl := s.rdl
		c.mu.Unlock()
		if done {
			return n, err
		}
		if err := s.wait(s.rdCh, dl); err != nil {
			return 0, err
		}
	}
}

// readLocked reads what is there; done is false when Read must wait.
//
// ai-generated: whole function.
func (s *Stream) readLocked(b []byte) (int, bool, error) {
	switch {
	case len(b) == 0:
		return 0, true, nil
	case s.rcv.Len() > 0:
		n := copy(b, s.rcv.Bytes())
		s.c.consume(s, n)
		if s.rcv.Len() > 0 {
			notify(s.rdCh)
		}
		return n, true, nil
	case s.rcvReset:
		return 0, true, resetErr("read", s.rcvCode)
	case s.closed:
		return 0, true, fmt.Errorf("read: %w", ErrClosed)
	case s.finRcvd && s.readOff == s.finalSize:
		return 0, true, io.EOF
	case s.c.err != nil:
		return 0, true, s.c.err
	}
	return 0, false, nil
}

// Write queues b on the stream, waiting while the send buffer is full.
//
// ai-generated: whole function.
func (s *Stream) Write(b []byte) (int, error) {
	c := s.c
	n := 0
	for {
		c.mu.Lock()
		if err := s.writeErr(); err != nil {
			c.mu.Unlock()
			return n, err
		}
		if k := min(len(b), c.sendBufMax()-s.snd.Len()); k > 0 {
			s.snd.Push(b[:k])
			b, n = b[k:], n+k
			c.queueSend(s)
			c.kick()
		}
		dl := s.wdl
		c.mu.Unlock()
		if len(b) == 0 {
			return n, nil
		}
		if err := s.wait(s.wrCh, dl); err != nil {
			return n, err
		}
	}
}

// writeErr is why Write cannot go on, if it cannot.
//
// ai-generated: whole function.
func (s *Stream) writeErr() error {
	switch {
	case s.c.err != nil:
		return s.c.err
	case s.sndReset:
		return resetErr("write", s.resetCode)
	case s.closed:
		return fmt.Errorf("write: %w", ErrClosed)
	case s.finQueued:
		return ErrWriteClosed
	}
	return nil
}

// wait blocks until ch is signalled, the connection closes or dl passes.
// It returns the timeout error only when dl had passed before it began, so
// the caller sees a deadline moved meanwhile.
//
// ai-generated: whole function.
func (s *Stream) wait(ch chan struct{}, dl time.Time) error {
	if dl.IsZero() {
		select {
		case <-ch:
		case <-s.c.closedCh:
		}
		return nil
	}
	d := time.Until(dl)
	if d <= 0 {
		return timeoutError{}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-s.c.closedCh:
	case <-t.C:
	}
	return nil
}

// CloseWrite ends the send side: FIN follows the queued data, and reads go
// on.
//
// ai-generated: whole function.
func (s *Stream) CloseWrite() error {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case s.sndReset:
		return resetErr("close write", s.resetCode)
	case s.finQueued:
		return nil
	case c.err != nil:
		return c.err
	}
	s.finQueued = true
	c.queueSend(s)
	c.kick()
	notify(s.wrCh)
	return nil
}

// Close closes both directions: FIN follows the queued data, unless sent
// already, and incoming data is discarded from now on; the peer is told to
// stop sending when it has not finished.
//
// ai-generated: whole function.
func (s *Stream) Close() error {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if !s.finQueued && !s.sndReset {
		s.finQueued = true
		c.queueSend(s)
	}
	c.stopReading(s)
	c.maybeRetire(s)
	c.kick()
	notify(s.rdCh)
	notify(s.wrCh)
	return nil
}

// SetDeadline sets both deadlines.
//
// ai-generated: whole function.
func (s *Stream) SetDeadline(t time.Time) error { return s.setDeadlines(t, true, true) }

// SetReadDeadline sets the read deadline.
//
// ai-generated: whole function.
func (s *Stream) SetReadDeadline(t time.Time) error { return s.setDeadlines(t, true, false) }

// SetWriteDeadline sets the write deadline.
//
// ai-generated: whole function.
func (s *Stream) SetWriteDeadline(t time.Time) error { return s.setDeadlines(t, false, true) }

// setDeadlines sets the read or write deadline, or both, and wakes the
// calls waiting on them. A closed stream refuses.
//
// ai-generated: whole function.
func (s *Stream) setDeadlines(t time.Time, read, write bool) error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if s.closed {
		return fmt.Errorf("set deadline: %w", ErrClosed)
	}
	if read {
		s.rdl = t
		notify(s.rdCh)
	}
	if write {
		s.wdl = t
		notify(s.wrCh)
	}
	return nil
}

// resetErr is the error of a stream the peer reset with code.
//
// ai-generated: whole function.
func resetErr(op string, code uint64) error {
	if code == codeRefused {
		return fmt.Errorf("%s: %w", op, ErrStreamRefused)
	}
	return fmt.Errorf("%s: %w (code %d)", op, ErrStreamReset, code)
}

// sndEnd is the offset after the last byte written.
//
// ai-generated: whole function.
func (s *Stream) sndEnd() uint64 { return s.acked.floor + u64(s.snd.Len()) }

// sendDone reports that everything, FIN included, was acknowledged, or
// that the send side was abandoned and the peer knows.
//
// ai-generated: whole function.
func (s *Stream) sendDone() bool { return (s.finAcked && s.snd.Len() == 0) || s.resetAcked }

// recvDone reports that nothing more is wanted from the peer.
//
// ai-generated: whole function.
func (s *Stream) recvDone() bool {
	return s.rcvReset || (s.finRcvd && (s.stopped || s.readOff == s.finalSize))
}

// small reports whether the stream sent less than smallStream lately; the
// count halves every second.
//
// ai-generated: whole function.
func (s *Stream) small(now time.Duration) bool {
	for now-s.recentAt >= time.Second && s.recent > 0 {
		s.recent >>= 1
		s.recentAt += time.Second
	}
	if s.recent == 0 {
		s.recentAt = now
	}
	return s.recent < smallStream
}
