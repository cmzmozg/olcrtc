package tunnelcore

import (
	"fmt"
	"net"

	"github.com/xtaci/smux"
)

// Stream is one tunnel stream: an smux stream, or a stream of an olt
// connection. CloseWrite ends this side's half and leaves the read side open.
//
// ai-generated: the whole file (olt data plane).
type Stream interface {
	net.Conn
	ID() uint32
	CloseWrite() error
}

// StreamSession opens and accepts the streams of one tunnel connection.
type StreamSession interface {
	OpenStream() (Stream, error)
	AcceptStream() (Stream, error)
	IsClosed() bool
	NumStreams() int
	Close() error
}

// SmuxSession adapts an smux session to StreamSession.
type SmuxSession struct{ *smux.Session }

var _ StreamSession = SmuxSession{}

// WrapSmux returns session as a StreamSession, nil for a nil session.
func WrapSmux(session *smux.Session) StreamSession {
	if session == nil {
		return nil
	}
	return SmuxSession{session}
}

// OpenStream opens an smux stream.
func (s SmuxSession) OpenStream() (Stream, error) {
	stream, err := s.Session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("smux open stream: %w", err)
	}
	return stream, nil
}

// AcceptStream accepts the peer's next smux stream.
func (s SmuxSession) AcceptStream() (Stream, error) {
	stream, err := s.Session.AcceptStream()
	if err != nil {
		return nil, fmt.Errorf("smux accept stream: %w", err)
	}
	return stream, nil
}

// Close closes the smux session.
func (s SmuxSession) Close() error {
	if err := s.Session.Close(); err != nil {
		return fmt.Errorf("smux close: %w", err)
	}
	return nil
}
