package tunnelcore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/hostprofile"
	"github.com/openlibrecommunity/olcrtc/internal/olt"
)

// The olt data plane carries the tunnel's streams over the transport's lossy
// datagram lane instead of smux over a reliable lane: one olt packet per
// datagram, sealed on its own under OLTRecordAAD and marked with OLTMagic so
// the datagram handlers tell it from a relayed UDP datagram before opening
// anything. olt orders each stream by itself, so a loss holds only the
// stream it hit.
//
// ai-generated: the whole file (olt data plane).
const (
	// OLTRecordAAD is the associated data of every sealed olt packet.
	OLTRecordAAD = "olcrtc/olt/v1"
	// OLTMagic leads an olt packet on the datagram lane.
	OLTMagic = "OLT1"
	// OLTMaxDatagram is the largest datagram an olt packet makes: what
	// vp8channel keeps inside one RTP packet (MaxAlignedDatagramSize), so a
	// lost RTP packet costs one olt packet and its FEC can rebuild it.
	OLTMaxDatagram = 1138
	// oltTick is the carrier's frame interval at 60 fps.
	oltTick = time.Second / 60
)

// OLTSession adapts an olt connection to StreamSession.
type OLTSession struct{ *olt.Conn }

var _ StreamSession = OLTSession{}

// OpenStream opens an olt stream.
func (s OLTSession) OpenStream() (Stream, error) {
	stream, err := s.Conn.OpenStream()
	if err != nil {
		return nil, err //nolint:wrapcheck // logged as olt reports it, like smux's
	}
	return stream, nil
}

// AcceptStream accepts the peer's next olt stream.
func (s OLTSession) AcceptStream() (Stream, error) {
	stream, err := s.Conn.AcceptStream()
	if err != nil {
		return nil, err //nolint:wrapcheck // logged as olt reports it, like smux's
	}
	return stream, nil
}

// OLTConnID is the connection id both ends derive from the session id the
// handshake agreed on.
func OLTConnID(sessionID string) uint64 {
	sum := sha256.Sum256([]byte("olcrtc/olt/conn/" + sessionID))
	return binary.BigEndian.Uint64(sum[:8])
}

// NewOLT starts the olt connection of a session over send, which puts one
// datagram on the transport's lossy lane. burst is the most the transport
// writes in one frame.
func NewOLT(keys *crypto.KeySet, send func([]byte) error, client bool, sessionID string, burst int) (*olt.Conn, error) {
	aad := []byte(OLTRecordAAD)
	streamWindow, connWindow := 1<<20, 8<<20
	if hostprofile.BuffersAreConstrained() {
		streamWindow, connWindow = 512<<10, 4<<20
	}
	conn, err := olt.New(olt.Config{
		Send: func(p []byte) error {
			out := make([]byte, len(OLTMagic), len(OLTMagic)+len(p)+crypto.WireOverhead)
			copy(out, OLTMagic)
			sealed, err := keys.SealInto(out, p, aad)
			if err != nil {
				return fmt.Errorf("seal olt packet: %w", err)
			}
			return send(sealed)
		},
		MaxPacket:    OLTMaxDatagram - len(OLTMagic) - crypto.WireOverhead,
		Client:       client,
		ConnID:       OLTConnID(sessionID),
		StreamWindow: streamWindow,
		ConnWindow:   connWindow,
		Tick:         oltTick,
		BurstBytes:   burst,
	})
	if err != nil {
		return nil, fmt.Errorf("olt: %w", err)
	}
	return conn, nil
}

// OLTPacket reports whether datagram is an olt packet, and returns it opened
// with keys when it is one that authenticates.
func OLTPacket(keys *crypto.KeySet, datagram []byte) ([]byte, bool) {
	if !bytes.HasPrefix(datagram, []byte(OLTMagic)) {
		return nil, false
	}
	if keys == nil {
		return nil, true
	}
	packet, err := keys.OpenInto(nil, datagram[len(OLTMagic):], []byte(OLTRecordAAD))
	if err != nil {
		return nil, true
	}
	return packet, true
}
