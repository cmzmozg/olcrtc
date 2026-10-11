package client

import (
	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// The olt data plane carries the session's new streams over the transport's
// lossy datagram lane, each stream ordered on its own, when the server's
// welcome announces it. The smux data session stays for the streams already
// on it and for an olt connection that has gone away.
//
// ai-generated: the whole file (olt data plane).

// oltBurst is the frame size olt allows for when the transport names none.
const oltBurst = 64 << 10

// attachOLT gives pair its olt connection when the server takes streams there.
func (c *Client) attachOLT(pair *tunnelcore.SessionPair, welcome handshake.Welcome) {
	if !welcome.Has(handshake.FeatureOLT) {
		return
	}
	dg, ok := c.ln.(transport.DatagramTransport)
	if !ok {
		return
	}
	burst := c.ln.Features().MaxPayloadSize
	if burst <= 0 {
		burst = oltBurst
	}
	conn, err := tunnelcore.NewOLT(c.keys, dg.SendDatagram, true, welcome.SessionID, burst)
	if err != nil {
		logger.Warnf("olt data plane: %v", err)
		return
	}
	pair.OLT = conn
	logger.Infof("olt data plane open")
}

// inputOLT takes a datagram for the olt data plane, and reports whether it
// was one.
func (c *Client) inputOLT(datagram []byte) bool {
	packet, ok := tunnelcore.OLTPacket(c.keys, datagram)
	if !ok {
		return false
	}
	c.sessMu.RLock()
	pair := c.pair
	c.sessMu.RUnlock()
	if packet != nil && pair != nil && pair.OLT != nil {
		pair.OLT.Input(packet)
	}
	return true
}
