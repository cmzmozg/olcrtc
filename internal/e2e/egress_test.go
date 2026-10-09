package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// ai-generated: the whole file (egress hardening).

// A CONNECT to the exit's own loopback is refused end to end: the server
// acks host unreachable and never dials. The exit announces early data, so
// the client has answered the SOCKS5 CONNECT with success already and the
// refusal reaches the app as a stream that ends before the target says
// anything. The same echo server answers through a tunnel whose egress
// policy is lifted (TestClientServerSOCKSTunnelOverMemoryDatachannel).
func TestClientServerSOCKSConnectBlocksPrivateTargetByDefault(t *testing.T) {
	echoAddr := startEchoServer(t)
	rt := startMemoryTunnel(t, transportData, false)
	defer rt.stop(t)

	conn, code := socksConnect(t, rt.socksAddr, echoAddr)
	defer func() { _ = conn.Close() }()
	if code != 0 {
		t.Fatalf("socks reply = %d, want 0: the exit takes early data", code)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write early data: %v", err)
	}
	echo := make([]byte, 4)
	if n, err := io.ReadFull(conn, echo); err == nil {
		t.Fatalf("read %q through a loopback target the exit must refuse", echo[:n])
	}
}

// socksConnect sends one SOCKS5 CONNECT for an IPv4 target and returns the
// connection and the reply code, success or not.
func socksConnect(t *testing.T, socksAddr, targetAddr string) (net.Conn, byte) {
	t.Helper()
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(context.Background(), "tcp4", socksAddr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err = conn.Write([]byte{5, 1, 0}); err != nil {
		t.Fatalf("write socks greeting: %v", err)
	}
	greeting := make([]byte, 2)
	if _, err = io.ReadFull(conn, greeting); err != nil || !bytes.Equal(greeting, []byte{5, 0}) {
		t.Fatalf("socks greeting = %v, %v; want [5 0]", greeting, err)
	}
	host, portText, err := net.SplitHostPort(targetAddr)
	if err != nil {
		t.Fatalf("split target addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse target port: %v", err)
	}
	req := append([]byte{5, 1, 0, 1}, net.ParseIP(host).To4()...)
	req = binary.BigEndian.AppendUint16(req, uint16(port)) //nolint:gosec // SOCKS5 port is uint16 by definition
	if _, err = conn.Write(req); err != nil {
		t.Fatalf("write socks connect: %v", err)
	}
	reply := make([]byte, 10)
	if _, err = io.ReadFull(conn, reply); err != nil {
		t.Fatalf("read socks connect reply: %v", err)
	}
	return conn, reply[1]
}
