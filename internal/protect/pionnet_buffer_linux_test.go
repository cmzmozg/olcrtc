// SPDX-License-Identifier: Apache-2.0

package protect

import (
	"net"
	"syscall"
	"testing"
)

// ai-generated: read the effective Linux socket budget without changing host sysctl.
func udpReadBudget(t *testing.T, conn *net.UDPConn) int {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var size int
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		size, socketErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	}); err != nil {
		t.Fatal(err)
	}
	if socketErr != nil {
		t.Fatal(socketErr)
	}
	return size
}

// ai-generated: cover every UDP socket entry point and retain defaults on the original adapter.
func TestProtectedNetUDPReadBudget(t *testing.T) {
	t.Parallel()
	target, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	defaultSize := udpReadBudget(t, target)
	base := &ProtectedNet{}
	const requested = 32 << 10
	configured := base.WithUDPReadBuffer(requested)
	remote, ok := target.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatal("unexpected UDP address")
	}
	openers := map[string]func(*ProtectedNet) (any, error){
		"ListenPacket": func(n *ProtectedNet) (any, error) { return n.ListenPacket("udp4", "127.0.0.1:0") },
		"ListenUDP": func(n *ProtectedNet) (any, error) {
			return n.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		},
		"DialUDP": func(n *ProtectedNet) (any, error) { return n.DialUDP("udp4", nil, remote) },
		"Dial":    func(n *ProtectedNet) (any, error) { return n.Dial("udp4", remote.String()) },
	}
	for name, open := range openers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, n := range []*ProtectedNet{base, configured} {
				conn, err := open(n)
				if err != nil {
					t.Fatal(err)
				}
				udp, ok := conn.(*net.UDPConn)
				if !ok {
					t.Fatalf("unexpected connection %T", conn)
				}
				t.Cleanup(func() { _ = udp.Close() })
				want := defaultSize
				if n == configured {
					want = 2 * requested
				}
				if size := udpReadBudget(t, udp); size != want {
					t.Fatalf("SO_RCVBUF=%d, want %d", size, want)
				}
			}
		})
	}
	if base.udpReadBuffer != 0 {
		t.Fatal("configuring one adapter mutated another")
	}
}
