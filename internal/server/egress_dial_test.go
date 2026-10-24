package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

// ai-generated: verify delayed IPv4 fallback, one DNS lookup and cancellation of the blocked dial.
func TestDialCheckedTargetsBypassesSlowFirstAddress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lookup := &stubLookup{answers: map[string][]net.IP{
			testTwoAddrsHost: {net.ParseIP(testPublicIP), net.ParseIP(testPublicIPAlt)},
		}}
		var canceled atomic.Bool
		s := &Server{resolver: lookup}
		s.dialTarget = func(ctx context.Context, network, address string) (net.Conn, error) {
			if network != "tcp4" {
				t.Errorf("network = %s", network)
			}
			if address == net.JoinHostPort(testPublicIP, "443") {
				<-ctx.Done()
				canceled.Store(true)
				return nil, ctx.Err()
			}
			left, right := net.Pipe()
			t.Cleanup(func() { _ = right.Close() })
			return left, nil
		}
		start := time.Now()
		conn, err := s.dialDirect(context.Background(), ConnectRequest{Addr: testTwoAddrsHost, Port: 443})
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		synctest.Wait()
		if elapsed := time.Since(start); elapsed != targetFallbackDelay {
			t.Fatalf("fallback took %s, want %s", elapsed, targetFallbackDelay)
		}
		if lookup.calls() != 1 || !canceled.Load() {
			t.Fatalf("lookups=%d loser canceled=%t", lookup.calls(), canceled.Load())
		}
	})
}

// ai-generated: ensure a successful dial that arrives after the winner cannot leak a connection.
func TestDialCheckedTargetsClosesLateSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		late, peer := net.Pipe()
		defer func() { _ = late.Close(); _ = peer.Close() }()
		release := make(chan struct{})
		dial := func(_ context.Context, _, address string) (net.Conn, error) {
			if address == net.JoinHostPort(testPublicIP, "443") {
				<-release
				return late, nil
			}
			left, right := net.Pipe()
			t.Cleanup(func() { _ = right.Close() })
			return left, nil
		}
		addrs := []netip.Addr{netip.MustParseAddr(testPublicIP), netip.MustParseAddr(testPublicIPAlt)}
		conn, err := dialCheckedTargets(context.Background(), dial, addrs, "443")
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		close(release)
		synctest.Wait()
		_ = peer.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("late connection stayed open: %v", err)
		}
	})
}

// ai-generated: verify that all checked addresses are attempted with at most two active dials.
func TestDialCheckedTargetsBoundsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var active, peak, calls atomic.Int32
		dial := func(_ context.Context, _, _ string) (net.Conn, error) {
			calls.Add(1)
			current := active.Add(1)
			for previous := peak.Load(); current > previous; previous = peak.Load() {
				if peak.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(time.Second)
			active.Add(-1)
			return nil, syscall.ECONNREFUSED
		}
		addrs := []netip.Addr{
			netip.MustParseAddr(testPublicIP), netip.MustParseAddr(testPublicIPAlt),
			netip.MustParseAddr("93.184.216.35"), netip.MustParseAddr("1.0.0.1"),
		}
		_, err := dialCheckedTargets(context.Background(), dial, addrs, "443")
		synctest.Wait()
		if !errors.Is(err, syscall.ECONNREFUSED) || calls.Load() != 4 || peak.Load() != 2 || active.Load() != 0 {
			t.Fatalf("error=%v calls=%d peak=%d active=%d", err, calls.Load(), peak.Load(), active.Load())
		}
	})
}

// ai-generated: a common deadline bounds all addresses and honors an earlier caller deadline.
func TestDialCheckedTargetsBoundsTotalTime(t *testing.T) {
	for _, budget := range []time.Duration{targetDialBudget, 100 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), budget)
				defer cancel()
				var calls atomic.Int32
				dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
					calls.Add(1)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				addrs := []netip.Addr{netip.MustParseAddr(testPublicIP), netip.MustParseAddr(testPublicIPAlt)}
				start := time.Now()
				_, err := dialCheckedTargets(ctx, dial, addrs, "443")
				synctest.Wait()
				wantCalls := int32(1)
				if budget > targetFallbackDelay {
					wantCalls = 2
				}
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != budget || calls.Load() != wantCalls {
					t.Fatalf("error=%v elapsed=%s calls=%d", err, time.Since(start), calls.Load())
				}
			})
		})
	}
}
