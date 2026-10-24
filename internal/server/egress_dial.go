package server

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"
)

const (
	targetFallbackDelay = 250 * time.Millisecond
	targetDialBudget    = 10 * time.Second
)

// targetDialResult is one worker's outcome after trying its checked addresses.
// ai-generated: carry a bounded server egress dial result.
type targetDialResult struct {
	conn net.Conn
	err  error
}

// dialCheckedTargets races at most two workers over already validated literal IPs.
// ai-generated: bounded IPv4 failover with cancellation and loser cleanup.
func dialCheckedTargets(ctx context.Context, dial func(context.Context, string, string) (net.Conn, error),
	addrs []netip.Addr, port string,
) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, targetDialBudget)
	defer cancel()
	if len(addrs) == 1 {
		conn, err := dial(dialCtx, "tcp4", net.JoinHostPort(addrs[0].String(), port))
		if err != nil {
			return nil, fmt.Errorf("dial target: %w", err)
		}
		return conn, nil
	}
	results := make(chan targetDialResult)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		dialTargetWorker(dialCtx, dial, addrs, port, 0, results)
	}()
	go func() {
		timer := time.NewTimer(targetFallbackDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-firstDone:
		case <-dialCtx.Done():
			return
		}
		dialTargetWorker(dialCtx, dial, addrs, port, 1, results)
	}()
	var firstErr error
	for range 2 {
		select {
		case result := <-results:
			if result.err == nil {
				return result.conn, nil
			}
			if firstErr == nil {
				firstErr = result.err
			}
		case <-dialCtx.Done():
			return nil, fmt.Errorf("dial targets: %w", dialCtx.Err())
		}
	}
	return nil, fmt.Errorf("dial targets: %w", firstErr)
}

// dialTargetWorker tries every other checked address and closes any late success.
// ai-generated: keep concurrency fixed and release sockets after cancellation.
func dialTargetWorker(ctx context.Context, dial func(context.Context, string, string) (net.Conn, error),
	addrs []netip.Addr, port string, offset int, results chan<- targetDialResult,
) {
	var result targetDialResult
	for i := offset; i < len(addrs); i += 2 {
		if ctx.Err() != nil {
			return
		}
		result.conn, result.err = dial(ctx, "tcp4", net.JoinHostPort(addrs[i].String(), port))
		if result.err == nil {
			break
		}
	}
	select {
	case results <- result:
	case <-ctx.Done():
		if result.conn != nil {
			_ = result.conn.Close()
		}
	}
}
