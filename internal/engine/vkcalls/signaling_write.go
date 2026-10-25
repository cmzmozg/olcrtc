package vkcalls

// ai-generated: cancellation-aware ownership and deadlines for signaling I/O.

import (
	"context"
	"fmt"
)

// ai-generated: cancel a queued writer without leaving a goroutine on a mutex.
func (c *signalingClient) acquireWrite(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("vkcalls: acquire signaling writer: %w", err)
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("vkcalls: acquire signaling writer: %w", ctx.Err())
	case <-c.closed:
		return ErrSignalingClosed
	case c.writeGate <- struct{}{}:
		return nil
	}
}

// ai-generated: cancel the socket while a WriteMessage is blocked, under one writer.
func (c *signalingClient) writeLocked(ctx context.Context, messageType int, data []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("vkcalls: signaling write: %w", err)
	}
	deadline, _ := ctx.Deadline()
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("vkcalls: signaling write deadline: %w", err)
	}
	stop := context.AfterFunc(ctx, c.shutdown)
	err := c.conn.WriteMessage(messageType, data)
	stop()
	if ctx.Err() != nil {
		return fmt.Errorf("vkcalls: signaling write: %w", ctx.Err())
	}
	if err != nil {
		c.shutdown()
		return fmt.Errorf("vkcalls: signaling write: %w", err)
	}
	return nil
}

// ai-generated: cancellation skips graceful I/O, and every close joins its reader.
func (c *signalingClient) closeContext(ctx context.Context) {
	if ctx.Err() == nil {
		c.hangup(ctx)
	}
	c.shutdown()
	<-c.readDone
}
