package vkcalls

// ai-generated: regressions for blocked socket I/O, writer ownership and cleanup.

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// writeTrapConn blocks writes after the handshake until the socket is closed.
type writeTrapConn struct {
	net.Conn
	block   atomic.Bool
	entered chan struct{}
	closed  chan struct{}
	enter   sync.Once
	close   sync.Once
}

// ai-generated: model a blackhole without relying on kernel send-buffer sizes.
func (c *writeTrapConn) Write(data []byte) (int, error) {
	if !c.block.Load() {
		return c.Conn.Write(data)
	}
	c.enter.Do(func() { close(c.entered) })
	<-c.closed
	return 0, net.ErrClosed
}

// ai-generated: closing a trapped socket must release its blocked writer.
func (c *writeTrapConn) Close() error {
	c.close.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// ai-generated: construct a real WebSocket over a controllable network writer.
func trappedSignaling(t *testing.T) (*signalingClient, *writeTrapConn) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	var trapped *writeTrapConn
	dialer := websocket.Dialer{NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		trapped = &writeTrapConn{Conn: conn, entered: make(chan struct{}), closed: make(chan struct{})}
		return trapped, nil
	}}
	conn, response, err := dialer.DialContext(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	c := &signalingClient{
		conn: conn, writeGate: make(chan struct{}, 1), notifications: make(chan notification, 16),
		closed: make(chan struct{}), readDone: make(chan struct{}),
	}
	go c.readLoop()
	trapped.block.Store(true)
	t.Cleanup(c.close)
	return c, trapped
}

// ai-generated: a command's context must interrupt a socket stuck inside Write.
func TestSignalingCanceledWrite(t *testing.T) {
	c, trapped := trappedSignaling(t)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := c.command(ctx, "blocked", nil); result <- err }()
	select {
	case <-trapped.entered:
	case <-time.After(time.Second):
		t.Fatal("command did not reach the blocked writer")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("command error=%v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not release Write")
	}
}

// ai-generated: close must skip an occupied writer and release it immediately.
func TestSignalingCloseInterruptsOccupiedWriter(t *testing.T) {
	c, trapped := trappedSignaling(t)
	result := make(chan error, 1)
	go func() { _, err := c.command(t.Context(), "blocked", nil); result <- err }()
	select {
	case <-trapped.entered:
	case <-time.After(time.Second):
		t.Fatal("command did not acquire the writer")
	}
	closed := make(chan struct{})
	go func() { c.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close waited for an occupied writer")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("blocked command succeeded on a closed socket")
		}
	case <-time.After(time.Second):
		t.Fatal("socket close did not release the command")
	}
}

// ai-generated: an otherwise idle socket may blackhole its hangup as well.
func TestSignalingHangupHasDeadline(t *testing.T) {
	c, trapped := trappedSignaling(t)
	closed := make(chan struct{})
	go func() { c.close(); close(closed) }()
	select {
	case <-trapped.entered:
	case <-time.After(time.Second):
		t.Fatal("close did not attempt hangup")
	}
	select {
	case <-closed:
	case <-time.After(signalingCloseWait + time.Second):
		t.Fatal("hangup did not respect its deadline")
	}
}

// ai-generated: a queued command must cancel even when another owner holds the gate.
func TestSignalingQueuedWriterCanCancel(t *testing.T) {
	c, _ := trappedSignaling(t)
	c.writeGate <- struct{}{}
	defer func() { <-c.writeGate }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := c.command(ctx, "queued", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("command error=%v, want deadline", err)
	}
	c.close()
}
