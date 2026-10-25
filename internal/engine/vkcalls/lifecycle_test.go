package vkcalls

// ai-generated: regression tests for bounded rejoin, callback events and cleanup.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// ai-generated: two failed recruits must release the supervisor to retry and recover.
func TestReconnectRecoversAfterFailedRecruits(t *testing.T) {
	f, endpoint := newLifecycleSFU(t, true)
	s := lifecycleSession(t, endpoint)
	s.recruitDelay = 5 * time.Millisecond
	var refreshes atomic.Int32
	s.cfg.Refresh = func(context.Context) (engine.Credentials, error) {
		call := refreshes.Add(1)
		if call == 2 || call == 3 {
			return engine.Credentials{}, errors.New("temporary guest auth failure")
		}
		return engine.Credentials{URL: endpoint}, nil
	}
	ready := make(chan struct{}, 1)
	s.SetReconnectCallback(func() { ready <- struct{}{} })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	watchDone := make(chan struct{})
	go func() { s.WatchConnection(ctx); close(watchDone) }()
	s.Reconnect("test")
	select {
	case <-ready:
	case err := <-f.errors:
		t.Fatalf("fake SFU: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("reconnect did not recover: refreshes=%d joins=%d", refreshes.Load(), f.joined.Load())
	}
	if refreshes.Load() != 4 || f.joined.Load() != 2 || !s.CanSend() {
		t.Fatalf("refreshes=%d joins=%d ready=%v", refreshes.Load(), f.joined.Load(), s.CanSend())
	}
	cancel()
	<-watchDone
}

// ai-generated: the successful callback may queue a failure of the new generation.
func TestReconnectCallbackKeepsNewGenerationFailure(t *testing.T) {
	f, endpoint := newLifecycleSFU(t, false)
	s := lifecycleSession(t, endpoint)
	if err := s.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	old := s.gen
	var callbacks atomic.Int32
	ready := make(chan struct{}, 1)
	s.SetReconnectCallback(func() {
		if s.reconnecting.Load() {
			t.Error("reconnect suppression extends into the successful callback")
		}
		s.reconnectAttempt(old)
		if callbacks.Add(1) == 1 {
			s.reconnectAttempt(s.gen)
			return
		}
		ready <- struct{}{}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { s.WatchConnection(ctx); close(done) }()
	s.Reconnect("test")
	select {
	case <-ready:
	case err := <-f.errors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("new generation's callback failure was lost")
	}
	select {
	case <-ready:
		t.Fatal("old generation queued another rejoin")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-done
	if callbacks.Load() != 2 || f.joined.Load() != 3 {
		t.Fatalf("callbacks=%d joins=%d, want 2/3", callbacks.Load(), f.joined.Load())
	}
}

// ai-generated: every replaced generation must release its socket and media peer.
func TestRepeatedReconnectClosesGenerations(t *testing.T) {
	f, endpoint := newLifecycleSFU(t, false)
	s := lifecycleSession(t, endpoint)
	if err := s.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 12 {
		old := s.gen
		if err := s.reconnect(t.Context()); err != nil {
			t.Fatal(err)
		}
		if !old.isDone() || old.pc.ConnectionState() != webrtc.PeerConnectionStateClosed {
			t.Fatal("replaced generation retained its media peer")
		}
		select {
		case <-old.signal.readDone:
		default:
			t.Fatal("replaced generation retained its signaling reader")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for f.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.active.Load() != 0 {
		t.Fatalf("active signaling connections=%d after Close", f.active.Load())
	}
}

// ai-generated: caller and provider deadlines both cover credential refresh.
func TestReconnectBoundsRefresh(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(map[bool]string{false: "provider_budget", true: "caller_budget"}[caller], func(t *testing.T) {
			s := lifecycleSession(t, "ws://127.0.0.1/")
			s.connectTimeout = 30 * time.Millisecond
			s.cfg.Refresh = func(ctx context.Context) (engine.Credentials, error) {
				<-ctx.Done()
				return engine.Credentials{}, ctx.Err()
			}
			ctx := t.Context()
			if caller {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			if err := s.reconnect(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("reconnect error=%v, want deadline", err)
			}
		})
	}
}

// ai-generated: Close cancels a live credential refresh and waits for its cleanup.
func TestCloseCancelsPendingReconnect(t *testing.T) {
	s := lifecycleSession(t, "ws://127.0.0.1/")
	entered := make(chan struct{})
	s.cfg.Refresh = func(ctx context.Context) (engine.Credentials, error) {
		close(entered)
		<-ctx.Done()
		return engine.Credentials{}, ctx.Err()
	}
	result := make(chan error, 1)
	go func() { result <- s.reconnect(t.Context()) }()
	<-entered
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconnect error=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close left its reconnect running")
	}
}

// ai-generated: the provider budget also bounds a join that never leaves DIRECT.
func TestConnectBoundsDirectTopology(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(map[bool]string{false: "provider_budget", true: "caller_budget"}[caller], func(t *testing.T) {
			f, endpoint := newLifecycleSFU(t, true)
			s := lifecycleSession(t, endpoint)
			s.connectTimeout = 60 * time.Millisecond
			ctx := t.Context()
			if caller {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			err := s.Connect(ctx)
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrSignalingNotServer) {
				t.Fatalf("connect error=%v, want topology deadline", err)
			}
			if f.joined.Load() != 1 {
				t.Fatalf("joined=%d, want one pending join", f.joined.Load())
			}
		})
	}
}

// ai-generated: a second join's caller deadline includes queued writer ownership.
func TestQueuedConnectHonorsContext(t *testing.T) {
	s := lifecycleSession(t, "ws://127.0.0.1/")
	entered := make(chan struct{})
	s.cfg.Refresh = func(ctx context.Context) (engine.Credentials, error) {
		close(entered)
		<-ctx.Done()
		return engine.Credentials{}, ctx.Err()
	}
	first := make(chan error, 1)
	go func() { first <- s.reconnect(t.Context()) }()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- s.Connect(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued connect error=%v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued connect ignored its caller's deadline")
	}
	_ = s.Close()
	<-first
}

// ai-generated: reconnect policy may inspect the session without taking a nested lock.
func TestReconnectPolicyCanReadSession(t *testing.T) {
	_, endpoint := newLifecycleSFU(t, false)
	s := lifecycleSession(t, endpoint)
	if err := s.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.SetShouldReconnect(func() bool {
		_ = s.CanSend()
		_ = s.GetBufferedAmount()
		return true
	})
	done := make(chan struct{})
	go func() { s.reconnectAttempt(s.gen); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconnect policy deadlocked while inspecting the session")
	}
	if !s.Drain() {
		t.Fatal("reconnect policy did not queue the active generation")
	}
}
