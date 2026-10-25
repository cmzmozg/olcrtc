package vkcalls

// ai-generated: bounded connection attempts and generation installation.

import (
	"context"
	"fmt"
	"time"

	"github.com/pion/webrtc/v4"
)

// connectBudget matches the transport's initial join budget, including rejoin.
const connectBudget = time.Minute

// connectAttempt owns cancellation while credentials and signaling are pending.
type connectAttempt struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Connect joins the conversation within the caller's and provider's budgets.
// ai-generated: serialize attempts and close any generation it replaces.
func (s *Session) Connect(ctx context.Context) error {
	ctx, finish, err := s.beginAttempt(ctx)
	if err != nil {
		return err
	}
	defer finish()
	s.detachGeneration(ctx)
	return s.connect(ctx)
}

// ai-generated: apply one budget to cleanup, credential refresh and connection.
func (s *Session) rejoin(ctx context.Context) error {
	ctx, finish, err := s.beginAttempt(ctx)
	if err != nil {
		return err
	}
	defer finish()
	s.reconnecting.Store(true)
	defer s.reconnecting.Store(false)
	s.detachGeneration(ctx)
	if s.cfg.Refresh != nil {
		creds, refreshErr := s.cfg.Refresh(ctx)
		if refreshErr != nil {
			return fmt.Errorf("refresh credentials: %w", refreshErr)
		}
		s.endpoint, s.peerID, s.iceJSON = creds.URL, creds.Extra["peer_id_hint"], creds.Extra["ice_servers"]
	}
	return s.connect(ctx)
}

// ai-generated: publish pending cancellation to Close without storing a context.
func (s *Session) beginAttempt(ctx context.Context) (context.Context, func(), error) {
	budget := connectBudget
	if s.connectTimeout > 0 {
		budget = s.connectTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	select {
	case s.connectGate <- struct{}{}:
	case <-s.closeCh:
		cancel()
		return nil, nil, ErrSessionClosed
	case <-ctx.Done():
		cancel()
		return nil, nil, fmt.Errorf("vkcalls: queued join: %w", ctx.Err())
	}
	attempt := &connectAttempt{cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		cancel()
		<-s.connectGate
		return nil, nil, ErrSessionClosed
	}
	s.attempt = attempt
	s.mu.Unlock()
	return ctx, func() {
		cancel()
		s.mu.Lock()
		s.attempt = nil
		s.mu.Unlock()
		close(attempt.done)
		<-s.connectGate
	}, nil
}

// ai-generated: remove the old generation before its teardown emits callbacks.
func (s *Session) detachGeneration(ctx context.Context) {
	s.mu.Lock()
	gen := s.gen
	s.gen = nil
	s.mu.Unlock()
	if gen != nil {
		gen.teardownContext(ctx)
	}
}

// ai-generated: refuse a canceled/dead join and start owned workers before Close.
func (s *Session) installGeneration(ctx context.Context, gen *generation, connection notification) error {
	s.mu.Lock()
	err := s.generationError(ctx, gen)
	if err != nil {
		s.mu.Unlock()
		gen.teardownContext(ctx)
		return err
	}
	gen.workers.Add(2)
	s.gen = gen
	s.reconnecting.Store(false)
	go func() {
		defer gen.workers.Done()
		gen.perfStatLoop()
	}()
	go func() { //nolint:contextcheck // the generation outlives its connect budget
		defer gen.workers.Done()
		s.run(gen, connection)
	}()
	s.mu.Unlock()
	return nil
}

// ai-generated: validate the attempt after ICE, before publishing its generation.
func (s *Session) generationError(ctx context.Context, gen *generation) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("vkcalls: install generation: %w", err)
	}
	if s.closed.Load() || gen.isDone() || gen.pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
		return ErrSessionClosed
	}
	select {
	case <-gen.signal.Closed():
		return ErrSignalingClosed
	default:
		return nil
	}
}
