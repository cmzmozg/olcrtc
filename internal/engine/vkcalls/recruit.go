package vkcalls

import (
	"context"
	"fmt"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// A fresh VK room keeps its first participants in DIRECT: two alone never
// switch, and a third simultaneous participant is what flips the SFU to
// SERVER - which then sticks for the room's lifetime (the spike's
// topology-flip runs, and the 1.0.444 fresh-link measurement: a server and
// its client sat in DIRECT to their connect timeouts with no
// topology-changed between them). While the engine waits in DIRECT it
// therefore recruits signaling-only guest joins of its own, and drops them
// the moment the switch arrives.
const (
	// maxRecruits is how many guests the engine adds besides itself: one
	// already makes three with the waiting peer, two cover the case the
	// peer has not joined yet.
	maxRecruits = 2
	// recruitFirstDelay lets the waiting peer join first, so its own
	// connection already counts toward the three.
	recruitFirstDelay = 2 * time.Second
	// recruitNextDelay spaces the joins: the flip lands on a join, and a
	// burst buys nothing.
	recruitNextDelay = 3 * time.Second
)

// recruitGuests holds at most two guests until SERVER or the join budget ends.
// ai-generated: report exhausted failures and clean guests before returning.
func (s *Session) recruitGuests(ctx context.Context) error {
	if s.cfg.Refresh == nil {
		<-ctx.Done()
		return nil
	}
	guests := make([]*signalingClient, 0, maxRecruits)
	defer func() {
		for _, guest := range guests {
			guest.closeContext(ctx)
		}
	}()
	delay := recruitFirstDelay
	var lastErr error
	for range maxRecruits {
		if s.recruitDelay > 0 {
			delay = s.recruitDelay
		}
		if !recruitPause(ctx, delay) {
			return nil
		}
		delay = recruitNextDelay
		guest, err := s.recruitOne(ctx)
		if err != nil {
			lastErr = err
			logger.Debugf("vkcalls: guest recruit: %v", err)
			continue
		}
		guests = append(guests, guest)
		logger.Debugf("vkcalls: recruited a signaling-only guest to flip the fresh room to SERVER")
	}
	if len(guests) == 0 && lastErr != nil {
		return fmt.Errorf("vkcalls: guest recruits exhausted: %w", lastErr)
	}
	<-ctx.Done()
	return nil
}

// recruitOne issues a fresh guest's credentials, joins its signaling socket
// and holds it open - answering keepalives and nothing else: no peer
// connection, no commands, the participant only counts.
// ai-generated: canceled guest joins skip graceful writes during cleanup.
func (s *Session) recruitOne(ctx context.Context) (*signalingClient, error) {
	creds, err := s.cfg.Refresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("guest credentials: %w", err)
	}
	guest, err := dialSignaling(ctx, creds.URL, s.cfg.Resolver)
	if err != nil {
		return nil, err
	}
	if _, err = guest.awaitJoin(ctx); err != nil {
		guest.closeContext(ctx)
		return nil, fmt.Errorf("guest join: %w", err)
	}
	go guest.drainNotifications()
	return guest, nil
}

// recruitPause waits d, or ends early when the attempt ends.
// ai-generated: use the recruit context for both success and failure cleanup.
func recruitPause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
