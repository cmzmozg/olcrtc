// Package panel is the server's web panel: the rooms the server runs, at
// once, and the clients it lets in, a key each, with the olcrtc:// links and
// subscriptions that hand them out. It runs in the server process: a room
// added starts its server at once, a client removed is cut off in every
// room.
//
// ai-generated: the whole package (panel).
package panel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/server"
)

var (
	// ErrPanelConfig is returned for panel settings the panel refuses.
	ErrPanelConfig = errors.New("panel config")
)

// ConfigRoomID is the id of the room the config names, which the panel runs
// and shows but does not edit.
const ConfigRoomID = "config"

// pathChars are what panel.path may hold: it is a URL segment.
const pathChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

const (
	minPasswordLen = 12
	minPathLen     = 8
	transportVP8   = "vp8channel"
	readTimeout    = 30 * time.Second
	stopTimeout    = 5 * time.Second
)

// Options configures the panel.
type Options struct {
	Listen, Path, Password string
	// Cert and Key serve the panel over TLS; required off loopback.
	Cert, Key string
	// PublicURL is the base of subscription links; empty takes the address
	// the panel was reached at.
	PublicURL string
	// Store is the JSON file of rooms and clients.
	Store string
	// ConfigRoom is the room the config names, nil for none. ConfigKeys are
	// the config's keys, always in the ring.
	ConfigRoom *Room
	ConfigKeys []string
	// Provider and Transport are a new room's defaults; Providers and
	// Transports what a room may use, Creators the providers CreateRoom
	// creates rooms on.
	Provider, Transport             string
	Providers, Transports, Creators []string
	// VP8FPS and VP8Batch are what a vp8channel link carries.
	VP8FPS, VP8Batch int
	RunRoom          RunRoomFunc
	// CreateRoom creates a room on provider and returns its room.id.
	CreateRoom func(ctx context.Context, provider string) (string, error)
}

// panel is a running panel.
type panel struct {
	opts    Options
	prefix  string // "/" + Path + "/"
	store   *store
	keys    *server.Keys
	meter   *server.Meter
	rooms   *roomRunner
	auth    *auth
	audit   *auditLog
	monitor *monitor
}

// Run serves the panel and runs its rooms until ctx ends.
func Run(ctx context.Context, opts Options) error {
	p, err := newPanel(ctx, opts)
	if err != nil {
		return err
	}
	defer p.rooms.stop()
	return p.serve(ctx)
}

// newPanel loads the store and runs its rooms until ctx ends.
func newPanel(ctx context.Context, opts Options) (*panel, error) {
	if err := checkOptions(opts); err != nil {
		return nil, err
	}
	st, err := loadStore(opts.Store)
	if err != nil {
		return nil, err
	}
	journal, err := loadAudit(opts.Store + ".audit.json")
	if err != nil {
		return nil, err
	}
	keys, err := server.NewKeys(nil)
	if err != nil {
		return nil, fmt.Errorf("panel keys: %w", err)
	}
	meter := server.NewMeter()
	p := &panel{
		opts: opts, prefix: "/" + opts.Path + "/", store: st, keys: keys, meter: meter,
		rooms: newRoomRunner(opts.RunRoom, keys, meter),
		auth:  newAuth(opts.Password, "/"+opts.Path+"/", opts.Cert != ""),
		audit: journal, monitor: &monitor{started: time.Now()},
	}
	if err := p.apply(ctx); err != nil {
		p.rooms.stop()
		return nil, err
	}
	return p, nil
}

// checkOptions refuses a panel anyone could reach unprotected.
func checkOptions(opts Options) error {
	host, _, err := net.SplitHostPort(opts.Listen)
	if err != nil {
		return fmt.Errorf("%w: panel.listen: %w", ErrPanelConfig, err)
	}
	switch {
	case len(opts.Password) < minPasswordLen:
		return fmt.Errorf("%w: panel.password needs %d characters or more", ErrPanelConfig, minPasswordLen)
	case len(opts.Path) < minPathLen || strings.Trim(opts.Path, pathChars) != "":
		return fmt.Errorf("%w: panel.path needs %d or more letters, digits, - or _", ErrPanelConfig, minPathLen)
	case (opts.Cert == "") != (opts.Key == ""):
		return fmt.Errorf("%w: panel.cert and panel.key go together", ErrPanelConfig)
	case opts.Cert == "" && !isLoopback(host):
		return fmt.Errorf("%w: panel.listen off loopback needs panel.cert and panel.key", ErrPanelConfig)
	case opts.RunRoom == nil:
		return fmt.Errorf("%w: no room runner", ErrPanelConfig)
	}
	return nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// apply puts the store into effect: the ring holds the config's keys and
// every enabled client's, and every enabled room runs.
func (p *panel) apply(ctx context.Context) error {
	data := p.store.snapshot()
	ring := slices.Clone(p.opts.ConfigKeys)
	for _, c := range data.Clients {
		if !c.Disabled {
			ring = append(ring, c.Key)
		}
	}
	if err := p.keys.Set(ring); err != nil {
		return fmt.Errorf("panel keys: %w", err)
	}
	p.rooms.sync(ctx, p.allRooms(data.Rooms))
	return nil
}

// allRooms is the config's room, when there is one, then rooms.
func (p *panel) allRooms(rooms []Room) []Room {
	if p.opts.ConfigRoom == nil {
		return rooms
	}
	return append([]Room{*p.opts.ConfigRoom}, rooms...)
}

// serve answers HTTP until ctx ends.
func (p *panel) serve(ctx context.Context) error {
	srv := &http.Server{
		Addr: p.opts.Listen, Handler: p.handler(),
		ReadHeaderTimeout: readTimeout, ReadTimeout: readTimeout, WriteTimeout: readTimeout,
	}
	errCh := make(chan error, 1)
	go func() {
		if p.opts.Cert != "" {
			errCh <- srv.ListenAndServeTLS(p.opts.Cert, p.opts.Key)
		} else {
			errCh <- srv.ListenAndServe()
		}
	}()
	logger.Infof("panel: serving on %s", p.opts.Listen)
	select {
	case err := <-errCh:
		return fmt.Errorf("panel: %w", err)
	case <-ctx.Done():
		stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		_ = srv.Shutdown(stopCtx) //nolint:contextcheck // ctx is already done; shutdown gets its own bound
		return nil
	}
}
