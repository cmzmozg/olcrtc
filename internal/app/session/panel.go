package session

// ai-generated: the whole file (panel).

import (
	"context"
	"fmt"
	"slices"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/panel"
	"github.com/openlibrecommunity/olcrtc/internal/server"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// RunPanel runs a server whose rooms and client keys come from its web
// panel: a server per room, all at once, on one ring of keys. cfg's own room
// and keys, when it names any, run and pair beside the panel's. opts carries
// the panel's own settings; RunPanel fills in the rest.
func RunPanel(ctx context.Context, cfg Config, opts panel.Options) error {
	if err := engine.ValidateDTLSProfile(engine.DTLSProfile(cfg.DTLSProfile)); err != nil {
		return fmt.Errorf("dtls: %w", err)
	}
	RegisterDefaults()
	cfg.PanelManaged = true
	cfg, err := prepareRunConfig(cfg)
	if err != nil {
		return err
	}
	cfg.Resolver = tunnelcore.Resolver(cfg.Resolver, cfg.DNSServer)
	opts.ConfigKeys = cfg.KeysHex
	if len(opts.ConfigKeys) == 0 && cfg.KeyHex != "" {
		opts.ConfigKeys = []string{cfg.KeyHex}
	}
	if cfg.RoomID != "" {
		opts.ConfigRoom = &panel.Room{
			ID: panel.ConfigRoomID, Name: panel.ConfigRoomID, Provider: cfg.Provider, Transport: cfg.Transport, URL: cfg.RoomID,
		}
	}
	opts.Provider, opts.Transport = cfg.Provider, cfg.Transport
	opts.Providers = slices.DeleteFunc(enginebuiltin.Available(), func(p string) bool { return p == providerNone })
	opts.Transports, opts.Creators = transport.Available(), auth.RoomCreators()
	opts.VP8FPS, opts.VP8Batch = cfg.VP8.FPS, cfg.VP8.BatchSize
	opts.RunRoom = func(ctx context.Context, room panel.Room, keys *server.Keys, meter *server.Meter,
		onLink func(server.LinkState),
	) error {
		return runPanelRoom(ctx, cfg, room, keys, meter, onLink)
	}
	opts.CreateRoom = func(ctx context.Context, provider string) (string, error) {
		gen := cfg
		gen.Provider, gen.Amount = provider, 1
		var room string
		err := Gen(ctx, gen, func(id string) { room = id })
		return room, err
	}
	if err := panel.Run(ctx, opts); err != nil {
		return fmt.Errorf("panel: %w", err)
	}
	return nil
}

// runPanelRoom runs room's server with base's settings until ctx ends. Only
// the config's own room serves stats.listen: the meter is the panel's, one
// for every room.
func runPanelRoom(
	ctx context.Context, base Config, room panel.Room, keys *server.Keys, meter *server.Meter,
	onLink func(server.LinkState),
) error {
	cfg := base
	cfg.Provider, cfg.Transport, cfg.RoomID = room.Provider, room.Transport, room.URL
	cfg.KeySource, cfg.Meter, cfg.OnLinkState = keys, meter, onLink
	if room.ID != panel.ConfigRoomID {
		cfg.StatsListen = ""
	}
	cfg, err := prepareRunConfig(cfg)
	if err != nil {
		return err
	}
	liveness, err := livenessConfig(cfg)
	if err != nil {
		return err
	}
	maxDuration, err := maxSessionDuration(cfg)
	if err != nil {
		return err
	}
	traffic, err := trafficConfig(cfg)
	if err != nil {
		return err
	}
	run := func(ctx context.Context) error {
		return runOnce(ctx, cfg, cfg.RoomID, liveness, traffic)
	}
	if maxDuration > 0 {
		return runWithSessionRotation(ctx, maxDuration, defaultSessionRestartDelay, run)
	}
	return run(ctx)
}
