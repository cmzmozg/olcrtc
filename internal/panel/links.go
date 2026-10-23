package panel

// ai-generated: the whole file (panel).

import (
	"fmt"
	"strings"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/link"
)

const (
	// linkVP8FPS and linkVP8Batch are the app's defaults: a link carries
	// the vp8 options only when they differ.
	linkVP8FPS   = 60
	linkVP8Batch = 64
	// subRefresh is how often a client should fetch its subscription again:
	// a room added in the panel reaches it within this.
	subRefresh = "1h"
	subName    = "olcrtc"
)

// roomLink is the olcrtc:// link of client in room.
func (p *panel) roomLink(room Room, client Client) string {
	l := link.Link{
		Provider: room.Provider, Transport: room.Transport, Room: room.URL, Key: client.Key,
		Label: client.Name + " / " + room.Name, VP8FPS: linkVP8FPS, VP8Batch: linkVP8Batch,
	}
	if room.Transport == transportVP8 {
		l.VP8FPS, l.VP8Batch = p.opts.VP8FPS, p.opts.VP8Batch
	}
	return l.String()
}

// subscription is client's subscription body (docs/sub.md): a link per
// running room.
func (p *panel) subscription(client Client, rooms []Room) string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "#name: %s\n#update: %d\n#refresh: %s\n", subName, time.Now().Unix(), subRefresh)
	for _, room := range rooms {
		if room.Disabled {
			continue
		}
		_, _ = fmt.Fprintf(&b, "\n%s\n##name: %s\n", p.roomLink(room, client), room.Name)
	}
	return b.String()
}

// subURL is the address client's subscription is served at: public_url
// when set, else the address the panel was reached at. It lies outside the
// panel's secret path, so a subscription does not give the path away.
func (p *panel) subURL(base string, client Client) string {
	if p.opts.PublicURL != "" {
		base = strings.TrimRight(p.opts.PublicURL, "/")
	}
	return base + "/sub/" + client.Sub
}
