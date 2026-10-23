package panel

// ai-generated: the whole file (panel).

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
)

var (
	// ErrNotFound is returned for a room or client id the store lacks.
	ErrNotFound = errors.New("not found")
	// ErrInvalid is returned for a name, room or field the store refuses.
	ErrInvalid = errors.New("invalid")
)

const (
	maxNameLen = 64
	maxRoomLen = 512
	// storeMode keeps the store to its owner: it holds every client key.
	storeMode = 0o600
)

// Room is a room the server runs. URL is room.id as its provider takes it.
type Room struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Provider  string    `json:"provider"`
	Transport string    `json:"transport"`
	URL       string    `json:"room"`
	Disabled  bool      `json:"disabled,omitempty"`
	Created   time.Time `json:"created"`
}

// Client is one user's key. Sub is the token of its subscription link.
type Client struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Key      string    `json:"key"`
	Sub      string    `json:"sub"`
	Note     string    `json:"note,omitempty"`
	Disabled bool      `json:"disabled,omitempty"`
	Created  time.Time `json:"created"`
}

type storeData struct {
	Rooms   []Room   `json:"rooms"`
	Clients []Client `json:"clients"`
}

// store is the panel's rooms and clients, saved to path on every change.
type store struct {
	path string
	mu   sync.Mutex
	data storeData
}

// loadStore reads path; a missing file is an empty store.
func loadStore(path string) (*store, error) {
	s := &store{path: path}
	raw, err := os.ReadFile(path) // #nosec G304 -- panel.store is an explicit path in the config
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("read panel store: %w", err)
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return nil, fmt.Errorf("parse panel store %s: %w", path, err)
	}
	return s, nil
}

// snapshot copies the rooms and clients.
func (s *store) snapshot() storeData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return storeData{Rooms: slices.Clone(s.data.Rooms), Clients: slices.Clone(s.data.Clients)}
}

// update applies fn to the data and saves it; nothing changes when fn or
// the save fails.
func (s *store) update(fn func(*storeData) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := storeData{Rooms: slices.Clone(s.data.Rooms), Clients: slices.Clone(s.data.Clients)}
	if err := fn(&next); err != nil {
		return err
	}
	if err := writeFileAtomic(s.path, next); err != nil {
		return err
	}
	s.data = next
	return nil
}

// writeFileAtomic writes v as JSON next to path and renames it over path.
func writeFileAtomic(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encode panel store: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".panel-*.json")
	if err != nil {
		return fmt.Errorf("write panel store: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err = tmp.Chmod(storeMode); err == nil {
		if _, err = tmp.Write(raw); err == nil {
			err = tmp.Sync()
		}
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		return fmt.Errorf("write panel store: %w", err)
	}
	return nil
}

// newID is a random id of n bytes in hex.
func newID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// cleanName trims name and refuses an empty, long or non-printable one, and
// one with a link separator: names end up in links and subscriptions.
func cleanName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxNameLen || strings.ContainsAny(name, "#$%@?<>") ||
		strings.IndexFunc(name, func(r rune) bool { return !unicode.IsPrint(r) }) >= 0 {
		return "", fmt.Errorf("%w: name must be 1-%d printable characters without #$%%@?<>", ErrInvalid, maxNameLen)
	}
	return name, nil
}

// cleanRoom trims a room link and refuses what would break a link around
// it: a '#' ends the room in a link, white space ends the line.
func cleanRoom(room string) (string, error) {
	room = strings.TrimSpace(room)
	if room == "" || len(room) > maxRoomLen || strings.ContainsAny(room, "#$") ||
		strings.IndexFunc(room, unicode.IsSpace) >= 0 {
		return "", fmt.Errorf("%w: room must be 1-%d characters without spaces, # or $", ErrInvalid, maxRoomLen)
	}
	return room, nil
}

// indexOf is the index of the item with id, or -1.
func indexOf[T any](items []T, id string, idOf func(*T) string) int {
	return slices.IndexFunc(items, func(item T) bool { return idOf(&item) == id })
}

func roomID(r *Room) string     { return r.ID }
func clientID(c *Client) string { return c.ID }
