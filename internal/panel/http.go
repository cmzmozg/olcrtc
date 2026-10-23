package panel

// ai-generated: the whole file (panel).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/panel/web"
)

const (
	maxBody       = 16 << 10
	createTimeout = 30 * time.Second
)

// handler is the panel's HTTP handler: the subscriptions at /sub/, the
// panel itself under its secret path, and a 404 for everything else.
func (p *panel) handler() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("POST /api/login", p.handleLogin)
	api.HandleFunc("POST /api/logout", p.handleLogout)
	api.HandleFunc("GET /api/state", p.auth.require(p.handleState))
	api.HandleFunc("POST /api/rooms", p.auth.require(p.handleAddRoom))
	api.HandleFunc("PATCH /api/rooms/{id}", p.auth.require(p.handleEditRoom))
	api.HandleFunc("DELETE /api/rooms/{id}", p.auth.require(p.handleDeleteRoom))
	api.HandleFunc("POST /api/clients", p.auth.require(p.handleAddClient))
	api.HandleFunc("PATCH /api/clients/{id}", p.auth.require(p.handleEditClient))
	api.HandleFunc("DELETE /api/clients/{id}", p.auth.require(p.handleDeleteClient))
	api.HandleFunc("GET /api/clients/{id}/links", p.auth.require(p.handleLinks))
	api.Handle("GET /", http.FileServerFS(web.Assets))

	root := http.NewServeMux()
	root.HandleFunc("GET /sub/{token}", p.handleSub)
	root.Handle(p.prefix, http.StripPrefix(strings.TrimSuffix(p.prefix, "/"), api))
	root.Handle("GET "+strings.TrimSuffix(p.prefix, "/"), http.RedirectHandler(p.prefix, http.StatusMovedPermanently))
	return securityHeaders(root, p.opts.Cert != "")
}

// securityHeaders sets what every answer carries. The panel loads nothing
// from elsewhere and inlines nothing, so its policy can be strict.
func securityHeaders(next http.Handler, tls bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; "+
			"img-src 'self' data:; connect-src 'self'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		if tls {
			h.Set("Strict-Transport-Security", "max-age=15552000")
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Debugf("panel: answer: %v", err)
	}
}

// errorBody is what a refused request gets.
type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(errorBody{Error: msg}); err != nil {
		logger.Debugf("panel: answer: %v", err)
	}
}

// writeStoreError answers a failed store change.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// readJSON decodes r's body into v, refusing a large or malformed one.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body")
		return false
	}
	return true
}

// change updates the store with fn, puts the result into effect and
// answers with the new state.
func (p *panel) change(w http.ResponseWriter, r *http.Request, fn func(*storeData) error) {
	if err := p.store.update(fn); err != nil {
		writeStoreError(w, err)
		return
	}
	if err := p.apply(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.handleState(w, r)
}

func (p *panel) handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Password string `json:"password"`
	}
	if r.Header.Get(csrfHeader) == "" || !readJSON(w, r, &body) {
		writeError(w, http.StatusBadRequest, "bad request")
		return
	}
	switch p.auth.login(w, remoteHost(r), body.Password) {
	case loginLimited:
		writeError(w, http.StatusTooManyRequests, "too many attempts, try later")
	case loginWrong:
		writeError(w, http.StatusUnauthorized, "wrong password")
	case loginOK:
		writeJSON(w, okBody{OK: true})
	}
}

// okBody is what a login or a logout gets.
type okBody struct {
	OK bool `json:"ok"`
}

func (p *panel) handleLogout(w http.ResponseWriter, r *http.Request) {
	p.auth.logout(w, r)
	writeJSON(w, okBody{OK: true})
}

// roomView is a room as the panel shows it.
type roomView struct {
	Room
	roomState
	Fixed bool `json:"fixed,omitempty"`
}

// clientView is a client as the panel shows it, without its key.
type clientView struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Note     string    `json:"note,omitempty"`
	Disabled bool      `json:"disabled,omitempty"`
	Created  time.Time `json:"created"`
	Up       uint64    `json:"up"`
	Down     uint64    `json:"down"`
}

func (p *panel) handleState(w http.ResponseWriter, _ *http.Request) {
	data := p.store.snapshot()
	traffic := p.meter.Snapshot().Keys
	rooms := make([]roomView, 0, len(data.Rooms)+1)
	for _, room := range p.allRooms(data.Rooms) {
		rooms = append(rooms, roomView{Room: room, roomState: p.rooms.state(room.ID), Fixed: room.ID == ConfigRoomID})
	}
	clients := make([]clientView, 0, len(data.Clients))
	for _, c := range data.Clients {
		id, _ := crypto.KeyIDFromHex(c.Key)
		t := traffic[id]
		clients = append(clients, clientView{
			ID: c.ID, Name: c.Name, Note: c.Note, Disabled: c.Disabled, Created: c.Created, Up: t.Up, Down: t.Down,
		})
	}
	writeJSON(w, stateView{
		Rooms: rooms, Clients: clients,
		Providers: p.opts.Providers, Transports: p.opts.Transports, Creators: p.opts.Creators,
		Provider: p.opts.Provider, Transport: p.opts.Transport,
	})
}

// stateView is what the page shows: the rooms, the clients and what a new
// room may be.
type stateView struct {
	Rooms      []roomView   `json:"rooms"`
	Clients    []clientView `json:"clients"`
	Providers  []string     `json:"providers"`
	Transports []string     `json:"transports"`
	Creators   []string     `json:"creators"`
	Provider   string       `json:"provider"`
	Transport  string       `json:"transport"`
}

// roomBody is what a room is added or edited with; Create asks the provider
// for a new room in place of URL.
type roomBody struct {
	Name      *string `json:"name"`
	Provider  string  `json:"provider"`
	Transport string  `json:"transport"`
	URL       *string `json:"room"`
	Create    bool    `json:"create"`
	Disabled  *bool   `json:"disabled"`
}

func (p *panel) handleAddRoom(w http.ResponseWriter, r *http.Request) {
	var body roomBody
	if !readJSON(w, r, &body) {
		return
	}
	room, err := p.newRoom(r.Context(), body)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	p.change(w, r, func(d *storeData) error {
		d.Rooms = append(d.Rooms, room)
		return nil
	})
}

// newRoom builds the room body asks for, creating it on its provider when
// body.Create says so.
func (p *panel) newRoom(ctx context.Context, body roomBody) (Room, error) {
	room := Room{ID: newID(8), Provider: body.Provider, Transport: body.Transport, Created: time.Now().UTC()}
	if !slices.Contains(p.opts.Providers, room.Provider) || !slices.Contains(p.opts.Transports, room.Transport) {
		return Room{}, fmt.Errorf("%w: unknown provider or transport", ErrInvalid)
	}
	var err error
	if room.Name, err = cleanName(deref(body.Name)); err != nil {
		return Room{}, err
	}
	url := deref(body.URL)
	if body.Create {
		if p.opts.CreateRoom == nil || !slices.Contains(p.opts.Creators, room.Provider) {
			return Room{}, fmt.Errorf("%w: %s does not create rooms", ErrInvalid, room.Provider)
		}
		createCtx, cancel := context.WithTimeout(ctx, createTimeout)
		defer cancel()
		if url, err = p.opts.CreateRoom(createCtx, room.Provider); err != nil {
			return Room{}, err
		}
	}
	room.URL, err = cleanRoom(url)
	return room, err
}

func (p *panel) handleEditRoom(w http.ResponseWriter, r *http.Request) {
	var body roomBody
	if !readJSON(w, r, &body) {
		return
	}
	id := r.PathValue("id")
	p.change(w, r, func(d *storeData) error {
		i := indexOf(d.Rooms, id, roomID)
		if i < 0 {
			return ErrNotFound
		}
		room := &d.Rooms[i]
		var err error
		if body.Name != nil {
			if room.Name, err = cleanName(*body.Name); err != nil {
				return err
			}
		}
		if body.URL != nil {
			if room.URL, err = cleanRoom(*body.URL); err != nil {
				return err
			}
		}
		if body.Disabled != nil {
			room.Disabled = *body.Disabled
		}
		return nil
	})
}

func (p *panel) handleDeleteRoom(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p.change(w, r, func(d *storeData) error {
		i := indexOf(d.Rooms, id, roomID)
		if i < 0 {
			return ErrNotFound
		}
		d.Rooms = slices.Delete(d.Rooms, i, i+1)
		return nil
	})
}

// clientBody is what a client is added or edited with; RotateSub gives it a
// new subscription token, so the old link stops working.
type clientBody struct {
	Name      *string `json:"name"`
	Note      *string `json:"note"`
	Disabled  *bool   `json:"disabled"`
	RotateSub bool    `json:"rotate_sub"`
}

func (p *panel) handleAddClient(w http.ResponseWriter, r *http.Request) {
	var body clientBody
	if !readJSON(w, r, &body) {
		return
	}
	name, err := cleanName(deref(body.Name))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	client := Client{
		ID: newID(8), Name: name, Key: newID(32), Sub: newID(32),
		Note: strings.TrimSpace(deref(body.Note)), Created: time.Now().UTC(),
	}
	p.change(w, r, func(d *storeData) error {
		d.Clients = append(d.Clients, client)
		return nil
	})
}

func (p *panel) handleEditClient(w http.ResponseWriter, r *http.Request) {
	var body clientBody
	if !readJSON(w, r, &body) {
		return
	}
	id := r.PathValue("id")
	p.change(w, r, func(d *storeData) error {
		i := indexOf(d.Clients, id, clientID)
		if i < 0 {
			return ErrNotFound
		}
		c := &d.Clients[i]
		if body.Name != nil {
			name, err := cleanName(*body.Name)
			if err != nil {
				return err
			}
			c.Name = name
		}
		if body.Note != nil {
			c.Note = strings.TrimSpace(*body.Note)
		}
		if body.Disabled != nil {
			c.Disabled = *body.Disabled
		}
		if body.RotateSub {
			c.Sub = newID(32)
		}
		return nil
	})
}

func (p *panel) handleDeleteClient(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p.change(w, r, func(d *storeData) error {
		i := indexOf(d.Clients, id, clientID)
		if i < 0 {
			return ErrNotFound
		}
		d.Clients = slices.Delete(d.Clients, i, i+1)
		return nil
	})
}

// handleLinks answers with a client's subscription address and its link in
// every room.
func (p *panel) handleLinks(w http.ResponseWriter, r *http.Request) {
	data := p.store.snapshot()
	i := indexOf(data.Clients, r.PathValue("id"), clientID)
	if i < 0 {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	client := data.Clients[i]
	links := make([]roomLink, 0, len(data.Rooms)+1)
	for _, room := range p.allRooms(data.Rooms) {
		links = append(links, roomLink{Room: room.Name, Link: p.roomLink(room, client), Disabled: room.Disabled})
	}
	writeJSON(w, linksView{Sub: p.subURL(requestBase(r, p.opts.Cert != ""), client), Links: links})
}

// linksView is a client's subscription address and its link in every room.
type linksView struct {
	Sub   string     `json:"sub"`
	Links []roomLink `json:"links"`
}

type roomLink struct {
	Room     string `json:"room"`
	Link     string `json:"link"`
	Disabled bool   `json:"disabled,omitempty"`
}

// handleSub serves a subscription to whoever holds its token. An unknown
// token or a disabled client gets the 404 any other path gets.
func (p *panel) handleSub(w http.ResponseWriter, r *http.Request) {
	data := p.store.snapshot()
	token := r.PathValue("token")
	i := slices.IndexFunc(data.Clients, func(c Client) bool { return c.Sub == token })
	if i < 0 || data.Clients[i].Disabled {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(p.subscription(data.Clients[i], p.allRooms(data.Rooms))))
}

// requestBase is the scheme and host r reached the panel at.
func requestBase(r *http.Request, tls bool) string {
	if tls {
		return "https://" + r.Host
	}
	return "http://" + r.Host
}

func deref[T any](v *T) T {
	var zero T
	if v == nil {
		return zero
	}
	return *v
}
