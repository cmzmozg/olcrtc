package panel

// ai-generated: the whole file (panel).

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/server"
)

const (
	// retryFloor and retryCeiling bound the wait before a room whose server
	// ended is run again, doubling while it keeps ending.
	retryFloor   = 5 * time.Second
	retryCeiling = time.Minute
	// stateStopped and stateError are what a room reports beside the
	// server's link states: not run, or ended with an error and waiting.
	stateStopped = "stopped"
	stateError   = "error"
)

// RunRoomFunc runs the server of room on keys, counting into meter, until
// ctx ends, and tells onLink every carrier-session transition.
type RunRoomFunc func(
	ctx context.Context, room Room, keys *server.Keys, meter *server.Meter, onLink func(server.LinkState),
) error

// roomRun is one room's server, running or waiting to run again.
type roomRun struct {
	room   Room
	cancel context.CancelFunc
	done   chan struct{}
	state  atomic.Pointer[roomState]
}

// roomState is what a room's server is doing, and since when.
type roomState struct {
	State string    `json:"state"`
	Error string    `json:"error,omitempty"`
	Since time.Time `json:"since"`
}

func (r *roomRun) set(state, errText string) {
	r.state.Store(&roomState{State: state, Error: errText, Since: time.Now()})
}

// roomRunner keeps one server running per enabled room, until stop.
type roomRunner struct {
	run   RunRoomFunc
	keys  *server.Keys
	meter *server.Meter
	mu    sync.Mutex
	runs  map[string]*roomRun
	// stopping is the done channel of each room's server that was told to
	// stop and may not be gone yet: the room's next server waits for it.
	stopping map[string]<-chan struct{}
}

func newRoomRunner(run RunRoomFunc, keys *server.Keys, meter *server.Meter) *roomRunner {
	return &roomRunner{
		run: run, keys: keys, meter: meter,
		runs: make(map[string]*roomRun), stopping: make(map[string]<-chan struct{}),
	}
}

// sync runs the enabled rooms and stops the rest. A room whose link,
// provider or transport changed is run anew once the old server is gone. A
// room outlives ctx, the request that changed it: stop ends it.
func (rr *roomRunner) sync(ctx context.Context, rooms []Room) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	want := make(map[string]Room, len(rooms))
	for _, room := range rooms {
		if !room.Disabled {
			want[room.ID] = room
		}
	}
	for id, run := range rr.runs {
		if room, ok := want[id]; !ok || !sameServer(room, run.room) {
			run.cancel()
			rr.stopping[id] = run.done
			delete(rr.runs, id)
		}
	}
	for id, room := range want {
		if run, ok := rr.runs[id]; ok {
			run.room.Name = room.Name
			continue
		}
		rr.runs[id] = rr.start(ctx, room, rr.stopping[id])
		delete(rr.stopping, id)
	}
	for id, done := range rr.stopping {
		select {
		case <-done:
			delete(rr.stopping, id)
		default:
		}
	}
}

// sameServer reports whether a and b run the same server.
func sameServer(a, b Room) bool {
	return a.URL == b.URL && a.Provider == b.Provider && a.Transport == b.Transport
}

// start runs room's server, once after has closed (the server it replaces
// is gone; nil for none), until its context ends, again after every end.
func (rr *roomRunner) start(ctx context.Context, room Room, after <-chan struct{}) *roomRun {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	run := &roomRun{room: room, cancel: cancel, done: make(chan struct{})}
	run.set(string(server.LinkConnecting), "")
	go func() {
		defer close(run.done)
		if after != nil {
			<-after
		}
		wait := retryFloor
		for ctx.Err() == nil {
			started := time.Now()
			err := rr.run(ctx, room, rr.keys, rr.meter, func(state server.LinkState) { run.set(string(state), "") })
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				logger.Warnf("panel: room %s ended: %v", room.Name, err)
				run.set(stateError, err.Error())
			}
			if time.Since(started) > retryCeiling {
				wait = retryFloor
			}
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
			wait = min(2*wait, retryCeiling)
		}
	}()
	return run
}

// state is what room id is doing: stopped when it is not run.
func (rr *roomRunner) state(id string) roomState {
	rr.mu.Lock()
	run := rr.runs[id]
	rr.mu.Unlock()
	if run == nil {
		return roomState{State: stateStopped}
	}
	return *run.state.Load()
}

// stop ends every room's server and waits for them, the ones already told
// to stop too.
func (rr *roomRunner) stop() {
	rr.mu.Lock()
	dones := make([]<-chan struct{}, 0, len(rr.runs)+len(rr.stopping))
	for id, run := range rr.runs {
		run.cancel()
		dones = append(dones, run.done)
		delete(rr.runs, id)
	}
	for id, done := range rr.stopping {
		dones = append(dones, done)
		delete(rr.stopping, id)
	}
	rr.mu.Unlock()
	for _, done := range dones {
		<-done
	}
}
