package panel

// ai-generated: authenticated process and host monitoring without external services.

import (
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/server"
)

type hostMetrics struct {
	CPUPercent   float64   `json:"cpu_percent"`
	CPUAvailable bool      `json:"cpu_available"`
	Load         []float64 `json:"load"`
	MemTotal     uint64    `json:"mem_total"`
	MemUsed      uint64    `json:"mem_used"`
	DiskTotal    uint64    `json:"disk_total"`
	DiskFree     uint64    `json:"disk_free"`
	UptimeSec    float64   `json:"uptime_sec"`
}

type monitor struct {
	mu        sync.Mutex
	started   time.Time
	prevTotal uint64
	prevIdle  uint64
}

type metricsView struct {
	At             time.Time             `json:"at"`
	Host           hostMetrics           `json:"host"`
	UptimeSec      float64               `json:"uptime_sec"`
	HeapBytes      uint64                `json:"heap_bytes"`
	Goroutines     int                   `json:"goroutines"`
	CPUs           int                   `json:"cpus"`
	Rooms          int                   `json:"rooms"`
	RoomsUp        int                   `json:"rooms_up"`
	Clients        int                   `json:"clients"`
	ClientsEnabled int                   `json:"clients_enabled"`
	Traffic        server.StatsDirection `json:"traffic"`
}

// ai-generated: sample host CPU deltas and process memory, then count panel objects.
func (p *panel) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	p.monitor.mu.Lock()
	host := p.monitor.host(filepath.Dir(p.opts.Store))
	p.monitor.mu.Unlock()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	view := metricsView{At: time.Now().UTC(), Host: host,
		UptimeSec: time.Since(p.monitor.started).Seconds(), HeapBytes: mem.HeapAlloc,
		Goroutines: runtime.NumGoroutine(), CPUs: runtime.NumCPU(), Traffic: p.meter.Snapshot().Total}
	data := p.store.snapshot()
	rooms := p.allRooms(data.Rooms)
	view.Rooms = len(rooms)
	for _, room := range rooms {
		if p.rooms.state(room.ID).State == string(server.LinkUp) {
			view.RoomsUp++
		}
	}
	view.Clients = len(data.Clients)
	for _, client := range data.Clients {
		if !client.Disabled {
			view.ClientsEnabled++
		}
	}
	writeJSON(w, view)
}
