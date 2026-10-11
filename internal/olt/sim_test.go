package olt

// ai-generated: the whole file (an in-memory lossy link and the connection
// pair the tests run over it).

import (
	"container/heap"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// burstSpan is the longest a loss burst lasts.
const burstSpan = 30 * time.Millisecond

// linkConfig shapes one direction of a simulated path.
type linkConfig struct {
	delay, jitter time.Duration
	// loss drops packets independently; burstProb starts a run of
	// burstMin..burstMax consecutive drops, as a lost video frame does.
	loss               float64
	burstProb          float64
	burstMin, burstMax int
	// reorder holds a packet back by reorderDelay; dup sends it twice.
	reorder      float64
	reorderDelay time.Duration
	dup          float64
	// rate is the bottleneck in bytes per second (0 for none), queue its
	// buffer in bytes; packets that do not fit are dropped.
	rate  float64
	queue int
	// tick holds every packet for the next frame of a carrier that writes
	// one frame per tick, after the bottleneck.
	tick time.Duration
}

// simPkt is a packet on its way, due at at.
type simPkt struct {
	at  time.Time
	seq uint64
	p   []byte
}

// pktHeap orders packets by arrival.
type pktHeap []simPkt

// ai-generated: heap.Interface.
func (h *pktHeap) Len() int { return len(*h) }

// ai-generated: heap.Interface.
func (h *pktHeap) Less(i, j int) bool {
	q := *h
	if q[i].at.Equal(q[j].at) {
		return q[i].seq < q[j].seq
	}
	return q[i].at.Before(q[j].at)
}

// ai-generated: heap.Interface.
func (h *pktHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// ai-generated: heap.Interface.
func (h *pktHeap) Push(x any) { *h = append(*h, x.(simPkt)) }

// ai-generated: heap.Interface.
func (h *pktHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// simLink delivers what send gets to dst after the configured damage.
type simLink struct {
	cfg  linkConfig
	dst  func([]byte)
	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	mu       sync.Mutex
	rng      *rand.Rand
	busy     time.Time
	last     time.Time
	burst    int
	burstEnd time.Time
	down     bool
	q        pktHeap
	seq      uint64
	sent     int
	dropped  int
	epoch    time.Time
}

// newLink starts a link that hands what survives to dst.
//
// ai-generated: whole function.
func newLink(cfg linkConfig, seed uint64, dst func([]byte)) *simLink {
	l := &simLink{
		cfg:   cfg,
		dst:   dst,
		wake:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		rng:   rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), //nolint:gosec // G404: test randomness
		epoch: time.Now(),
	}
	go l.run()
	return l
}

// send is a Config.Send: it drops, queues, delays and reorders p.
//
// ai-generated: whole function.
func (l *simLink) send(p []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sent++
	if l.down || l.lose() {
		l.dropped++
		return nil
	}
	now := time.Now()
	dep := now
	if l.cfg.rate > 0 {
		start := now
		if l.busy.After(now) {
			start = l.busy
		}
		if start.Sub(now).Seconds()*l.cfg.rate+float64(len(p)) > float64(l.cfg.queue) {
			l.dropped++
			return nil
		}
		dep = start.Add(time.Duration(float64(len(p)) / l.cfg.rate * float64(time.Second)))
		l.busy = dep
	}
	if t := l.cfg.tick; t > 0 {
		if r := dep.Sub(l.epoch) % t; r != 0 {
			dep = dep.Add(t - r)
		}
	}
	at := dep.Add(l.cfg.delay)
	if l.cfg.jitter > 0 {
		at = at.Add(time.Duration(l.rng.Int64N(int64(l.cfg.jitter))))
	}
	if l.cfg.reorder > 0 && l.rng.Float64() < l.cfg.reorder {
		at = at.Add(l.cfg.reorderDelay)
	} else {
		if at.Before(l.last) {
			at = l.last
		}
		l.last = at
	}
	l.push(at, p)
	if l.cfg.dup > 0 && l.rng.Float64() < l.cfg.dup {
		l.push(at.Add(time.Millisecond), p)
	}
	return nil
}

// lose decides whether the next packet is dropped. A burst ends after its
// count or burstSpan, whichever comes first: a lost video frame takes the
// packets of one frame interval, not the next N whenever they come.
//
// ai-generated: whole function.
func (l *simLink) lose() bool {
	now := time.Now()
	if l.burst > 0 && now.Before(l.burstEnd) {
		l.burst--
		return true
	}
	l.burst = 0
	if l.cfg.burstProb > 0 && l.rng.Float64() < l.cfg.burstProb {
		l.burst = l.cfg.burstMin + l.rng.IntN(l.cfg.burstMax-l.cfg.burstMin+1) - 1
		l.burstEnd = now.Add(burstSpan)
		return true
	}
	return l.cfg.loss > 0 && l.rng.Float64() < l.cfg.loss
}

// push queues a copy of p for delivery at at.
//
// ai-generated: whole function.
func (l *simLink) push(at time.Time, p []byte) {
	l.seq++
	heap.Push(&l.q, simPkt{at: at, seq: l.seq, p: slices.Clone(p)})
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// setDown makes the link drop everything, or stop doing so.
//
// ai-generated: whole function.
func (l *simLink) setDown(down bool) {
	l.mu.Lock()
	l.down = down
	l.mu.Unlock()
}

// counts reports the packets sent and dropped.
//
// ai-generated: whole function.
func (l *simLink) counts() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sent, l.dropped
}

// run delivers the packets as they fall due.
//
// ai-generated: whole function.
func (l *simLink) run() {
	defer close(l.done)
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		l.mu.Lock()
		wait := time.Hour
		var p []byte
		if len(l.q) > 0 {
			if d := time.Until(l.q[0].at); d <= 0 {
				p = heap.Pop(&l.q).(simPkt).p
			} else {
				wait = d
			}
		}
		l.mu.Unlock()
		if p != nil {
			l.dst(p)
			continue
		}
		t.Reset(wait)
		select {
		case <-l.wake:
		case <-t.C:
		case <-l.stop:
			return
		}
	}
}

// close stops the link.
//
// ai-generated: whole function.
func (l *simLink) close() {
	close(l.stop)
	<-l.done
}

// pair is a client and a server connection over two simulated links.
type pair struct {
	cli, srv *Conn
	up, down *simLink // up carries client to server
}

// newPair connects a client and a server over links shaped by lc; tweak,
// when set, edits each side's config.
//
// ai-generated: whole function.
func newPair(t *testing.T, lc linkConfig, tweak func(cfg *Config)) *pair {
	t.Helper()
	return newPairLinks(t, lc, lc, tweak)
}

// newPairLinks is newPair with the uplink, client to server, shaped by ul
// and the downlink by dl.
//
// ai-generated: whole function.
func newPairLinks(t *testing.T, ul, dl linkConfig, tweak func(cfg *Config)) *pair {
	t.Helper()
	var cliP, srvP atomic.Pointer[Conn]
	up := newLink(ul, 1, func(p []byte) {
		if c := srvP.Load(); c != nil {
			c.Input(p)
		}
	})
	down := newLink(dl, 2, func(p []byte) {
		if c := cliP.Load(); c != nil {
			c.Input(p)
		}
	})
	mk := func(client bool, l *simLink) *Conn {
		cfg := Config{Send: l.send, Client: client, ConnID: testConnID}
		if tweak != nil {
			tweak(&cfg)
		}
		c, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return c
	}
	cli := mk(true, up)
	cliP.Store(cli)
	srv := mk(false, down)
	srvP.Store(srv)
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
		up.close()
		down.close()
	})
	return &pair{cli: cli, srv: srv, up: up, down: down}
}

// percentile returns the p-th percentile of d.
//
// ai-generated: whole function.
func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	return s[min(len(s)-1, int(p*float64(len(s))))]
}
