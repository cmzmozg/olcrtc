package olt

// ai-generated: the whole file (integrity, isolation and throughput over the
// simulated link).

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ai-generated: a 20 Mbit/s path with 40 ms each way.
func path20() linkConfig {
	return linkConfig{delay: 40 * time.Millisecond, rate: 20e6 / 8, queue: 300_000}
}

// echo serves every accepted stream: it copies the stream back and then
// closes it.
//
// ai-generated: whole function.
func echo(c *Conn) {
	for {
		s, err := c.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			_, _ = io.Copy(s, s)
			_ = s.CloseWrite()
			_ = s.Close()
		}()
	}
}

// exchange opens a stream on c, writes data, closes the write side and
// checks that the echo is exact and ends with EOF.
//
// ai-generated: whole function.
func exchange(c *Conn, data []byte) error {
	s, err := c.OpenStream()
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	werr := make(chan error, 1)
	go func() {
		_, werrv := s.Write(data)
		if werrv == nil {
			werrv = s.CloseWrite()
		}
		werr <- werrv
	}()
	got, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if err := <-werr; err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return errors.New("echo differs")
	}
	return nil
}

// waitFor polls cond for up to d.
//
// ai-generated: whole function.
func waitFor(d time.Duration, cond func() bool) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// TestIntegrity runs 50 concurrent streams, opened from both sides, over a
// path that loses, reorders and duplicates packets.
//
// ai-generated: whole function.
func TestIntegrity(t *testing.T) {
	p := newPair(t, linkConfig{
		delay: 15 * time.Millisecond, jitter: 5 * time.Millisecond,
		loss: 0.05, reorder: 0.03, reorderDelay: 20 * time.Millisecond, dup: 0.03,
		rate: 50e6 / 8, queue: 1 << 20,
	}, nil)
	go echo(p.cli)
	go echo(p.srv)
	rng := rand.New(rand.NewPCG(7, 7)) //nolint:gosec // G404: test data
	fill := rand.NewChaCha8([32]byte{7})
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	var total atomic.Int64
	start := time.Now()
	for i := range 50 {
		data := make([]byte, 1+rng.IntN(120<<10))
		_, _ = fill.Read(data)
		total.Add(int64(len(data)))
		side := p.cli
		if i%2 == 1 {
			side = p.srv
		}
		wg.Go(func() { errs <- exchange(side, data) })
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("transfers did not finish")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%d bytes each way in %v; client %+v", total.Load(), time.Since(start), p.cli.Stats())
	if !waitFor(5*time.Second, func() bool { return p.cli.NumStreams() == 0 && p.srv.NumStreams() == 0 }) {
		t.Fatalf("streams left: client %d, server %d", p.cli.NumStreams(), p.srv.NumStreams())
	}
}

// bulkServer answers a stream that starts with 'B' with endless data and a
// stream that starts with 'P' with an echo.
//
// ai-generated: whole function.
func bulkServer(c *Conn) {
	for {
		s, err := c.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			var kind [1]byte
			if _, err := io.ReadFull(s, kind[:]); err != nil {
				return
			}
			if kind[0] == 'P' {
				_, _ = io.Copy(s, s)
				return
			}
			buf := make([]byte, 32<<10)
			for {
				if _, err := s.Write(buf); err != nil {
					return
				}
			}
		}()
	}
}

// bulk opens a bulk stream and counts what arrives on it into got.
//
// ai-generated: whole function.
func bulk(t *testing.T, c *Conn, got *atomic.Int64) {
	t.Helper()
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte{'B'}); err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := s.Read(buf)
			got.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
}

// pings measures n request/response round trips of 32 bytes, gap apart.
//
// ai-generated: whole function.
func pings(t *testing.T, c *Conn, n int, gap time.Duration) []time.Duration {
	t.Helper()
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.Write([]byte{'P'}); err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 32)
	back := make([]byte, 32)
	rtts := make([]time.Duration, 0, n)
	for range n {
		start := time.Now()
		if _, err := s.Write(msg); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(s, back); err != nil {
			t.Fatal(err)
		}
		rtts = append(rtts, time.Since(start))
		time.Sleep(gap)
	}
	return rtts
}

// TestIsolation runs a ping stream next to a bulk download on a 20 Mbit/s
// path that loses runs of 30-40 packets, 1% in all: the pings must stay
// near the path RTT.
//
// ai-generated: whole function.
func TestIsolation(t *testing.T) {
	lc := path20()
	lc.burstProb, lc.burstMin, lc.burstMax = 0.01/35, 30, 40
	p := newPair(t, lc, nil)
	go bulkServer(p.srv)
	var got atomic.Int64
	bulk(t, p.cli, &got)
	time.Sleep(1500 * time.Millisecond)
	start, before := time.Now(), got.Load()
	rtts := pings(t, p.cli, 50, 40*time.Millisecond)
	rate := float64(got.Load()-before) / time.Since(start).Seconds()
	_, dropped := p.down.counts()
	rtt := 2 * lc.delay
	t.Logf("ping p50 %v p90 %v max %v; bulk %.2f Mbit/s; downlink drops %d; server %+v",
		percentile(rtts, 0.5), percentile(rtts, 0.9), percentile(rtts, 1), rate*8/1e6, dropped, p.srv.Stats())
	if p90 := percentile(rtts, 0.9); p90 > 3*rtt {
		t.Fatalf("ping p90 %v over %v", p90, 3*rtt)
	}
	if rate < 0.5*lc.rate {
		t.Fatalf("bulk at %.2f Mbit/s", rate*8/1e6)
	}
}

// TestThroughput fills a clean 20 Mbit/s path: the bulk transfer must
// reach 75% of it, and the queue it builds must not delay a ping by more
// than 100 ms.
//
// ai-generated: whole function.
func TestThroughput(t *testing.T) {
	lc := path20()
	p := newPair(t, lc, nil)
	go bulkServer(p.srv)
	var got atomic.Int64
	bulk(t, p.cli, &got)
	time.Sleep(2 * time.Second)
	start, before := time.Now(), got.Load()
	rtts := pings(t, p.cli, 30, 100*time.Millisecond)
	rate := float64(got.Load()-before) / time.Since(start).Seconds()
	rtt := 2 * lc.delay
	t.Logf("bulk %.2f Mbit/s (%.0f%% of the path); ping p50 %v p90 %v; server %+v",
		rate*8/1e6, 100*rate/lc.rate, percentile(rtts, 0.5), percentile(rtts, 0.9), p.srv.Stats())
	if rate < 0.75*lc.rate {
		t.Fatalf("bulk at %.2f Mbit/s, under 75%% of 20", rate*8/1e6)
	}
	if p50 := percentile(rtts, 0.5); p50 > rtt+100*time.Millisecond {
		t.Fatalf("ping p50 %v over %v", p50, rtt+100*time.Millisecond)
	}
}
