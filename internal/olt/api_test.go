package olt

// ai-generated: the whole file (half-close, close and reset, deadlines,
// connection close, idle timeout, MaxStreams and flow control bounds).

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// ai-generated: a clean 5 ms path.
func cleanPath() linkConfig { return linkConfig{delay: 5 * time.Millisecond} }

// accept waits up to five seconds for the peer's next stream.
//
// ai-generated: whole function.
func accept(t *testing.T, c *Conn) *Stream {
	t.Helper()
	type res struct {
		s   *Stream
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := c.AcceptStream()
		ch <- res{s, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("accept: %v", r.err)
		}
		return r.s
	case <-time.After(5 * time.Second):
		t.Fatal("accept timed out")
	}
	return nil
}

// open opens a stream and writes msg on it, so the peer accepts it.
//
// ai-generated: whole function.
func open(t *testing.T, c *Conn, msg string) *Stream {
	t.Helper()
	s, err := c.OpenStream()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	return s
}

// TestHalfClose checks that CloseWrite sends FIN while reads go on.
//
// ai-generated: whole function.
func TestHalfClose(t *testing.T) {
	p := newPair(t, cleanPath(), nil)
	cs := open(t, p.cli, "hello")
	if err := cs.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("x")); !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("write after CloseWrite: %v", err)
	}
	ss := accept(t, p.srv)
	got, err := io.ReadAll(ss)
	if err != nil || string(got) != "hello" {
		t.Fatalf("server read %q, %v", got, err)
	}
	if _, err = ss.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	_ = ss.Close()
	got, err = io.ReadAll(cs)
	if err != nil || string(got) != "world" {
		t.Fatalf("client read %q, %v", got, err)
	}
	_ = cs.Close()
	if !waitFor(3*time.Second, func() bool { return p.cli.NumStreams() == 0 && p.srv.NumStreams() == 0 }) {
		t.Fatalf("streams left: client %d, server %d", p.cli.NumStreams(), p.srv.NumStreams())
	}
}

// TestCloseStopsPeer checks that Close on one side stops the other side's
// writes with a reset, and still delivers its FIN.
//
// ai-generated: whole function.
func TestCloseStopsPeer(t *testing.T) {
	p := newPair(t, cleanPath(), nil)
	cs := open(t, p.cli, "a")
	ss := accept(t, p.srv)
	if _, err := io.ReadFull(ss, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	_ = ss.Close()
	if _, err := ss.Read(make([]byte, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after Close: %v", err)
	}
	end := time.Now().Add(5 * time.Second)
	for {
		_, err := cs.Write(make([]byte, 1024))
		if err != nil {
			if !errors.Is(err, ErrStreamReset) {
				t.Fatalf("write error %v, want reset", err)
			}
			break
		}
		if time.Now().After(end) {
			t.Fatal("writes never failed")
		}
		time.Sleep(time.Millisecond)
	}
	if got, err := io.ReadAll(cs); err != nil || len(got) != 0 {
		t.Fatalf("client read %q, %v; want EOF", got, err)
	}
	_ = cs.Close()
	if !waitFor(3*time.Second, func() bool { return p.cli.NumStreams() == 0 && p.srv.NumStreams() == 0 }) {
		t.Fatalf("streams left: client %d, server %d", p.cli.NumStreams(), p.srv.NumStreams())
	}
}

// TestDeadlines checks read and write timeouts and their error.
//
// ai-generated: whole function.
func TestDeadlines(t *testing.T) {
	p := newPair(t, cleanPath(), nil)
	cs := open(t, p.cli, "x")
	ss := accept(t, p.srv)
	_ = cs.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	start := time.Now()
	_, err := cs.Read(make([]byte, 8))
	ne, ok := err.(net.Error) //nolint:errorlint // the direct assertion is what callers do
	if !ok || !ne.Timeout() || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read error %v, want a timeout", err)
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > time.Second {
		t.Fatalf("read timed out after %v", d)
	}
	_ = cs.SetReadDeadline(time.Time{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = ss.Write([]byte("y"))
	}()
	if _, err = io.ReadFull(cs, make([]byte, 1)); err != nil {
		t.Fatalf("read after clearing the deadline: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, rerr := cs.Read(make([]byte, 8))
		done <- rerr
	}()
	time.Sleep(50 * time.Millisecond)
	_ = cs.SetDeadline(time.Now())
	select {
	case err = <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("woken read: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a deadline in the past did not wake the read")
	}
	_ = cs.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	n, err := cs.Write(make([]byte, 16<<20))
	if !errors.Is(err, os.ErrDeadlineExceeded) || n == 0 || n >= 16<<20 {
		t.Fatalf("write wrote %d, %v; want part and a timeout", n, err)
	}
}

// TestConnClose checks Accept, Open and stream calls after Close, and the
// peer's view of it.
//
// ai-generated: whole function.
func TestConnClose(t *testing.T) {
	p := newPair(t, cleanPath(), nil)
	cs := open(t, p.cli, "x")
	accept(t, p.srv)
	errc := make(chan error, 1)
	go func() {
		_, err := p.cli.AcceptStream()
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = p.cli.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) || !errors.Is(err, net.ErrClosed) {
			t.Fatalf("blocked accept: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake AcceptStream")
	}
	if _, err := p.cli.AcceptStream(); !errors.Is(err, ErrClosed) {
		t.Fatalf("accept after close: %v", err)
	}
	if _, err := p.cli.OpenStream(); !errors.Is(err, ErrClosed) {
		t.Fatalf("open after close: %v", err)
	}
	if _, err := cs.Read(make([]byte, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
	if !p.cli.IsClosed() {
		t.Fatal("IsClosed is false")
	}
	if !waitFor(2*time.Second, p.srv.IsClosed) {
		t.Fatal("the peer did not learn of the close")
	}
	if _, err := p.srv.AcceptStream(); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("peer accept: %v", err)
	}
}

// TestIdleTimeout checks that keepalives hold an idle connection up and
// that a dead path closes it.
//
// ai-generated: whole function.
func TestIdleTimeout(t *testing.T) {
	p := newPair(t, cleanPath(), func(cfg *Config) {
		cfg.KeepAlive = 100 * time.Millisecond
		cfg.IdleTimeout = 500 * time.Millisecond
	})
	cs := open(t, p.cli, "x")
	accept(t, p.srv)
	time.Sleep(1200 * time.Millisecond)
	if p.cli.IsClosed() || p.srv.IsClosed() {
		t.Fatal("an idle connection closed despite keepalives")
	}
	p.up.setDown(true)
	p.down.setDown(true)
	if !waitFor(3*time.Second, func() bool { return p.cli.IsClosed() && p.srv.IsClosed() }) {
		t.Fatal("a dead path did not close")
	}
	if _, err := cs.Read(make([]byte, 1)); !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("read on a timed out connection: %v", err)
	}
}

// TestMaxStreams checks the peer's refusal past its MaxStreams and the
// local limit on OpenStream.
//
// ai-generated: whole function.
func TestMaxStreams(t *testing.T) {
	p := newPair(t, cleanPath(), func(cfg *Config) {
		cfg.MaxStreams = 3
		if cfg.Client {
			cfg.MaxStreams = 5
		}
	})
	streams := make([]*Stream, 0, 5)
	for range 5 {
		streams = append(streams, open(t, p.cli, "x"))
	}
	if _, err := p.cli.OpenStream(); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("sixth open: %v", err)
	}
	for range 3 {
		accept(t, p.srv)
	}
	for _, s := range streams[3:] {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := s.Read(make([]byte, 1)); !errors.Is(err, ErrStreamRefused) {
			t.Fatalf("stream %d: %v, want refused", s.ID(), err)
		}
		if _, err := s.Write([]byte("y")); !errors.Is(err, ErrStreamRefused) {
			t.Fatalf("stream %d write: %v, want refused", s.ID(), err)
		}
	}
	if !waitFor(2*time.Second, func() bool { return p.cli.NumStreams() == 3 }) {
		t.Fatalf("client holds %d streams, want 3", p.cli.NumStreams())
	}
	if n := p.srv.NumStreams(); n != 3 {
		t.Fatalf("server holds %d streams, want 3", n)
	}
}

// TestSlowReader checks that a reader that does not read bounds what both
// sides buffer, per stream and per connection.
//
// ai-generated: whole function.
func TestSlowReader(t *testing.T) {
	const total, nstreams = 4 << 20, 3
	p := newPair(t, linkConfig{delay: 5 * time.Millisecond, rate: 50e6 / 8, queue: 1 << 20}, func(cfg *Config) {
		cfg.ConnWindow = 2 << 20
	})
	var written atomic.Int64
	css := make([]*Stream, nstreams)
	for i := range css {
		css[i] = open(t, p.cli, "")
		go func(s *Stream) {
			buf := make([]byte, total)
			for j := range buf {
				buf[j] = byte(j * 7)
			}
			n, _ := s.Write(buf)
			written.Add(int64(n))
			_ = s.CloseWrite()
		}(css[i])
	}
	sss := make([]*Stream, nstreams)
	for i := range sss {
		sss[i] = accept(t, p.srv)
	}
	time.Sleep(time.Second)
	checkBuffered(t, p, css)
	if written.Load() != 0 {
		t.Fatal("a write finished although nothing was read")
	}
	errs := make(chan error, nstreams)
	for _, s := range sss {
		go func() { errs <- readPattern(s, total) }()
	}
	for range sss {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// readPattern reads s to EOF and checks it carried n bytes of the pattern
// byte(i*7).
//
// ai-generated: whole function.
func readPattern(s *Stream, n int) error {
	got, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if len(got) != n {
		return fmt.Errorf("read %d bytes, want %d", len(got), n)
	}
	for j, b := range got {
		if b != byte(j*7) {
			return fmt.Errorf("byte %d is %d", j, b)
		}
	}
	return nil
}

// checkBuffered checks the buffers of a connection whose reader is idle.
//
// ai-generated: whole function.
func checkBuffered(t *testing.T, p *pair, css []*Stream) {
	t.Helper()
	p.srv.mu.Lock()
	held := p.srv.rcvd - p.srv.read
	for _, cs := range css {
		s := p.srv.streams[cs.ID()]
		n := s.rcv.Len()
		for _, ch := range s.ooo {
			n += len(ch.data)
		}
		if u64(n) > p.srv.streamWin {
			t.Errorf("stream %d buffers %d bytes over its window", s.id, n)
		}
	}
	p.srv.mu.Unlock()
	if held > p.srv.connWin {
		t.Errorf("connection holds %d bytes over its window %d", held, p.srv.connWin)
	}
	p.cli.mu.Lock()
	defer p.cli.mu.Unlock()
	for _, s := range css {
		if s.snd.Len() > p.cli.sendBufMax() {
			t.Errorf("sender buffers %d bytes", s.snd.Len())
		}
	}
	t.Logf("server holds %d bytes of a %d window", held, p.srv.connWin)
}
