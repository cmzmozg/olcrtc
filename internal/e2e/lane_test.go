package e2e

import (
	"bytes"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a log sink the tunnel's goroutines may write concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestStreamsOpenedUnderBulkTakeTheSecondLane runs a bulk echo over a
// vp8channel tunnel and opens short connections beside it: they must work,
// and the client must have put them on the second lane, which the server
// builds on that lane's first frame.
//
// ai-generated: the whole test (second lane).
func TestStreamsOpenedUnderBulkTakeTheSecondLane(t *testing.T) {
	logs := &syncBuffer{}
	log.SetOutput(io.MultiWriter(os.Stderr, logs))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	echoAddr := startEchoServer(t)
	rt := startMemoryTunnel(t, transportVP8, true)
	defer rt.stop(t)
	if !strings.Contains(logs.String(), "second lane open") {
		t.Fatal("client did not open the second lane after the welcome")
	}

	bulk := connectViaSOCKS(t, rt.socksAddr, echoAddr)
	defer func() { _ = bulk.Close() }()
	bulkErr := make(chan error, 1)
	go func() { bulkErr <- streamPatternAndVerifyEcho(bulk, 4<<20) }()
	time.Sleep(300 * time.Millisecond)

	for i := range 3 {
		conn := connectViaSOCKS(t, rt.socksAddr, echoAddr)
		msg := []byte("lane ping")
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("short conn %d write: %v", i, err)
		}
		got := make([]byte, len(msg))
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("short conn %d echo %q: %v", i, got, err)
		}
		_ = conn.Close()
	}
	if err := <-bulkErr; err != nil {
		t.Fatalf("bulk echo: %v", err)
	}
	if !strings.Contains(logs.String(), "second lane session created") {
		t.Fatal("no stream reached the server on the second lane")
	}
}
