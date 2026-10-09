package client

import (
	"encoding/json"
	"errors"
	"io"
	"testing"
)

var errEarlyMissing = errors.New("early data missing before the ack")

// TestConnectStreamEarlyData checks that against an exit announcing early
// data the client's first bytes leave before any ack, and that the ack is
// taken off the stream: the data behind an OK reaches the caller, a refusal
// ends the stream.
//
// ai-generated: the whole test (optimistic connect).
func TestConnectStreamEarlyData(t *testing.T) {
	for _, ack := range []byte{0x00, 0x01} {
		serverSess, clientSess := newSmuxPair(t)
		served := make(chan error, 1)
		go func() {
			stream := acceptStream(serverSess)
			if stream == nil {
				served <- io.ErrClosedPipe
				return
			}
			dec := json.NewDecoder(stream)
			var req map[string]any
			if err := dec.Decode(&req); err != nil {
				served <- err
				return
			}
			early := make([]byte, 4)
			if _, err := io.ReadFull(io.MultiReader(dec.Buffered(), stream), early); err != nil || string(early) != "ping" {
				served <- errEarlyMissing
				return
			}
			_, err := stream.Write(append([]byte{ack}, "pong"...))
			served <- err
		}()

		stream, err := clientSess.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream() error = %v", err)
		}
		c := &Client{deviceID: "client-1"}
		c.earlyData.Store(true)
		tunnel, err := c.connectStream(stream, testConnectHost, 443)
		if err != nil {
			t.Fatalf("connectStream() error = %v", err)
		}
		if _, writeErr := tunnel.Write([]byte("ping")); writeErr != nil {
			t.Fatalf("Write() error = %v", writeErr)
		}
		reply := make([]byte, 4)
		_, err = io.ReadFull(tunnel, reply)
		if serveErr := <-served; serveErr != nil {
			t.Fatalf("ack=0x%02x: server side error = %v", ack, serveErr)
		}
		switch {
		case ack == 0x00 && (err != nil || string(reply) != "pong"):
			t.Fatalf("ack=0x00: read = %q, %v; want pong", reply, err)
		case ack != 0x00 && !errors.Is(err, ErrRemoteNotReady):
			t.Fatalf("ack=0x%02x: read error = %v, want %v", ack, err, ErrRemoteNotReady)
		}
		_ = tunnel.Close()
	}
}
