package vp8channel

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pion/rtp/codecs"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ai-generated: compare server fragmentation with pinned Pion across every picture ID and its wrap.
func TestServerVP8PayloadMatchesPion(t *testing.T) {
	for _, picture := range []bool{false, true} {
		t.Run(fmt.Sprintf("picture=%t", picture), func(t *testing.T) {
			server := newVP8Payloader()
			server.EnablePictureID, server.scratch = picture, &vp8Scratch{}
			reference := &codecs.VP8Payloader{EnablePictureID: picture}
			data := bytes.Repeat([]byte{0x35, 0x76, 0xaa}, defaultMaxPayloadSize/3)
			sizes := []int{1, 1024, 1184, 1188, 12 * 1024, len(data)}
			for id := range 32770 {
				payload := data[:sizes[id%len(sizes)]]
				assertVP8Fragments(t, server.Payload(1188, payload), reference.Payload(1188, payload))
				if id%127 == 0 {
					assertVP8Fragments(t, server.Payload(1, payload), reference.Payload(1, payload))
					assertVP8Fragments(t, server.Payload(1188, nil), reference.Payload(1188, nil))
				}
			}
		})
	}
}

// ai-generated: compare every fragment immediately, before the next sample reuses server storage.
func assertVP8Fragments(t *testing.T, actual, expected [][]byte) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("fragments=%d, want %d", len(actual), len(expected))
	}
	for i := range actual {
		if !bytes.Equal(actual[i], expected[i]) {
			t.Fatalf("fragment %d differs from Pion", i)
		}
	}
}

// ai-generated: preserve empty/invalid input semantics, caller ownership, and bounded retained scratch.
func TestServerVP8ScratchBounds(t *testing.T) {
	v := newVP8Payloader()
	v.scratch = &vp8Scratch{}
	for _, mtu := range []uint16{0, 1} {
		if len(v.Payload(mtu, []byte{1})) != 0 || v.next.Load() != 0 {
			t.Fatal("invalid MTU consumed a picture")
		}
	}
	if len(v.Payload(1188, nil)) != 0 || v.next.Load() != 0 {
		t.Fatal("empty sample consumed a picture")
	}
	data := bytes.Repeat([]byte{0x35}, defaultMaxPayloadSize)
	fragments := v.Payload(1188, data)
	data[0] = 0xaa
	if fragments[0][1] != 0x35 {
		t.Fatal("fragment retained caller-owned sample data")
	}
	beforeBytes, beforeFragments := cap(v.scratch.data), cap(v.scratch.fragments)
	if beforeBytes > maxVP8ScratchBytes || beforeFragments > maxFramePackets {
		t.Fatal("normal sample exceeded scratch bounds")
	}
	v.Payload(1188, make([]byte, 4*defaultMaxPayloadSize))
	if cap(v.scratch.data) != beforeBytes || cap(v.scratch.fragments) != beforeFragments {
		t.Fatal("oversized sample grew retained scratch")
	}
	v.Payload(1188, []byte{1})
	for _, fragment := range v.scratch.fragments[1:cap(v.scratch.fragments)] {
		if fragment != nil {
			t.Fatal("short sample retained unused fragment pointers")
		}
	}
}

// ai-generated: keep the client's existing Pion allocator and ownership behavior.
func TestClientVP8PayloadKeepsPionStorage(t *testing.T) {
	v := newVP8Payloader()
	first := v.Payload(1188, []byte{1, 2, 3})
	v.Payload(1188, []byte{4, 5, 6})
	if v.scratch != nil || !bytes.Equal(first[0], []byte{0x10, 1, 2, 3}) {
		t.Fatal("client payload storage changed")
	}
}

// ai-generated: enable scratch only in the server constructor and keep the client on Pion.
func TestVP8ScratchServerOnly(t *testing.T) {
	for _, server := range []bool{false, true} {
		name := fmt.Sprintf("vp8-scratch-server-%t", server)
		enginebuiltin.Register(name, func(context.Context, enginebuiltin.Config) (engine.Session, error) {
			return &fakeEngineSession{stream: &fakeVideoStream{}}, nil
		})
		cfg := transport.Config{Provider: name}
		if server {
			cfg.OnPeerData = func(string, []byte) {}
		}
		link, err := New(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = link.Close() })
		tr, ok := link.(*streamTransport)
		if !ok || (tr.vp8.scratch != nil) != server || !tr.vp8.EnablePictureID {
			t.Fatalf("server=%t: wrong payloader allocation mode", server)
		}
	}
}

// ai-generated: measure server scratch against the unchanged Pion client path on the same runtime.
func BenchmarkVP8PayloadStorage(b *testing.B) {
	for _, server := range []bool{false, true} {
		for _, size := range []int{1024, 12 * 1024, defaultMaxPayloadSize} {
			b.Run(fmt.Sprintf("server=%t/%dB", server, size), func(b *testing.B) {
				v := newVP8Payloader()
				if server {
					v.scratch = &vp8Scratch{}
				}
				data := make([]byte, size)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for range b.N {
					if len(v.Payload(1188, data)) == 0 {
						b.Fatal("empty payload")
					}
				}
			})
		}
	}
}
