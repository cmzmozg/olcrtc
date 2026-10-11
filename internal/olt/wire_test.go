package olt

// ai-generated: the whole file (varints, ACK encoding, the range set, the
// packet decoder against malformed input, and a transfer benchmark).

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

const testConnID = 0x0123456789abcdef

// TestVarint round-trips the varint boundaries.
//
// ai-generated: whole function.
func TestVarint(t *testing.T) {
	for _, v := range []uint64{0, 63, 64, 16383, 16384, 1<<30 - 1, 1 << 30, maxVarint} {
		b := appendVarint(nil, v)
		if len(b) != varintLen(v) {
			t.Fatalf("%d encodes to %d bytes, want %d", v, len(b), varintLen(v))
		}
		r := reader{b: b}
		if got := r.varint(); r.bad || got != v || len(r.b) != 0 {
			t.Fatalf("%d decodes to %d (bad %v)", v, got, r.bad)
		}
	}
}

// TestRangeSet checks add, contains, gap, advance and trim.
//
// ai-generated: whole function.
func TestRangeSet(t *testing.T) {
	var r rangeSet
	for _, s := range []span{{10, 20}, {30, 40}, {20, 25}, {50, 60}, {39, 45}} {
		r.add(s.lo, s.hi)
	}
	if want := []span{{10, 25}, {30, 45}, {50, 60}}; !slices.Equal(r.spans, want) {
		t.Fatalf("spans %v, want %v", r.spans, want)
	}
	if !r.contains(10) || r.contains(25) || !r.contains(44) || r.contains(5) {
		t.Fatal("contains is wrong")
	}
	if lo, hi := r.gap(12, 100); lo != 25 || hi != 30 {
		t.Fatalf("gap(12, 100) = %d, %d", lo, hi)
	}
	if lo, hi := r.gap(31, 44); lo < hi {
		t.Fatalf("gap(31, 44) = %d, %d, want none", lo, hi)
	}
	r.floor = 10
	r.advance()
	if r.floor != 25 || len(r.spans) != 2 {
		t.Fatalf("advance: floor %d, spans %v", r.floor, r.spans)
	}
	r.trim(1)
	if r.floor != 45 || !r.contains(26) || len(r.spans) != 1 {
		t.Fatalf("trim: floor %d, spans %v", r.floor, r.spans)
	}
}

// TestAckRoundTrip encodes the received ranges and decodes them back.
//
// ai-generated: whole function.
func TestAckRoundTrip(t *testing.T) {
	c := &Conn{ackRanges: maxAckRanges}
	for pn := range uint64(1000) {
		if pn%7 != 3 && pn%50 != 49 {
			c.rcvdPN.add(pn, pn+1)
		}
	}
	p := appendHeader(nil, testConnID, 5)
	p = c.encodeAck(p, 0)
	var pk inPacket
	if err := pk.decode(p, testConnID); err != nil {
		t.Fatal(err)
	}
	if len(pk.frames) != 1 || pk.frames[0].typ != frAck || pk.elicit {
		t.Fatalf("frames %+v", pk.frames)
	}
	got := pk.ranges[pk.frames[0].r0:pk.frames[0].r1]
	sp := c.rcvdPN.spans
	want := slices.Clone(sp[len(sp)-1-maxAckRanges:])
	slices.Reverse(want)
	if !slices.Equal(got, want) {
		t.Fatalf("ranges\n%v\nwant\n%v", got, want)
	}
}

// TestDecodeMalformed feeds the decoder broken packets.
//
// ai-generated: whole function.
func TestDecodeMalformed(t *testing.T) {
	hdr := appendHeader(nil, testConnID, 1)
	with := func(b ...byte) []byte { return append(slices.Clone(hdr), b...) }
	cases := map[string][]byte{
		"empty":           nil,
		"short":           {version, 1, 2},
		"version":         append([]byte{2}, hdr[1:]...),
		"no frames":       hdr,
		"bad type":        with(0x3f),
		"short varint":    with(frMaxData, 0x80, 1),
		"stream past end": with(frStream, 1, 0, 10, 'a'),
		"stream id":       append(with(frStream), appendVarint(nil, 1<<33)...),
		"ack first":       with(frAck, 5, 0, 0, 6),
		"ack ranges":      append(with(frAck, 5, 0), appendVarint(nil, maxAckDecode+1)...),
		"ack gap":         with(frAck, 5, 0, 1, 1, 3, 0),
		"ack range":       with(frAck, 10, 0, 1, 1, 0, 9),
		"reset short":     with(frReset, 1, 0),
	}
	var pk inPacket
	for name, p := range cases {
		if err := pk.decode(p, testConnID); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want malformed", name, err)
		}
	}
	if err := pk.decode(hdr, testConnID+1); !errors.Is(err, ErrWrongConn) {
		t.Errorf("foreign conn: %v", err)
	}
}

// FuzzDecode checks that no input makes the decoder panic, and that what it
// accepts stays inside the packet.
//
// ai-generated: whole function.
func FuzzDecode(f *testing.F) {
	hdr := appendHeader(nil, testConnID, 77)
	f.Add(append(slices.Clone(hdr), frPing))
	f.Add(appendStreamFrame(slices.Clone(hdr), 3, 1000, []byte("data"), true))
	f.Add(appendIDFrame(slices.Clone(hdr), frReset, 4, 1, 99))
	f.Add(append(slices.Clone(hdr), frAck, 9, 1, 1, 2, 1, 1))
	f.Add(binary.BigEndian.AppendUint64([]byte{version}, testConnID))
	f.Fuzz(func(t *testing.T, p []byte) {
		var pk inPacket
		if pk.decode(p, testConnID) != nil {
			return
		}
		for _, fr := range pk.frames {
			if len(fr.data) > len(p) || fr.r0 > fr.r1 || fr.r1 > len(pk.ranges) {
				t.Fatalf("frame out of bounds: %+v", fr)
			}
		}
		for _, r := range pk.ranges {
			if r.lo >= r.hi {
				t.Fatalf("empty range %v", r)
			}
		}
	})
}

// BenchmarkStream moves data over a stream between two connections joined
// directly.
//
// ai-generated: whole function.
func BenchmarkStream(b *testing.B) {
	var cliP, srvP atomic.Pointer[Conn]
	link := func(dst *atomic.Pointer[Conn]) func([]byte) error {
		return func(p []byte) error {
			if c := dst.Load(); c != nil {
				c.Input(p)
			}
			return nil
		}
	}
	cli, err := New(Config{Client: true, Send: link(&srvP), Tick: time.Millisecond, BurstBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	srv, err := New(Config{Send: link(&cliP), Tick: time.Millisecond, BurstBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	cliP.Store(cli)
	srvP.Store(srv)
	b.Cleanup(func() { _ = cli.Close(); _ = srv.Close() })
	s, err := cli.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		if ss, err := srv.AcceptStream(); err == nil {
			_, _ = io.Copy(io.Discard, ss)
		}
	}()
	buf := bytes.Repeat([]byte{1}, 1<<20)
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for range b.N {
		if _, err := s.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}
