package muxconn

import (
	"io"
	"testing"
)

// ai-generated: advertise selectable copy semantics for the synchronous benchmark link.
type copyingBenchmarkLink struct {
	synchronousTransport
	copying bool
}

// ai-generated: select the baseline allocation or bounded scratch path in one benchmark.
func (l *copyingBenchmarkLink) CopiesOnSend() bool { return l.copying }

// ai-generated: benchmark full server record Write, decrypt and Read with both send contracts.
func BenchmarkConnSendScratch12KiB(b *testing.B) {
	for _, copying := range []bool{false, true} {
		name := "allocate"
		if copying {
			name = "copying"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkSendScratch(b, copying)
		})
	}
}

// ai-generated: compare the same server-to-client record pipeline with and without scratch.
func benchmarkSendScratch(b *testing.B, copying bool) {
	b.Helper()
	clientKeys, serverKeys := benchmarkKeyPair(b)
	client := New(&synchronousTransport{}, clientKeys)
	link := &copyingBenchmarkLink{copying: copying}
	link.onSend = client.Push
	server := New(link, serverKeys)
	payload := make([]byte, muxBenchmarkPayloadSize)
	readBuf := make([]byte, muxBenchmarkPayloadSize)
	b.ReportAllocs()
	b.SetBytes(muxBenchmarkPayloadSize)
	b.ResetTimer()
	for range b.N {
		if _, err := server.Write(payload); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(client, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}
