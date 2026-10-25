package muxconn

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	cryptopkg "github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ai-generated: copying test link that holds each borrowed record until Send returns.
type copyingLink struct {
	stubLink
	hold    <-chan struct{}
	entered chan<- struct{}
}

// ai-generated: opt the copying test link into the send lifetime contract.
func (*copyingLink) CopiesOnSend() bool { return true }

// ai-generated: delay consumption before making the link's own copy.
func (l *copyingLink) Send(data []byte) error {
	if l.entered != nil {
		l.entered <- struct{}{}
	}
	if l.hold != nil {
		<-l.hold
	}
	return l.stubLink.Send(data)
}

// ai-generated: async test link that retains the caller's input after Send returns.
type retainingLink struct {
	stubLink
	records [][]byte
}

// ai-generated: preserve queued ciphertext without copying it.
func (l *retainingLink) Send(data []byte) error {
	l.records = append(l.records, data)
	return nil
}

// ai-generated: explicitly decline scratch reuse for an asynchronous transport.
type nonCopyingLink struct{ *retainingLink }

// ai-generated: an advertised but disabled capability must keep the allocation path.
func (*nonCopyingLink) CopiesOnSend() bool { return false }

// ai-generated: an unverified wrapper must not inherit its inner send capability.
type opaqueCopyingLink struct {
	transport.Transport
	queued [][]byte
}

// ai-generated: defer forwarding while the inner transport copies only its own sends.
func (l *opaqueCopyingLink) Send(data []byte) error {
	l.queued = append(l.queued, data)
	return nil
}

// ai-generated: verify asynchronous and capability-hiding transports keep their records.
func TestSendScratchKeepsAsynchronousRecords(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit_%t", explicit), func(t *testing.T) {
			clientKeys, serverKeys := newTestKeyPair(t)
			link := &retainingLink{stubLink: stubLink{canSend: true}}
			var tr transport.Transport = link
			if explicit {
				tr = &nonCopyingLink{link}
			}
			conn := New(tr, serverKeys)
			for i := range 32 {
				p := bytes.Repeat([]byte{byte(i)}, 12*1024)
				mustWrite(t, conn, p)
				clear(p)
			}
			for i, record := range link.records {
				assertOpenedRecord(t, clientKeys, record, bytes.Repeat([]byte{byte(i)}, 12*1024))
			}
		})
	}
	checkOpaqueSendWrapper(t)
}

// ai-generated: verify an asynchronous wrapper cannot borrow its inner copy guarantee.
func checkOpaqueSendWrapper(t *testing.T) {
	t.Helper()
	clientKeys, serverKeys := newTestKeyPair(t)
	inner := &copyingLink{stubLink: stubLink{canSend: true}}
	wrapped := &opaqueCopyingLink{Transport: inner}
	conn := New(wrapped, serverKeys)
	for i := range 16 {
		mustWrite(t, conn, bytes.Repeat([]byte{byte(i)}, 1024))
	}
	for i, record := range wrapped.queued {
		if err := inner.Send(record); err != nil {
			t.Fatal(err)
		}
		assertOpenedRecord(t, clientKeys, inner.sent[i], bytes.Repeat([]byte{byte(i)}, 1024))
	}
}

// ai-generated: verify concurrent writes retain separate buffers until their sends finish.
func TestSendScratchConcurrentLifetime(t *testing.T) {
	const writers = 8
	clientKeys, serverKeys := newTestKeyPair(t)
	hold, entered := make(chan struct{}), make(chan struct{}, writers)
	link := &copyingLink{stubLink: stubLink{canSend: true}, hold: hold, entered: entered}
	conn := New(link, serverKeys)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		value := byte(i)
		wg.Go(func() {
			_, err := conn.Write(bytes.Repeat([]byte{value}, 12*1024))
			errs <- err
		})
	}
	for range writers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(hold)
			wg.Wait()
			t.Fatal("concurrent sends did not enter")
		}
	}
	close(hold)
	wg.Wait()
	for range writers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	seen := make([]bool, writers)
	if len(link.sent) != writers {
		t.Fatalf("sent %d concurrent records, want %d", len(link.sent), writers)
	}
	for _, record := range link.sent {
		plain, err := clientKeys.Open(record, []byte(dataRecordAAD))
		if err != nil || len(plain) != 12*1024 || int(plain[0]) >= writers {
			t.Fatalf("invalid concurrent record: %v", err)
		}
		i := int(plain[0])
		if seen[i] || !bytes.Equal(plain, bytes.Repeat([]byte{plain[0]}, len(plain))) {
			t.Fatal("concurrent record was overwritten")
		}
		seen[i] = true
	}
}

// ai-generated: verify Send failures and oversized records preserve encryption and later writes.
func TestSendScratchErrorAndOversizedRecord(t *testing.T) {
	clientKeys, serverKeys := newTestKeyPair(t)
	link := &copyingLink{stubLink: stubLink{canSend: true, sendErr: errMuxBoom}}
	conn := New(link, serverKeys)
	if n, err := conn.Write([]byte("failed")); n != 0 || !errors.Is(err, errMuxBoom) {
		t.Fatalf("failed Write = %d, %v", n, err)
	}
	link.sendErr = nil
	large := bytes.Repeat([]byte{0xa5}, pooledFrameCap*3)
	mustWrite(t, conn, large)
	mustWrite(t, conn, []byte("after large"))
	for i, want := range [][]byte{[]byte("failed"), large, []byte("after large")} {
		assertOpenedRecord(t, clientKeys, link.sent[i], want)
	}
}

// ai-generated: assert full authenticated plaintext for records sent with reusable scratch.
func assertOpenedRecord(t *testing.T, keys *cryptopkg.KeySet, record, want []byte) {
	t.Helper()
	got, err := keys.Open(record, []byte(dataRecordAAD))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("record plaintext equal=%t: %v", bytes.Equal(got, want), err)
	}
}
