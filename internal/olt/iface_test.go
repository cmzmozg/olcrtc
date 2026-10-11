package olt_test

// ai-generated: the whole file (the interfaces the tunnel needs of olt).

import (
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/olt"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// TestInterfaces pins Stream to tunnelcore.Stream and Conn to the shape of
// tunnelcore.StreamSession, with *olt.Stream in place of the interface.
//
// ai-generated: whole function.
func TestInterfaces(_ *testing.T) {
	var _ tunnelcore.Stream = (*olt.Stream)(nil)
	var _ interface {
		OpenStream() (*olt.Stream, error)
		AcceptStream() (*olt.Stream, error)
		IsClosed() bool
		NumStreams() int
		Close() error
	} = (*olt.Conn)(nil)
}
