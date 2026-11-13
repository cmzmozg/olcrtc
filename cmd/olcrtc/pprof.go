package main

// ai-generated: the whole file (the debug pprof listener, cmd/olcrtc).

import (
	"fmt"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// servePprof serves the net/http/pprof handlers under /debug/pprof on addr
// until the returned close runs. The listener is a debugging aid, not a
// service: bind it to loopback or reach it over SSH.
func servePprof(addr string) (func(), error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof: listen %s: %w", addr, err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	logger.Infof("pprof: serving on %s", addr)
	return func() { _ = srv.Close() }, nil
}
