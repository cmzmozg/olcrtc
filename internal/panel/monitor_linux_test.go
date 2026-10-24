//go:build linux

package panel

// ai-generated: test CPU counter resets and Linux memory accounting.

import (
	"strings"
	"testing"
)

// ai-generated: guard CPU deltas against first samples, resets and guest double counting.
func TestMonitorCPUDeltas(t *testing.T) {
	var m monitor
	for _, tc := range []struct {
		line string
		want float64
		ok   bool
	}{
		{"cpu 100 0 0 100 0 0 0 0 100 0", 0, false},
		{"cpu 150 0 0 150 0 0 0 0 150 0", 50, true},
		{"cpu 1 0 0 1 0 0 0 0", 0, false},
		{"cpu 2 0 0 2 0 0 0 0", 50, true},
		{"cpu 2 0 0 2 0 0 0 0", 0, false},
		{"cpu malformed 0 0 0", 0, false},
		{"invalid", 0, false},
	} {
		got, ok := m.cpu(strings.Fields(tc.line))
		if got != tc.want || ok != tc.ok {
			t.Fatalf("%q: %.2f %v, want %.2f %v", tc.line, got, ok, tc.want, tc.ok)
		}
	}
}

// ai-generated: subtract available memory and prevent unsigned underflow.
func TestMonitorMemoryAvailable(t *testing.T) {
	total, used := memoryInfo("MemTotal: 1000 kB\nMemFree: 100 kB\nMemAvailable: 600 kB\n")
	if total != 1000*1024 || used != 400*1024 {
		t.Fatalf("memory = %d / %d", used, total)
	}
	_, used = memoryInfo("MemTotal: 10 kB\nMemAvailable: 20 kB")
	if used != 0 {
		t.Fatal("available memory underflow")
	}
}
