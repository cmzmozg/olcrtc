//go:build linux

package panel

// ai-generated: Linux procfs metrics and filesystem capacity, adapted from Mirage.

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// ai-generated: read host metrics independently so unavailable counters stay absent.
func (m *monitor) host(path string) hostMetrics {
	var h hostMetrics
	if raw, err := os.ReadFile("/proc/stat"); err == nil {
		h.CPUPercent, h.CPUAvailable = m.cpu(strings.Fields(strings.SplitN(string(raw), "\n", 2)[0]))
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		h.MemTotal, h.MemUsed = memoryInfo(string(raw))
	}
	if raw, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) >= 3 {
			h.Load = []float64{metricFloat(fields[0]), metricFloat(fields[1]), metricFloat(fields[2])}
		}
	}
	if raw, err := os.ReadFile("/proc/uptime"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) > 0 {
			h.UptimeSec = metricFloat(fields[0])
		}
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(path, &disk); err == nil && disk.Bsize > 0 {
		h.DiskTotal = disk.Blocks * uint64(disk.Bsize)
		h.DiskFree = disk.Bavail * uint64(disk.Bsize)
	}
	return h
}

// ai-generated: calculate CPU use excluding guest ticks already included in user ticks.
func (m *monitor) cpu(fields []string) (float64, bool) {
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, false
	}
	var total, idle uint64
	for i := 1; i < min(len(fields), 9); i++ {
		n, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return 0, false
		}
		total += n
		if i == 4 || i == 5 {
			idle += n
		}
	}
	prevTotal, prevIdle := m.prevTotal, m.prevIdle
	m.prevTotal, m.prevIdle = total, idle
	if prevTotal == 0 || total <= prevTotal || idle < prevIdle || idle-prevIdle > total-prevTotal {
		return 0, false
	}
	return 100 * (1 - float64(idle-prevIdle)/float64(total-prevTotal)), true
}

// ai-generated: use available memory rather than treating the Linux page cache as used.
func memoryInfo(raw string) (uint64, uint64) {
	var total, available uint64
	for line := range strings.SplitSeq(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		n, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = n * 1024
		case "MemAvailable:":
			available = n * 1024
		}
	}
	return total, total - min(total, available)
}

// ai-generated: parse optional procfs floating point counters.
func metricFloat(s string) float64 {
	n, _ := strconv.ParseFloat(s, 64)
	return n
}
