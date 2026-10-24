//go:build !linux

package panel

// ai-generated: keep process monitoring available without Linux procfs.

// ai-generated: report unavailable host metrics on other operating systems.
func (*monitor) host(_ string) hostMetrics {
	return hostMetrics{}
}
