package vkcalls

import "testing"

// ai-generated: tests for iceRestarted.
func TestICERestartedComparesTheCredentials(t *testing.T) {
	sdp := func(ufrag, eol string) string {
		return "v=0" + eol + "m=video 9 UDP/TLS/RTP/SAVPF 96" + eol + "a=ice-ufrag:" + ufrag + eol + "a=ice-pwd:x" + eol
	}
	cases := []struct {
		name          string
		current, next string
		want          bool
	}{
		{"same credentials", sdp("abcd", "\n"), sdp("abcd", "\r\n"), false},
		{"new credentials", sdp("abcd", "\r\n"), sdp("wxyz", "\r\n"), true},
		{"new credentials, LF", sdp("abcd", "\n"), sdp("wxyz", "\n"), true},
		{"no current credentials", "v=0\n", sdp("wxyz", "\n"), false},
		{"no offered credentials", sdp("abcd", "\n"), "v=0\n", false},
	}
	for _, c := range cases {
		if got := iceRestarted(c.current, c.next); got != c.want {
			t.Errorf("%s: iceRestarted=%v, want %v", c.name, got, c.want)
		}
	}
}
