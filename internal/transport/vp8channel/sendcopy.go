package vp8channel

// CopiesOnSend reports the server's synchronous KCP framing copy guarantee.
// All data, peer, control and lane paths copy under kcpRuntime.writeMu.
// ai-generated: opt server VP8 sends into ciphertext scratch reuse.
func (p *streamTransport) CopiesOnSend() bool { return p.serverMode }
