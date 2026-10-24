// SPDX-License-Identifier: Apache-2.0

package vkcalls

import "github.com/pion/interceptor"

// rtcpCacheFactory must be registered last so every RTCP reader receives the
// same non-nil attributes. Pion stats v0.1.45 uses its input attributes rather
// than the cache returned by the reader, otherwise decoding once per SSRC.
// ai-generated: the RTCP cache factory and interceptor types.
type rtcpCacheFactory struct{}

type rtcpCache struct{ interceptor.NoOp }

// NewInterceptor creates a cache boundary for one peer connection.
// ai-generated: this factory method.
func (rtcpCacheFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &rtcpCache{}, nil
}

// BindRTCPReader provides one fresh cache per read, retaining caller attributes.
// ai-generated: this method and its per-read attributes allocation.
func (*rtcpCache) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(func(buf []byte, attrs interceptor.Attributes) (int, interceptor.Attributes, error) {
		if attrs == nil {
			attrs = make(interceptor.Attributes)
		}
		return reader.Read(buf, attrs)
	})
}
