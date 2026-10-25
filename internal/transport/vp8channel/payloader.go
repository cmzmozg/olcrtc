package vp8channel

// ai-generated: bounded server VP8 fragmentation matching Pion's descriptor and picture numbering.

const maxVP8ScratchBytes = defaultMaxPayloadSize + maxFramePackets*4

// vp8Scratch owns one track's fragment storage. Its caller serializes the whole
// WriteSample, including every binding's synchronous write or pacer copy.
type vp8Scratch struct {
	data      []byte
	fragments [][]byte
}

// payload cuts one sample into the same payloads as Pion's VP8Payloader.
// ai-generated: reuse bounded server fragment buffers until the next sample.
func (s *vp8Scratch) payload(mtu uint16, payload []byte, id uint32, picture bool) [][]byte {
	header := vp8DescriptorSize(id, picture)
	stride := int(mtu) - header
	if stride <= 0 || len(payload) == 0 {
		return nil
	}
	count := (len(payload)-1)/stride + 1
	if count > (int(^uint(0)>>1)-len(payload))/header {
		return nil
	}
	storage, fragments := s.buffers(len(payload)+count*header, count)
	for i, offset, source := 0, 0, 0; source < len(payload); i++ {
		size := min(stride, len(payload)-source)
		out := storage[offset : offset+header+size]
		vp8Descriptor(out[:header], id)
		if source == 0 {
			out[0] |= 0x10
		}
		copy(out[header:], payload[source:source+size])
		fragments[i] = out
		offset, source = offset+header+size, source+size
	}
	return fragments
}

// buffers keeps only storage fitting a normal sample; oversized input is temporary.
// ai-generated: bound retained bytes and clear fragment pointers left by a shorter sample.
func (s *vp8Scratch) buffers(size, count int) ([]byte, [][]byte) {
	if size > maxVP8ScratchBytes || count > maxFramePackets {
		return make([]byte, size), make([][]byte, count)
	}
	if cap(s.data) < size {
		s.data = make([]byte, size)
	}
	if count < len(s.fragments) {
		clear(s.fragments[count:])
	}
	if cap(s.fragments) < count {
		s.fragments = make([][]byte, count)
	}
	s.fragments = s.fragments[:count]
	return s.data[:size], s.fragments
}

// vp8DescriptorSize is Pion's descriptor length for the next picture.
// ai-generated: preserve the omitted picture zero and short/long picture boundaries.
func vp8DescriptorSize(id uint32, picture bool) int {
	if !picture || id == 0 {
		return 1
	}
	if id < 128 {
		return 3
	}
	return 4
}

// vp8Descriptor writes a descriptor whose size already encodes the picture mode.
// ai-generated: preserve Pion's descriptor bytes without retaining its fragment allocations.
func vp8Descriptor(out []byte, id uint32) {
	out[0] = 0
	if len(out) == 1 {
		return
	}
	out[0], out[1], out[2] = 0x80, 0x80, byte(id&0x7f)
	if len(out) == 4 {
		out[2], out[3] = 0x80|byte((id>>8)&0x7f), byte(id&0xff)
	}
}
