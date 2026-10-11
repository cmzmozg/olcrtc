package olt

import (
	"slices"
	"sort"
	"time"
)

// span is the half-open range [lo, hi).
type span struct{ lo, hi uint64 }

// rangeSet is a set of uint64 values kept as sorted, disjoint, non-adjacent
// spans. Every value below floor counts as a member.
type rangeSet struct {
	spans []span
	floor uint64
}

// contains reports whether v is in the set.
//
// ai-generated: whole function.
func (r *rangeSet) contains(v uint64) bool {
	if v < r.floor {
		return true
	}
	i := sort.Search(len(r.spans), func(k int) bool { return r.spans[k].hi > v })
	return i < len(r.spans) && r.spans[i].lo <= v
}

// add puts [lo, hi) into the set.
//
// ai-generated: whole function.
func (r *rangeSet) add(lo, hi uint64) {
	lo = max(lo, r.floor)
	if lo >= hi {
		return
	}
	s := r.spans
	i := sort.Search(len(s), func(k int) bool { return s[k].hi >= lo })
	j := i
	for j < len(s) && s[j].lo <= hi {
		lo, hi = min(lo, s[j].lo), max(hi, s[j].hi)
		j++
	}
	if i == j {
		r.spans = slices.Insert(s, i, span{lo, hi})
		return
	}
	s[i] = span{lo, hi}
	r.spans = slices.Delete(s, i+1, j)
}

// trim keeps the newest limit spans; what lies below the rest joins floor.
//
// ai-generated: whole function.
func (r *rangeSet) trim(limit int) {
	if len(r.spans) <= limit {
		return
	}
	k := len(r.spans) - limit
	r.floor = r.spans[k-1].hi
	r.spans = slices.Delete(r.spans, 0, k)
}

// advance folds the spans that reach floor into it.
//
// ai-generated: whole function.
func (r *rangeSet) advance() {
	k := 0
	for k < len(r.spans) && r.spans[k].lo <= r.floor {
		r.floor = max(r.floor, r.spans[k].hi)
		k++
	}
	if k > 0 {
		r.spans = slices.Delete(r.spans, 0, k)
	}
}

// gap returns the first part of [lo, hi) outside the set; lo >= hi in the
// result means there is none.
//
// ai-generated: whole function.
func (r *rangeSet) gap(lo, hi uint64) (uint64, uint64) {
	lo = max(lo, r.floor)
	s := r.spans
	i := sort.Search(len(s), func(k int) bool { return s[k].hi > lo })
	if i < len(s) && s[i].lo <= lo {
		lo = s[i].hi
		i++
	}
	if i < len(s) {
		hi = min(hi, s[i].lo)
	}
	return lo, hi
}

// cutFront removes everything below v from the first span.
//
// ai-generated: whole function.
func (r *rangeSet) cutFront(v uint64) {
	if len(r.spans) == 0 {
		return
	}
	r.spans[0].lo = max(r.spans[0].lo, v)
	if r.spans[0].lo >= r.spans[0].hi {
		r.spans = slices.Delete(r.spans, 0, 1)
	}
}

// byteQueue is a FIFO of bytes that reuses its buffer.
type byteQueue struct {
	b []byte
	h int
}

// Len is the number of queued bytes.
//
// ai-generated: whole function.
func (q *byteQueue) Len() int { return len(q.b) - q.h }

// Bytes returns the queued bytes; they stay valid until the next Push.
//
// ai-generated: whole function.
func (q *byteQueue) Bytes() []byte { return q.b[q.h:] }

// Push appends p, moving the queued bytes to the front when that saves a
// reallocation.
//
// ai-generated: whole function.
func (q *byteQueue) Push(p []byte) {
	if q.h > 0 && len(q.b)+len(p) > cap(q.b) {
		n := copy(q.b, q.b[q.h:])
		q.b, q.h = q.b[:n], 0
	}
	q.b = append(q.b, p...)
}

// Drop removes the first n bytes.
//
// ai-generated: whole function.
func (q *byteQueue) Drop(n uint64) {
	if n >= u64(q.Len()) {
		q.b, q.h = q.b[:0], 0
		return
	}
	q.h += int(n) //nolint:gosec // G115: n < Len
}

// chunk is out-of-order stream data.
type chunk struct {
	off  uint64
	data []byte
}

// end is the offset after the chunk.
//
// ai-generated: whole function.
func (c chunk) end() uint64 { return c.off + u64(len(c.data)) }

// Frame kinds a sent packet remembers, to act on its loss or acknowledgement.
const (
	fStream = iota
	fMaxData
	fMaxStreamData
	fStop
	fReset
	fRefuse
	fPing
)

// sentFrame is what a sent packet carried, enough to send it again.
type sentFrame struct {
	kind     byte
	fin      bool
	id       uint32
	off, end uint64
}

// sentPacket is one sent packet, kept until it is acknowledged or lost.
type sentPacket struct {
	pn       uint64
	sentAt   time.Duration
	size     int
	inFlight bool // asked for an acknowledgement and counts in flight
	done     bool // acknowledged, lost or never in flight
	probed   bool // its frames were queued again by a PTO
	// lost marks a packet declared lost, at lostAt, lostGap numbers behind
	// the largest acknowledged; spurious marks one acknowledged after all.
	// A lost packet stays in the ring a while to be caught being late.
	lost     bool
	spurious bool
	lostAt   time.Duration
	lostGap  uint64
	// Delivery-rate state at send time, as in the BBR draft.
	delivered   int64
	deliveredAt time.Duration
	firstSentAt time.Duration
	appLimited  bool
	frames      []sentFrame
}

// sentRing holds the sent packets by number, oldest first. Every number
// from base on has a slot; slots keep their frame buffers for reuse.
type sentRing struct {
	pk   []sentPacket
	head int
	n    int
	base uint64
}

// push returns the slot for packet pn, the next number.
//
// ai-generated: whole function.
func (r *sentRing) push(pn uint64) *sentPacket {
	if r.n == 0 {
		r.base = pn
	}
	if r.n == len(r.pk) {
		grown := make([]sentPacket, max(64, 2*len(r.pk)))
		for i := range r.n {
			grown[i] = r.pk[(r.head+i)%len(r.pk)]
		}
		r.pk, r.head = grown, 0
	}
	p := &r.pk[(r.head+r.n)%len(r.pk)]
	r.n++
	return p
}

// end is the number after the newest packet.
//
// ai-generated: whole function.
func (r *sentRing) end() uint64 { return r.base + u64(r.n) }

// at returns packet pn, which must lie in [base, end).
//
// ai-generated: whole function.
func (r *sentRing) at(pn uint64) *sentPacket {
	return &r.pk[(r.head+int(pn-r.base))%len(r.pk)] //nolint:gosec // G115: pn-base < n
}

// popDone drops the finished packets at the front; one declared lost stays
// for keep after that, so a late acknowledgement still finds it.
//
// ai-generated: whole function.
func (r *sentRing) popDone(now, keep time.Duration) {
	for r.n > 0 {
		p := &r.pk[r.head]
		if !p.done || (p.lost && now-p.lostAt < keep) {
			return
		}
		r.head = (r.head + 1) % len(r.pk)
		r.n--
		r.base++
	}
}
