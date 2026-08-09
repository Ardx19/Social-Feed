package api

import (
	"errors"
	"sync"
)

// ErrQueueFull is returned when a write queue is at capacity. The coordinator
// maps it to 503 — the queue must never silently drop an acknowledged write
// (silent loss on a 200 is the worst outcome; see design.md).
var ErrQueueFull = errors.New("write queue full")

// QueuedOp is one offline-materializable write destined for a down primary:
// absolute key/value pairs and/or deletes. Idempotent by construction, so
// draining replays them safely (a repeat apply is a no-op overwrite).
type QueuedOp struct {
	Addr    string
	Pairs   []byteKV
	Deletes [][]byte
}

// WriteQueue is a bounded, in-memory buffer of writes to down primaries. It
// lives in the coordinator (lost on restart — documented in design.md).
// Overflow returns ErrQueueFull; the caller rejects with 503 rather than drop.
//
// Eligibility is the caller's responsibility: only writes whose key/value
// pairs can be materialized without reading the down shard belong here (A-side
// follow edges, unfollows, deletes). Post creation is never queued — it needs
// the celebrity gate and fan-out pairs from the down shard, so it returns 502.
type WriteQueue struct {
	mu  sync.Mutex
	q   []QueuedOp
	max int
}

// NewWriteQueue creates a queue with a max capacity.
func NewWriteQueue(max int) *WriteQueue {
	return &WriteQueue{max: max}
}

// Enqueue appends an op. Returns ErrQueueFull at capacity.
func (w *WriteQueue) Enqueue(op QueuedOp) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.q) >= w.max {
		return ErrQueueFull
	}
	w.q = append(w.q, op)
	return nil
}

// Drain returns and removes all queued ops for a recovered primary addr.
func (w *WriteQueue) Drain(addr string) []QueuedOp {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []QueuedOp
	var kept []QueuedOp
	for _, op := range w.q {
		if op.Addr == addr {
			out = append(out, op)
		} else {
			kept = append(kept, op)
		}
	}
	w.q = kept
	return out
}

// Len returns the number of queued ops.
func (w *WriteQueue) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.q)
}
