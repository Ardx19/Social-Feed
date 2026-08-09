package api

import "testing"

func TestWriteQueueOverflow(t *testing.T) {
	q := NewWriteQueue(2)
	if err := q.Enqueue(QueuedOp{Addr: "s1"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(QueuedOp{Addr: "s1"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(QueuedOp{Addr: "s1"}); err != ErrQueueFull {
		t.Fatalf("overflow err = %v, want ErrQueueFull", err)
	}
}

func TestWriteQueueDrain(t *testing.T) {
	q := NewWriteQueue(10)
	_ = q.Enqueue(QueuedOp{Addr: "s1", Pairs: []byteKV{{Key: "a", Value: "1"}}})
	_ = q.Enqueue(QueuedOp{Addr: "s2", Pairs: []byteKV{{Key: "b", Value: "2"}}})
	_ = q.Enqueue(QueuedOp{Addr: "s1", Pairs: []byteKV{{Key: "c", Value: "3"}}})

	drained := q.Drain("s1")
	if len(drained) != 2 {
		t.Fatalf("drained s1 = %d, want 2", len(drained))
	}
	// In order.
	if drained[0].Pairs[0].Key != "a" || drained[1].Pairs[0].Key != "c" {
		t.Errorf("drain order wrong: %q then %q", drained[0].Pairs[0].Key, drained[1].Pairs[0].Key)
	}
	// s2 remains.
	if q.Len() != 1 {
		t.Fatalf("remaining queue = %d, want 1 (s2)", q.Len())
	}
	// Draining again for s1 is a no-op.
	if got := q.Drain("s1"); len(got) != 0 {
		t.Errorf("second drain returned %d, want 0", len(got))
	}
}
