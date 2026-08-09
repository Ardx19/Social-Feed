package replication

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingReplica is a fake /replicate server that records delivered ops.
// When block != nil, each op signals `entered` then waits for a release token,
// so tests can synchronize deterministically.
type recordingReplica struct {
	mu      sync.Mutex
	ops     []Op
	block   chan struct{}
	entered chan struct{}
}

func (rr *recordingReplica) handler(w http.ResponseWriter, r *http.Request) {
	var wire struct {
		Pairs   []wirePair `json:"pairs"`
		Deletes []string   `json:"deletes"`
	}
	_ = json.NewDecoder(r.Body).Decode(&wire)
	op := Op{Deletes: wire.Deletes}
	for _, p := range wire.Pairs {
		v, err := base64.StdEncoding.DecodeString(p.Value)
		if err != nil {
			v = nil
		}
		op.Pairs = append(op.Pairs, Pair{Key: []byte(p.Key), Value: v})
	}
	if rr.block != nil {
		rr.entered <- struct{}{} // tell the test we're about to block
		<-rr.block               // wait for release
	}
	rr.mu.Lock()
	rr.ops = append(rr.ops, op)
	rr.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (rr *recordingReplica) delivered() int {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return len(rr.ops)
}

func (rr *recordingReplica) valueOf(i int, key string) string {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for _, p := range rr.ops[i].Pairs {
		if string(p.Key) == key {
			return string(p.Value)
		}
	}
	return ""
}

func newReplica(t *testing.T, rr *recordingReplica, buffer int) (*Replicator, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(rr.handler))
	t.Cleanup(ts.Close)
	rep := NewReplicator(strings.TrimPrefix(ts.URL, "http://"), buffer)
	t.Cleanup(rep.Stop)
	return rep, ts
}

func TestReplicatorPreservesOrder(t *testing.T) {
	rr := &recordingReplica{}
	rep, _ := newReplica(t, rr, 100)

	// Two rapid updates to the same key (count 5->6, then 6->7) must arrive in
	// order, or the replica settles on the wrong value. Single-drain ordering.
	rep.Enqueue(Op{Pairs: []Pair{{Key: []byte("count"), Value: []byte("6")}}})
	rep.Enqueue(Op{Pairs: []Pair{{Key: []byte("count"), Value: []byte("7")}}})

	deadline := time.Now().Add(2 * time.Second)
	for rr.delivered() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rr.delivered() != 2 {
		t.Fatalf("delivered = %d, want 2", rr.delivered())
	}
	if rr.valueOf(0, "count") != "6" || rr.valueOf(1, "count") != "7" {
		t.Fatalf("order wrong: %s then %s (want 6 then 7)", rr.valueOf(0, "count"), rr.valueOf(1, "count"))
	}
}

func TestReplicatorDropsNewestOnOverflow(t *testing.T) {
	// A blocked replica + buffer 1: op1 is taken by the drain and blocks; op2
	// fills the buffer; op3 must be dropped (never delivered). Deterministic
	// handshake via the replica's entered/release channels.
	rr := &recordingReplica{block: make(chan struct{}), entered: make(chan struct{})}
	rep, _ := newReplica(t, rr, 1)

	rep.Enqueue(Op{Pairs: []Pair{{Key: []byte("k"), Value: []byte("1")}}})
	<-rr.entered // drain has op1, about to block on the replica

	rep.Enqueue(Op{Pairs: []Pair{{Key: []byte("k"), Value: []byte("2")}}}) // fills buffer
	rep.Enqueue(Op{Pairs: []Pair{{Key: []byte("k"), Value: []byte("3")}}}) // buffer full -> dropped

	rr.block <- struct{}{} // release op1
	<-rr.entered           // drain has op2, about to block
	rr.block <- struct{}{} // release op2

	deadline := time.Now().Add(2 * time.Second)
	for rr.delivered() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rr.delivered() != 2 {
		t.Fatalf("delivered = %d, want 2 (op3 dropped on overflow, never delivered)", rr.delivered())
	}
}

func TestReplicatorCarriesDeletes(t *testing.T) {
	rr := &recordingReplica{}
	rep, _ := newReplica(t, rr, 10)
	rep.Enqueue(Op{Deletes: []string{"edge:key"}})
	deadline := time.Now().Add(2 * time.Second)
	for rr.delivered() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if len(rr.ops) != 1 || len(rr.ops[0].Deletes) != 1 || rr.ops[0].Deletes[0] != "edge:key" {
		t.Fatalf("delete not replicated: %+v", rr.ops)
	}
}
