package replication

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// Op is one committed primary transaction's write-set, sent to the replica as
// absolute final values (never operations — op replay would double-apply
// increments). It carries a whole transaction so the replica applies the
// edge + count (or any pair) atomically and drops/keeps them as a unit.
type Op struct {
	Pairs   []Pair   `json:"pairs"`
	Deletes []string `json:"deletes"`
}

// Pair is an absolute key/value.
type Pair struct {
	Key   []byte
	Value []byte
}

// wire form for JSON (value base64).
type wirePair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Replicator sends a primary's committed write-sets to its replica over ONE
// ordered channel drained by a single goroutine, so enqueue order == apply
// order (per-key order by construction — per-write goroutines would let two
// updates to the same key arrive out of order and leave the replica wrong).
//
// Enqueue is non-blocking: on a full buffer the NEWEST op is dropped and the
// replica stays stale on those keys until the next write re-sends a fresh
// absolute value (self-healing under traffic). Replication never sits on the
// primary's write latency, and there is no ack or retry. See design.md.
type Replicator struct {
	ch     chan Op
	url    string
	client *http.Client
	done   chan struct{}
}

// NewReplicator starts a replicator POSTing to the replica's /replicate.
// replicaURL is host:port (no scheme).
func NewReplicator(replicaURL string, buffer int) *Replicator {
	r := &Replicator{
		ch:     make(chan Op, buffer),
		url:    "http://" + replicaURL + "/replicate",
		client: &http.Client{Timeout: 2 * time.Second},
		done:   make(chan struct{}),
	}
	go r.loop()
	return r
}

func (r *Replicator) loop() {
	for {
		select {
		case <-r.done:
			return
		case op := <-r.ch:
			r.post(op)
		}
	}
}

func (r *Replicator) post(op Op) {
	body, err := json.Marshal(wire(op))
	if err != nil {
		log.Printf("replicate: marshal: %v", err)
		return
	}
	resp, err := r.client.Post(r.url, "application/json", bytes.NewReader(body))
	if err != nil {
		// Dropped — the replica is now stale on these keys until the next
		// write to them re-sends a fresh absolute value (self-healing).
		log.Printf("replicate: %v (dropped; replica stale until next write)", err)
		return
	}
	resp.Body.Close()
}

// Enqueue adds a committed write-set to the ordered channel. Non-blocking: a
// full buffer drops the newest op (logged), never blocks the primary write.
func (r *Replicator) Enqueue(op Op) {
	select {
	case r.ch <- op:
	default:
		log.Printf("replicate: buffer full, dropping newest op (documented async gap)")
	}
}

// Stop drains outstanding work and shuts down the drain goroutine.
func (r *Replicator) Stop() { close(r.done) }

func wire(op Op) (struct {
	Pairs   []wirePair `json:"pairs"`
	Deletes []string   `json:"deletes"`
}, ) {
	w := struct {
		Pairs   []wirePair `json:"pairs"`
		Deletes []string   `json:"deletes"`
	}{Deletes: op.Deletes}
	for _, p := range op.Pairs {
		w.Pairs = append(w.Pairs, wirePair{Key: string(p.Key), Value: base64.StdEncoding.EncodeToString(p.Value)})
	}
	return w
}
