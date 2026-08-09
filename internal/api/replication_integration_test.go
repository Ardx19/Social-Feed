package api

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"socialfeed/internal/codec"
	"socialfeed/internal/replication"
	"socialfeed/internal/storage"
)

// TestPrimaryReplicatesToReplica verifies the full chain: a primary shard write
// triggers the store's commit hook, which enqueues to a replicator, which POSTs
// to the replica's /replicate, which applies the write-set.
func TestPrimaryReplicatesToReplica(t *testing.T) {
	c, _ := codec.New()

	// Replica shard (real store + shard server).
	repStore, err := storage.Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repStore.Close() })
	repSrv := NewShard(repStore, "replica1", "replica")
	repTS := httptest.NewServer(repSrv.Handler())
	t.Cleanup(repTS.Close)
	replicaURL := strings.TrimPrefix(repTS.URL, "http://")

	// Primary store with commit hook wired to a replicator.
	priStore, err := storage.Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = priStore.Close() })
	rep := replication.NewReplicator(replicaURL, 64)
	t.Cleanup(rep.Stop)
	priStore.SetCommitHook(func(ws storage.WriteSet) {
		op := replication.Op{}
		for _, p := range ws.Pairs {
			op.Pairs = append(op.Pairs, replication.Pair{Key: p.Key, Value: p.Value})
		}
		for _, d := range ws.Deletes {
			op.Deletes = append(op.Deletes, string(d))
		}
		rep.Enqueue(op)
	})

	// Write to the primary through its shard API.
	priSrv := NewShard(priStore, "shard1", "primary")
	priTS := httptest.NewServer(priSrv.Handler())
	t.Cleanup(priTS.Close)

	code, _ := doJSON(t, "POST", priTS.URL+"/write", map[string]any{"key": "k1", "value": "dmFsdWUx"})
	if code != 200 {
		t.Fatalf("primary write status = %d", code)
	}

	// Wait for async replication to land, then verify the replica has it.
	deadline := time.Now().Add(3 * time.Second)
	var found bool
	for !found && time.Now().Before(deadline) {
		_, f, _ := repStore.Get([]byte("k1"))
		found = f
		if !found {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !found {
		t.Fatal("replica never received the write")
	}
	val, _, _ := repStore.Get([]byte("k1"))
	// The shard stores the base64-decoded raw bytes ("value1"), which is what
	// must replicate — byte-for-byte identical to the primary.
	if string(val) != "value1" {
		t.Fatalf("replica value = %q, want %q (byte-identical to primary)", val, "value1")
	}
	priVal, _, _ := priStore.Get([]byte("k1"))
	if string(priVal) != string(val) {
		t.Fatalf("replica %q != primary %q", val, priVal)
	}
}

// TestReplicaDownDoesNotBlockPrimary verifies fire-and-forget: a down replica
// never blocks the primary's write path.
func TestReplicaDownDoesNotBlockPrimary(t *testing.T) {
	c, _ := codec.New()
	priStore, err := storage.Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = priStore.Close() })

	// Point at a dead address.
	rep := replication.NewReplicator("127.0.0.1:1", 64)
	t.Cleanup(rep.Stop)
	priStore.SetCommitHook(func(ws storage.WriteSet) {
		op := replication.Op{}
		for _, p := range ws.Pairs {
			op.Pairs = append(op.Pairs, replication.Pair{Key: p.Key, Value: p.Value})
		}
		rep.Enqueue(op)
	})

	priSrv := NewShard(priStore, "shard1", "primary")
	priTS := httptest.NewServer(priSrv.Handler())
	t.Cleanup(priTS.Close)

	// The write must succeed quickly even though the replica is unreachable.
	start := time.Now()
	code, _ := doJSON(t, "POST", priTS.URL+"/write", map[string]any{"key": "k", "value": "dg=="})
	if code != 200 {
		t.Fatalf("primary write status = %d", code)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("primary write blocked on a down replica (took %v)", time.Since(start))
	}
	// The write landed locally regardless.
	if _, found, _ := priStore.Get([]byte("k")); !found {
		t.Fatal("primary write not stored locally")
	}
}
