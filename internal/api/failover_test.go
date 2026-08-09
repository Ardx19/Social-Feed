package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"socialfeed/internal/keys"
)

// markDown forces a node's monitor state to down (deterministic; the heartbeat
// loop uses a 5s default interval so it won't interfere during a fast test).
func markDown(t *testing.T, coord *CoordinatorServer, nodeID string) {
	t.Helper()
	coord.monitor.mu.Lock()
	defer coord.monitor.mu.Unlock()
	coord.monitor.state[nodeID].down = true
}

func markUp(t *testing.T, coord *CoordinatorServer, nodeID string) {
	t.Helper()
	coord.monitor.mu.Lock()
	defer coord.monitor.mu.Unlock()
	coord.monitor.state[nodeID].down = false
	coord.monitor.state[nodeID].misses = 0
}

// The node that owns "author" for the post test.
func nodeFor(t *testing.T, coord *CoordinatorServer, id string) string {
	t.Helper()
	n, _, err := coord.ShardFor(id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostToDownAuthorIs502NeverQueued(t *testing.T) {
	coord, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")

	markDown(t, coord, nodeFor(t, coord, "alice"))
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "hi"})
	if code != http.StatusBadGateway {
		t.Fatalf("post to down author status = %d, want 502 (never queued)", code)
	}
	if coord.queue.Len() != 0 {
		t.Fatalf("queue len = %d, want 0 (post creation must not be queued)", coord.queue.Len())
	}
}

func TestFollowASideQueuedWhenFollowerDown(t *testing.T) {
	coord, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	mustCreate(t, ts, "bob")
	followerNode := nodeFor(t, coord, "alice")

	markDown(t, coord, followerNode)
	code, _ := doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "alice", "followee": "bob"})
	if code != http.StatusOK {
		t.Fatalf("follow with follower shard down status = %d, want 200 (A-side queued)", code)
	}
	// The A-side following edge is queued, not lost.
	if coord.queue.Len() != 1 {
		t.Fatalf("queue len = %d, want 1 (A-side queued)", coord.queue.Len())
	}
}

func TestDrainReplaysQueuedASideEdge(t *testing.T) {
	coord, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	mustCreate(t, ts, "bob")
	followerNode, followerAddr, _ := coord.ShardFor("alice")

	// Queue the A-side edge while the follower's shard is "down".
	markDown(t, coord, followerNode)
	code, _ := doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "alice", "followee": "bob"})
	if code != http.StatusOK {
		t.Fatalf("follow status = %d", code)
	}
	// The B-side also ran (bob's shard up), so bob's count went up.
	_, out := doJSON(t, "GET", ts.URL+"/users/bob", nil)
	if out["follower_count"] != float64(1) {
		t.Fatalf("bob count = %v, want 1", out["follower_count"])
	}

	// Recover the follower's shard and drain.
	markUp(t, coord, followerNode)
	coord.drainQueue(followerNode)

	if coord.queue.Len() != 0 {
		t.Fatalf("queue len after drain = %d, want 0", coord.queue.Len())
	}
	// The following edge now exists on the follower's shard.
	_, found, err := coord.Client().Read(followerAddr, keys.FollowingEdgeKey("alice", "bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("A-side following edge not replayed after drain")
	}
}

func TestReadFailoverToReplicaWhenPrimaryDown(t *testing.T) {
	// A primary that is down (unreachable) with a working replica. The
	// coordinator must serve reads from the replica.
	primaryTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // primary broken
	}))
	defer primaryTS.Close()
	replicaTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"value":"cmVwbGljYQ=="}`)) // base64 "replica"
	}))
	defer replicaTS.Close()

	coord := testCoordinator(t, map[string]string{"s1": trimHost(primaryTS.URL)}, map[string]string{"s1": trimHost(replicaTS.URL)}, 1000)
	// Mark s1 down so readAddrFor picks the replica.
	markDown(t, coord, "s1")

	nodeID, addr, err := coord.readAddrFor("somekey")
	if err != nil {
		t.Fatal(err)
	}
	if nodeID != "s1" || addr != trimHost(replicaTS.URL) {
		t.Fatalf("readAddrFor = %s,%s want replica", nodeID, addr)
	}
	val, found, err := coord.readWithFailover(nodeID, addr, "k")
	if err != nil || !found {
		t.Fatalf("readWithFailover err=%v found=%v", err, found)
	}
	if string(val) != "replica" {
		t.Fatalf("read value = %q, want replica", val)
	}
}
