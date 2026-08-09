package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"socialfeed/internal/codec"
	"socialfeed/internal/keys"
	"socialfeed/internal/models"
	"socialfeed/internal/storage"
)

// fakeShard spins up a real ShardServer on httptest and returns its addr.
func fakeShard(t *testing.T, id string) (addr string) {
	t.Helper()
	c, _ := codec.New()
	store, err := storage.Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ts := httptest.NewServer(NewShard(store, id, "primary").Handler())
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func testCoordinator(t *testing.T, shards, replicas map[string]string, threshold int64) *CoordinatorServer {
	t.Helper()
	return NewCoordinator(CoordinatorConfig{
		Shards:    shards,
		Replicas:  replicas,
		Threshold: threshold,
		Client:    NewShardClient(2 * 1000 * 1000 * 1000),
	})
}

func TestShardForConsistentAndComplete(t *testing.T) {
	coord := testCoordinator(t, map[string]string{
		"s1": fakeShard(t, "s1"),
		"s2": fakeShard(t, "s2"),
		"s3": fakeShard(t, "s3"),
	}, nil, 1000)

	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := "user" + strings.Repeat(string(rune('a'+i%26)), i%5+1)
		node1, _, err := coord.ShardFor(id)
		if err != nil {
			t.Fatal(err)
		}
		node2, _, _ := coord.ShardFor(id)
		if node1 != node2 {
			t.Fatalf("ShardFor(%q) inconsistent: %q vs %q", id, node1, node2)
		}
		seen[node1] = true
	}
	for _, n := range []string{"s1", "s2", "s3"} {
		if !seen[n] {
			t.Errorf("node %s never chosen", n)
		}
	}
}

func TestClientClassification(t *testing.T) {
	c := NewShardClient(2 * 1000 * 1000 * 1000)

	// 200 + null => missing (found=false, no error)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":null}`))
	}))
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")
	_, found, err := c.Read(addr, "somekey")
	if err != nil || found {
		t.Fatalf("missing read: found=%v err=%v, want found=false err=nil", found, err)
	}

	// 500 => shard-error
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer ts2.Close()
	addr2 := strings.TrimPrefix(ts2.URL, "http://")
	if _, _, err := c.Read(addr2, "k"); err == nil || !errorsIs(err, ErrShard) {
		t.Fatalf("500 read err = %v, want ErrShard", err)
	}

	// connection refused => down
	if _, _, err := c.Read("127.0.0.1:1", "k"); err == nil || !errorsIs(err, ErrDown) {
		t.Fatalf("refused read err = %v, want ErrDown", err)
	}
}

// errorsIs is a tiny alias to avoid importing errors in a helper-heavy test.
func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestRoutingWriteThroughClient(t *testing.T) {
	shards := map[string]string{
		"s1": fakeShard(t, "s1"),
		"s2": fakeShard(t, "s2"),
		"s3": fakeShard(t, "s3"),
	}
	coord := testCoordinator(t, shards, nil, 1000)
	c, _ := codec.New()

	// Route a user to its shard, write it, read it back from that shard.
	userID := "alice"
	nodeID, addr, err := coord.ShardFor(userID)
	if err != nil {
		t.Fatal(err)
	}
	u := models.User{ID: userID, Name: "Alice", FollowerCount: 0}
	val, _ := c.Encode(&u)
	b64 := base64.StdEncoding.EncodeToString(val)
	code, out := doJSON(t, "POST", "http://"+addr+"/write", map[string]any{"key": keys.UserKey(userID), "value": b64})
	if code != http.StatusOK {
		t.Fatalf("write status = %d", code)
	}
	got, found, err := coord.Client().Read(addr, keys.UserKey(userID))
	if err != nil || !found || len(got) == 0 {
		t.Fatalf("read back via client: found=%v err=%v", found, err)
	}
	var gotUser models.User
	if err := c.Decode(got, &gotUser); err != nil {
		t.Fatal(err)
	}
	if gotUser.ID != userID {
		t.Errorf("roundtrip user ID = %q, want %q", gotUser.ID, userID)
	}
	_ = out
	_ = nodeID
}

func TestClusterListsTopology(t *testing.T) {
	shards := map[string]string{"s1": fakeShard(t, "s1"), "s2": fakeShard(t, "s2")}
	replicas := map[string]string{"s1": fakeShard(t, "r1"), "s2": fakeShard(t, "r2")}
	coord := testCoordinator(t, shards, replicas, 1000)
	ts := httptest.NewServer(coord.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 4 {
		t.Fatalf("cluster nodes = %d, want 4 (2 shards + 2 replicas)", len(out))
	}
	for _, n := range out {
		if n["reachable"] != true {
			t.Errorf("node %v not reachable", n["id"])
		}
	}
}

func TestParseTopology(t *testing.T) {
	// Two-segment form (plan.txt / compose): host == nodeID.
	two := ParseTopology("shard1:8081,shard2:8082,shard3:8083")
	if len(two) != 3 {
		t.Fatalf("2-seg topology parsed %d nodes, want 3", len(two))
	}
	if two["shard1"] != "shard1:8081" {
		t.Errorf("shard1 addr = %q, want shard1:8081", two["shard1"])
	}
	// Three-segment form: explicit host.
	three := ParseTopology("s1:10.0.0.1:8081")
	if three["s1"] != "10.0.0.1:8081" {
		t.Errorf("3-seg addr = %q", three["s1"])
	}
	// Empty input yields empty map, not nil.
	if got := ParseTopology(""); got == nil || len(got) != 0 {
		t.Errorf("empty topology = %v, want empty map", got)
	}
}
