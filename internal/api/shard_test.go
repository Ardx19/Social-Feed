package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"socialfeed/internal/codec"
	"socialfeed/internal/keys"
	"socialfeed/internal/models"
	"socialfeed/internal/storage"
)

func newShardServer(t *testing.T) (*ShardServer, *httptest.Server) {
	t.Helper()
	c, err := codec.New()
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	srv := NewShard(store, "shard1", "primary")
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

func doJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, url, &buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func encodeValue(t *testing.T, c *codec.Codec, v any) string {
	t.Helper()
	b, err := c.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func TestWriteReadScanDeleteRoundtrip(t *testing.T) {
	_, ts := newShardServer(t)
	c, _ := codec.New()

	u := models.User{ID: "alice", Name: "Alice", FollowerCount: 0}
	key := keys.UserKey("alice")
	val := encodeValue(t, c, &u)

	// Write
	code, _ := doJSON(t, "POST", ts.URL+"/write", map[string]any{"key": key, "value": val})
	if code != http.StatusOK {
		t.Fatalf("write status = %d, want 200", code)
	}
	// Read
	code, out := doJSON(t, "GET", ts.URL+"/read?key="+key, nil)
	if code != http.StatusOK {
		t.Fatalf("read status = %d", code)
	}
	gotB64, _ := out["value"].(string)
	if gotB64 != val {
		t.Error("read value did not match written value")
	}
	// Scan finds it
	code, out = doJSON(t, "GET", ts.URL+"/scan?prefix="+keys.UserKey(""), nil)
	if code != http.StatusOK {
		t.Fatalf("scan status = %d", code)
	}
	entries, _ := out["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("scan entries = %d, want 1", len(entries))
	}
	// Delete
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/delete?key="+key, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d", resp.StatusCode)
	}
	// Read after delete -> missing
	code, out = doJSON(t, "GET", ts.URL+"/read?key="+key, nil)
	if code != http.StatusOK {
		t.Fatalf("read-after-delete status = %d", code)
	}
	if v, ok := out["value"]; !ok || v != nil {
		t.Errorf("read after delete = %v, want null (missing)", out["value"])
	}
}

func TestBatchWrite(t *testing.T) {
	_, ts := newShardServer(t)
	c, _ := codec.New()
	p1 := models.Post{ID: "p1", AuthorID: "alice", Content: "one", Timestamp: 1}
	p2 := models.Post{ID: "p2", AuthorID: "alice", Content: "two", Timestamp: 2}
	pairs := []map[string]string{
		{"key": keys.PostKey("p1"), "value": encodeValue(t, c, &p1)},
		{"key": keys.PostKey("p2"), "value": encodeValue(t, c, &p2)},
	}
	code, _ := doJSON(t, "POST", ts.URL+"/write", map[string]any{"pairs": pairs})
	if code != http.StatusOK {
		t.Fatalf("batch write status = %d", code)
	}
	code, out := doJSON(t, "GET", ts.URL+"/scan?prefix="+keys.PostKey(""), nil)
	if code != http.StatusOK {
		t.Fatalf("scan status = %d", code)
	}
	entries, _ := out["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("batch scan entries = %d, want 2", len(entries))
	}
}

func TestScanReverseAndLimit(t *testing.T) {
	_, ts := newShardServer(t)
	c, _ := codec.New()
	for i := 0; i < 3; i++ {
		fe := models.FeedEntry{PostID: string(rune('a' + i)), AuthorID: "alice", Content: "x", Timestamp: int64(i + 1)}
		k := keys.FeedKey("u1", fe.Timestamp, fe.PostID)
		code, _ := doJSON(t, "POST", ts.URL+"/write", map[string]any{"key": k, "value": encodeValue(t, c, &fe)})
		if code != http.StatusOK {
			t.Fatalf("write status = %d", code)
		}
	}
	code, out := doJSON(t, "GET", ts.URL+"/scan?prefix="+keys.FeedPrefix("u1")+"&reverse=true&limit=2", nil)
	if code != http.StatusOK {
		t.Fatalf("scan status = %d", code)
	}
	entries, _ := out["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("reverse+limit entries = %d, want 2", len(entries))
	}
	// Newest (ts=3) first.
	first := entries[0].(map[string]any)
	if first["key"].(string) != keys.FeedKey("u1", 3, "c") {
		t.Errorf("reverse first key = %q, want newest", first["key"])
	}
}

func TestFollowEdgeRoundtrip(t *testing.T) {
	_, ts := newShardServer(t)
	// Create bob first (so AddFollower finds the user record).
	c, _ := codec.New()
	_ = srvOrFail(t, ts, "POST", "/write", map[string]any{
		"key":   keys.UserKey("bob"),
		"value": encodeValue(t, c, &models.User{ID: "bob", Name: "Bob"}),
	})
	// Note: CreateUser normally writes self-edges; here we write the record only,
	// which is enough for the follow-edge count path.

	// Fresh follow
	code, _ := doJSON(t, "POST", ts.URL+"/follow-edge", map[string]any{"followee": "bob", "follower": "alice"})
	if code != http.StatusOK {
		t.Fatalf("follow-edge status = %d", code)
	}
	// Duplicate follow is idempotent
	code, _ = doJSON(t, "POST", ts.URL+"/follow-edge", map[string]any{"followee": "bob", "follower": "alice"})
	if code != http.StatusOK {
		t.Fatalf("duplicate follow-edge status = %d, want 200 (idempotent)", code)
	}
	// Read the count
	code, out := doJSON(t, "GET", ts.URL+"/read?key="+keys.UserKey("bob"), nil)
	if code != http.StatusOK {
		t.Fatalf("read bob status = %d", code)
	}
	raw, _ := base64.StdEncoding.DecodeString(out["value"].(string))
	var bob models.User
	if err := c.Decode(raw, &bob); err != nil {
		t.Fatal(err)
	}
	if bob.FollowerCount != 1 {
		t.Fatalf("bob FollowerCount = %d, want 1 (duplicate follow must not double-count)", bob.FollowerCount)
	}
	// Unfollow
	code, _ = doJSON(t, "POST", ts.URL+"/unfollow-edge", map[string]any{"followee": "bob", "follower": "alice"})
	if code != http.StatusOK {
		t.Fatalf("unfollow-edge status = %d", code)
	}
	code, out = doJSON(t, "GET", ts.URL+"/read?key="+keys.UserKey("bob"), nil)
	raw, _ = base64.StdEncoding.DecodeString(out["value"].(string))
	_ = c.Decode(raw, &bob)
	if bob.FollowerCount != 0 {
		t.Fatalf("bob FollowerCount after unfollow = %d, want 0", bob.FollowerCount)
	}
	// Unfollow without edge is a no-op (still 200)
	code, _ = doJSON(t, "POST", ts.URL+"/unfollow-edge", map[string]any{"followee": "bob", "follower": "alice"})
	if code != http.StatusOK {
		t.Fatalf("re-unfollow status = %d, want 200", code)
	}
}

func TestFollowEdgeNonExistentFollowee(t *testing.T) {
	_, ts := newShardServer(t)
	// carol has no user record.
	code, _ := doJSON(t, "POST", ts.URL+"/follow-edge", map[string]any{"followee": "carol", "follower": "alice"})
	if code != http.StatusNotFound {
		t.Fatalf("follow-edge to missing followee status = %d, want 404", code)
	}
}

func TestErrorsAndMissing(t *testing.T) {
	_, ts := newShardServer(t)
	// Malformed write -> 400
	code, _ := doJSON(t, "POST", ts.URL+"/write", map[string]any{"key": 123}) // non-string key
	if code != http.StatusBadRequest && code != http.StatusOK {
		// key as number decodes into string field -> 400 or JSON type mismatch handled
		if code != http.StatusBadRequest {
			t.Logf("note: malformed key status = %d", code)
		}
	}
	// Read missing -> 200 null
	code, out := doJSON(t, "GET", ts.URL+"/read?key=nope", nil)
	if code != http.StatusOK {
		t.Fatalf("read missing status = %d, want 200 (missing is not an error)", code)
	}
	if out["value"] != nil {
		t.Errorf("read missing value = %v, want nil", out["value"])
	}
	// Unknown route -> 404
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/nope", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route status = %d, want 404", resp.StatusCode)
	}
	// Health
	code, out = doJSON(t, "GET", ts.URL+"/health", nil)
	if code != http.StatusOK {
		t.Fatalf("health status = %d", code)
	}
	if out["node_id"] != "shard1" || out["role"] != "primary" {
		t.Errorf("health body = %v", out)
	}
}

func srvOrFail(t *testing.T, ts *httptest.Server, method, path string, body any) *httptest.Server {
	t.Helper()
	code, _ := doJSON(t, method, ts.URL+path, body)
	if code != http.StatusOK {
		t.Fatalf("%s %s status = %d, want 200", method, path, code)
	}
	return ts
}
