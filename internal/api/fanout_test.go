package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"socialfeed/internal/keys"
)

// publicCoordinatorWith returns a coordinator (with real fake shards) and its
// public server, with a configurable celebrity threshold.
func publicCoordinatorWith(t *testing.T, threshold int64) (*CoordinatorServer, *httptest.Server) {
	t.Helper()
	coord := testCoordinator(t, map[string]string{
		"s1": fakeShard(t, "s1"),
		"s2": fakeShard(t, "s2"),
		"s3": fakeShard(t, "s3"),
	}, nil, threshold)
	ts := httptest.NewServer(coord.Handler())
	t.Cleanup(ts.Close)
	return coord, ts
}

func mustCreate(t *testing.T, ts *httptest.Server, id string) {
	t.Helper()
	code, _ := doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": id, "name": id})
	if code != http.StatusOK {
		t.Fatalf("create %s status = %d, want 200", id, code)
	}
}

func mustFollow(t *testing.T, ts *httptest.Server, follower, followee string) {
	t.Helper()
	code, _ := doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": follower, "followee": followee})
	if code != http.StatusOK {
		t.Fatalf("follow %s->%s status = %d, want 200", follower, followee, code)
	}
}

// feedLen scans a user's feed on their shard and returns how many entries it has.
func feedLen(t *testing.T, coord *CoordinatorServer, user string) int {
	t.Helper()
	_, addr, err := coord.ShardFor(user)
	if err != nil {
		t.Fatal(err)
	}
	pairs, err := coord.Client().Scan(addr, keys.FeedPrefix(user), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	return len(pairs)
}

func TestNonCelebrityFansOut(t *testing.T) {
	coord, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	mustCreate(t, ts, "bob")
	mustFollow(t, ts, "bob", "alice") // bob follows alice

	code, out := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "hello world"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d, want 200", code)
	}
	postID, _ := out["post_id"].(string)
	if postID == "" {
		t.Fatal("no post_id returned")
	}
	// Bob's feed (on bob's shard) must contain the entry (fan-out happened).
	if n := feedLen(t, coord, "bob"); n != 1 {
		t.Fatalf("bob feed entries = %d, want 1 (fan-out on write)", n)
	}
	// GET /posts/{id} resolves via the in-memory post->author map.
	code, p := doJSON(t, "GET", ts.URL+"/posts/"+postID, nil)
	if code != http.StatusOK {
		t.Fatalf("get post status = %d, want 200", code)
	}
	if p["content"] != "hello world" || p["author_id"] != "alice" {
		t.Errorf("post = %v", p)
	}
}

func TestCelebrityDoesNotFanOut(t *testing.T) {
	// threshold=2: a user with 2 real followers is a celebrity -> lazy delivery.
	coord, ts := publicCoordinatorWith(t, 2)
	mustCreate(t, ts, "celeb")
	mustCreate(t, ts, "f1")
	mustCreate(t, ts, "f2")
	mustFollow(t, ts, "f1", "celeb")
	mustFollow(t, ts, "f2", "celeb")

	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "celeb", "content": "big post"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d", code)
	}
	// No fan-out: follower feeds stay empty (delivery is lazy, on read).
	if n := feedLen(t, coord, "f1"); n != 0 {
		t.Fatalf("f1 feed entries = %d, want 0 (celebrity lazy delivery)", n)
	}
	if n := feedLen(t, coord, "f2"); n != 0 {
		t.Fatalf("f2 feed entries = %d, want 0", n)
	}
	// Self-edge: celeb's own feed gets nothing either (celebrity reads own via read path).
	if n := feedLen(t, coord, "celeb"); n != 0 {
		t.Fatalf("celeb own feed entries = %d, want 0 (no fan-out)", n)
	}
}

func TestThresholdBoundary(t *testing.T) {
	// threshold=2: 1 follower -> fan-out; 2 followers -> celebrity.
	coord, ts := publicCoordinatorWith(t, 2)
	mustCreate(t, ts, "normal")
	mustCreate(t, ts, "f1")
	mustFollow(t, ts, "f1", "normal") // normal has 1 follower < 2 -> fan-out
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "normal", "content": "x"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d", code)
	}
	if n := feedLen(t, coord, "f1"); n != 1 {
		t.Fatalf("f1 feed entries = %d, want 1 (fan-out)", n)
	}
}

func TestPostValidation(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	// Missing author -> 404
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "ghost", "content": "x"})
	if code != http.StatusNotFound {
		t.Fatalf("post by missing author status = %d, want 404", code)
	}
	// Empty content -> 400
	code, _ = doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "  "})
	if code != http.StatusBadRequest {
		t.Fatalf("empty content status = %d, want 400", code)
	}
	// Invalid author ID -> 400
	code, _ = doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "bad:id", "content": "x"})
	if code != http.StatusBadRequest {
		t.Fatalf("invalid author status = %d, want 400", code)
	}
}

func TestGetPostNotFound(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 1000)
	// Unknown post (no post->author mapping) -> 404.
	code, _ := doJSON(t, "GET", ts.URL+"/posts/doesnotexist", nil)
	if code != http.StatusNotFound {
		t.Fatalf("get unknown post status = %d, want 404", code)
	}
}
