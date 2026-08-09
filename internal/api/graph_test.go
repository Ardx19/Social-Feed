package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// publicCoordinator spins up a coordinator with 3 real fake shards and returns
// its public httptest server.
func publicCoordinator(t *testing.T) *httptest.Server {
	t.Helper()
	coord := testCoordinator(t, map[string]string{
		"s1": fakeShard(t, "s1"),
		"s2": fakeShard(t, "s2"),
		"s3": fakeShard(t, "s3"),
	}, nil, 1000)
	ts := httptest.NewServer(coord.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestCreateAndGetUser(t *testing.T) {
	ts := publicCoordinator(t)
	code, out := doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "alice", "name": "Alice"})
	if code != http.StatusOK {
		t.Fatalf("create user status = %d, want 200", code)
	}
	if out["id"] != "alice" {
		t.Errorf("create response = %v", out)
	}
	code, out = doJSON(t, "GET", ts.URL+"/users/alice", nil)
	if code != http.StatusOK {
		t.Fatalf("get user status = %d, want 200", code)
	}
	if out["follower_count"] != float64(0) {
		t.Errorf("new user follower_count = %v, want 0 (self-edge not counted)", out["follower_count"])
	}
}

func TestCreateUserDuplicate409(t *testing.T) {
	ts := publicCoordinator(t)
	_, _ = doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "bob", "name": "Bob"})
	code, _ := doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "bob", "name": "Bob"})
	if code != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want 409 (no overwrite)", code)
	}
}

func TestCreateUserInvalidID(t *testing.T) {
	ts := publicCoordinator(t)
	for _, id := range []string{"bad:colon", "with space", ""} {
		code, _ := doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": id, "name": "X"})
		if code != http.StatusBadRequest {
			t.Errorf("invalid id %q status = %d, want 400", id, code)
		}
	}
}

func TestGetUserNotFound(t *testing.T) {
	ts := publicCoordinator(t)
	code, _ := doJSON(t, "GET", ts.URL+"/users/ghost", nil)
	if code != http.StatusNotFound {
		t.Fatalf("get ghost status = %d, want 404", code)
	}
}

func TestFollowUnfollowFlow(t *testing.T) {
	ts := publicCoordinator(t)
	_, _ = doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "alice", "name": "Alice"})
	_, _ = doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "bob", "name": "Bob"})

	// Follow
	code, _ := doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "alice", "followee": "bob"})
	if code != http.StatusOK {
		t.Fatalf("follow status = %d, want 200", code)
	}
	// Duplicate follow is idempotent
	code, _ = doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "alice", "followee": "bob"})
	if code != http.StatusOK {
		t.Fatalf("duplicate follow status = %d, want 200", code)
	}
	// bob's follower count = 1 (not 2)
	_, out := doJSON(t, "GET", ts.URL+"/users/bob", nil)
	if out["follower_count"] != float64(1) {
		t.Fatalf("bob follower_count after dup follow = %v, want 1", out["follower_count"])
	}
	// Unfollow
	code, _ = doJSON(t, "POST", ts.URL+"/unfollow", map[string]any{"follower": "alice", "followee": "bob"})
	if code != http.StatusOK {
		t.Fatalf("unfollow status = %d", code)
	}
	_, out = doJSON(t, "GET", ts.URL+"/users/bob", nil)
	if out["follower_count"] != float64(0) {
		t.Fatalf("bob follower_count after unfollow = %v, want 0", out["follower_count"])
	}
}

func TestFollowEdgeCases(t *testing.T) {
	ts := publicCoordinator(t)
	_, _ = doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "alice", "name": "Alice"})

	// follow self -> 400
	code, _ := doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "alice", "followee": "alice"})
	if code != http.StatusBadRequest {
		t.Fatalf("self-follow status = %d, want 400", code)
	}
	// follow missing followee -> 404
	code, _ = doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "alice", "followee": "ghost"})
	if code != http.StatusNotFound {
		t.Fatalf("follow missing followee status = %d, want 404", code)
	}
	// follow by missing follower -> 404
	code, _ = doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "ghost", "followee": "alice"})
	if code != http.StatusNotFound {
		t.Fatalf("follow by missing follower status = %d, want 404", code)
	}
	// invalid ID -> 400
	code, _ = doJSON(t, "POST", ts.URL+"/follow", map[string]any{"follower": "bad:id", "followee": "alice"})
	if code != http.StatusBadRequest {
		t.Fatalf("invalid follow id status = %d, want 400", code)
	}
}

func TestUnfollowWhenNotFollowing(t *testing.T) {
	ts := publicCoordinator(t)
	_, _ = doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "alice", "name": "Alice"})
	_, _ = doJSON(t, "POST", ts.URL+"/users", map[string]any{"id": "bob", "name": "Bob"})
	code, _ := doJSON(t, "POST", ts.URL+"/unfollow", map[string]any{"follower": "alice", "followee": "bob"})
	if code != http.StatusOK {
		t.Fatalf("unfollow-when-not-following status = %d, want 200 (idempotent no-op)", code)
	}
}
