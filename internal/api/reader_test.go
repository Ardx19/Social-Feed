package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// feedEntries hits GET /feed/{user} and returns the parsed entries + cursor.
func feedEntries(t *testing.T, ts *httptest.Server, user, before string, limit int) ([]map[string]any, string) {
	t.Helper()
	url := ts.URL + "/feed/" + user + "?limit=" + strconv.Itoa(limit)
	if before != "" {
		url += "&before=" + before
	}
	code, out := doJSON(t, "GET", url, nil)
	if code != http.StatusOK {
		t.Fatalf("feed %s status = %d, want 200", user, code)
	}
	entries, _ := out["entries"].([]any)
	res := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		res = append(res, e.(map[string]any))
	}
	cursor, _ := out["next_cursor"].(string)
	return res, cursor
}

func TestFeedEmptyReturnsEmptyArray(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	entries, cursor := feedEntries(t, ts, "alice", "", 20)
	if len(entries) != 0 {
		t.Fatalf("empty feed entries = %d, want 0", len(entries))
	}
	if cursor != "" {
		t.Errorf("empty feed cursor = %q, want empty", cursor)
	}
}

// AE1: non-celebrity author's post reaches the follower's feed via fan-out.
func TestFeedNonCelebrityViaFanOut(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	mustCreate(t, ts, "bob")
	mustFollow(t, ts, "bob", "alice") // bob follows alice (alice is normal)
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "hello bob"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d", code)
	}
	entries, _ := feedEntries(t, ts, "bob", "", 20)
	if len(entries) != 1 {
		t.Fatalf("bob feed len = %d, want 1", len(entries))
	}
	if entries[0]["content"] != "hello bob" || entries[0]["author_id"] != "alice" {
		t.Errorf("bob feed entry = %v", entries[0])
	}
}

// AE3: follower sees a celebrity's post via the author index (no fan-out).
func TestFeedCelebrityPostReachesFollower(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 2) // threshold 2
	mustCreate(t, ts, "celeb")
	mustCreate(t, ts, "f1")
	mustCreate(t, ts, "f2")
	mustFollow(t, ts, "f1", "celeb")
	mustFollow(t, ts, "f2", "celeb") // celeb has 2 followers -> celebrity
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "celeb", "content": "famous"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d", code)
	}
	// f1's feed must show the celebrity post (lazy, via author index).
	entries, _ := feedEntries(t, ts, "f1", "", 20)
	if len(entries) != 1 {
		t.Fatalf("f1 feed len = %d, want 1 (celebrity lazy delivery)", len(entries))
	}
	if entries[0]["content"] != "famous" || entries[0]["author_id"] != "celeb" {
		t.Errorf("f1 feed entry = %v", entries[0])
	}
}

// AE2: a celebrity sees their OWN post via the read path (self is a celebrity
// followee), with no fan-out.
func TestCelebrityOwnPostInOwnFeed(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 2)
	mustCreate(t, ts, "celeb")
	mustCreate(t, ts, "f1")
	mustCreate(t, ts, "f2")
	mustFollow(t, ts, "f1", "celeb")
	mustFollow(t, ts, "f2", "celeb")
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "celeb", "content": "my post"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d", code)
	}
	entries, _ := feedEntries(t, ts, "celeb", "", 20)
	if len(entries) != 1 {
		t.Fatalf("celeb own feed len = %d, want 1 (via read path, self followee)", len(entries))
	}
	if entries[0]["author_id"] != "celeb" {
		t.Errorf("celeb own feed entry = %v", entries[0])
	}
}

// Own posts in feed for a NORMAL user (AE1): alice's own feed shows her post
// because self is in her follower set and fan-out writes to her own feed.
func TestNormalUserOwnPostInOwnFeed(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 1000)
	mustCreate(t, ts, "alice")
	code, _ := doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "just me"})
	if code != http.StatusOK {
		t.Fatalf("post status = %d", code)
	}
	entries, _ := feedEntries(t, ts, "alice", "", 20)
	if len(entries) != 1 {
		t.Fatalf("alice own feed len = %d, want 1 (self-fan-out)", len(entries))
	}
	if entries[0]["author_id"] != "alice" {
		t.Errorf("own feed entry = %v", entries[0])
	}
}

// Mixed feed: normal + celebrity followees merge newest-first.
func TestFeedMixedOrdering(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 2)
	// celeb with 2 followers
	for _, id := range []string{"celeb", "f1", "f2", "reader"} {
		mustCreate(t, ts, id)
	}
	mustFollow(t, ts, "f1", "celeb")
	mustFollow(t, ts, "f2", "celeb")
	// reader follows celeb and normal "alice"
	mustCreate(t, ts, "alice")
	mustFollow(t, ts, "reader", "celeb")
	mustFollow(t, ts, "reader", "alice")
	// alice (normal) posts (fans out to reader), then celeb posts (lazy)
	_, _ = doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "normal post"})
	_, _ = doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "celeb", "content": "celebrity post"})
	entries, _ := feedEntries(t, ts, "reader", "", 20)
	if len(entries) != 2 {
		t.Fatalf("reader feed len = %d, want 2 (mixed)", len(entries))
	}
	// Both posts present.
	got := map[string]bool{}
	for _, e := range entries {
		got[e["content"].(string)] = true
	}
	if !got["normal post"] || !got["celebrity post"] {
		t.Errorf("reader feed missing posts: %v", got)
	}
}

func TestFeedUnknownUser404(t *testing.T) {
	_, ts := publicCoordinatorWith(t, 1000)
	code, _ := doJSON(t, "GET", ts.URL+"/feed/ghost", nil)
	if code != http.StatusNotFound {
		t.Fatalf("feed ghost status = %d, want 404 (unknown user)", code)
	}
}

// Pagination crosses a celebrity boundary with no drops/repeats.
func TestFeedPaginationWithCelebrity(t *testing.T) {
	coord, ts := publicCoordinatorWith(t, 2)
	for _, id := range []string{"celeb", "f1", "f2", "reader", "alice"} {
		mustCreate(t, ts, id)
	}
	mustFollow(t, ts, "f1", "celeb")
	mustFollow(t, ts, "f2", "celeb")
	mustFollow(t, ts, "reader", "celeb")
	mustFollow(t, ts, "reader", "alice")

	// Two posts each -> 4 feed entries; page size 2.
	for i := 0; i < 2; i++ {
		_, _ = doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "alice", "content": "a" + strconv.Itoa(i)})
		_, _ = doJSON(t, "POST", ts.URL+"/posts", map[string]any{"author_id": "celeb", "content": "c" + strconv.Itoa(i)})
	}

	page1, cur1 := feedEntries(t, ts, "reader", "", 2)
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	if cur1 == "" {
		t.Fatal("page1 has no cursor")
	}
	page2, cur2 := feedEntries(t, ts, "reader", cur1, 2)
	if len(page2) != 2 {
		t.Fatalf("page2 len = %d, want 2", len(page2))
	}
	// No repeats across pages.
	seen := map[string]bool{}
	for _, e := range append(page1, page2...) {
		key := e["post_id"].(string)
		if seen[key] {
			t.Fatalf("post %s repeated across pages", key)
		}
		seen[key] = true
	}
	if len(seen) != 4 {
		t.Fatalf("total unique posts = %d, want 4", len(seen))
	}
	// Third page is empty (all 4 shown).
	page3, cur3 := feedEntries(t, ts, "reader", cur2, 2)
	if len(page3) != 0 || cur3 != "" {
		t.Fatalf("page3 len=%d cur=%q, want empty", len(page3), cur3)
	}
	_ = coord
}

