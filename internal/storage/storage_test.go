package storage

import (
	"fmt"
	"sync"
	"testing"

	"socialfeed/internal/codec"
	"socialfeed/internal/keys"
	"socialfeed/internal/models"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	c, err := codec.New()
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSetGetDeleteRoundtrip(t *testing.T) {
	s := newStore(t)
	k := []byte("k")
	v := []byte("v1")

	if _, found, _ := s.Get(k); found {
		t.Fatal("expected key absent before Set")
	}
	if err := s.Set(k, v); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Get(k)
	if err != nil || !found || string(got) != string(v) {
		t.Fatalf("Get after Set = %q,%v,%v", got, found, err)
	}
	if err := s.Delete(k); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.Get(k); found {
		t.Fatal("expected key gone after Delete")
	}
	// Delete of absent key is a no-op.
	if err := s.Delete(k); err != nil {
		t.Fatalf("delete absent key: %v", err)
	}
}

func TestScanOrderAndLimit(t *testing.T) {
	s := newStore(t)
	// feed:u1:{ts19}:{pid} keys in ascending ts
	ks := [][]byte{
		[]byte(keys.FeedKey("u1", 100, "a")),
		[]byte(keys.FeedKey("u1", 200, "b")),
		[]byte(keys.FeedKey("u1", 300, "c")),
		[]byte(keys.FeedKey("u2", 100, "x")), // different prefix
	}
	for i, k := range ks {
		if err := s.Set(k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	prefix := []byte(keys.FeedPrefix("u1"))
	asc, err := s.Scan(prefix, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(asc) != 3 {
		t.Fatalf("asc scan len = %d, want 3 (u2 excluded)", len(asc))
	}
	desc, err := s.Scan(prefix, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	// Reverse should put the highest ts first.
	if string(desc[0]) != "v2" {
		t.Errorf("reverse first = %q, want v2 (newest)", desc[0])
	}
	limited, _ := s.Scan(prefix, 2, true)
	if len(limited) != 2 {
		t.Errorf("limit scan len = %d, want 2", len(limited))
	}
}

func TestScanKeys(t *testing.T) {
	s := newStore(t)
	if err := s.Set([]byte(keys.FeedKey("u1", 100, "a")), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte(keys.FeedKey("u1", 200, "b")), []byte("v")); err != nil {
		t.Fatal(err)
	}
	ks, err := s.ScanKeys([]byte(keys.FeedPrefix("u1")), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 2 {
		t.Fatalf("ScanKeys len = %d, want 2", len(ks))
	}
	// Reverse: newest (ts 200) first.
	if string(ks[0]) != string([]byte(keys.FeedKey("u1", 200, "b"))) {
		t.Errorf("ScanKeys reverse first = %q", ks[0])
	}
}

func TestBatchAtomic(t *testing.T) {
	s := newStore(t)
	pairs := []KV{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}
	if err := s.Batch(pairs); err != nil {
		t.Fatal(err)
	}
	for _, p := range pairs {
		if _, found, _ := s.Get(p.Key); !found {
			t.Errorf("batch did not persist %q", p.Key)
		}
	}
}

func TestCreateUserWritesSelfEdges(t *testing.T) {
	s := newStore(t)
	u := models.User{ID: "alice", Name: "Alice"}
	if err := s.CreateUser(u); err != nil {
		t.Fatal(err)
	}
	raw, found, _ := s.Get([]byte(keys.UserKey("alice")))
	if !found {
		t.Fatal("user record not written")
	}
	var got models.User
	if err := s.decode(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.FollowerCount != 0 {
		t.Errorf("new user FollowerCount = %d, want 0 (self-edge not counted)", got.FollowerCount)
	}
	// Both self-edges exist.
	fe, _ := s.ScanKeys([]byte(keys.FollowersPrefix("alice")), 0, false)
	if len(fe) != 1 {
		t.Errorf("followers self-edge count = %d, want 1", len(fe))
	}
	fg, _ := s.ScanKeys([]byte(keys.FollowingPrefix("alice")), 0, false)
	if len(fg) != 1 {
		t.Errorf("following self-edge count = %d, want 1", len(fg))
	}
}

func (s *Store) decode(data []byte, v any) error { return s.codec.Decode(data, v) }

func TestAddFollowerFreshAndIdempotent(t *testing.T) {
	s := newStore(t)
	_ = s.CreateUser(models.User{ID: "bob", Name: "Bob"})

	if err := s.AddFollower("bob", "alice"); err != nil {
		t.Fatalf("AddFollower fresh: %v", err)
	}
	count := followerCount(t, s, "bob")
	if count != 1 {
		t.Fatalf("count after fresh follow = %d, want 1", count)
	}
	// Duplicate follow is a no-op.
	if err := s.AddFollower("bob", "alice"); err != nil {
		t.Fatalf("AddFollower duplicate: %v", err)
	}
	if count := followerCount(t, s, "bob"); count != 1 {
		t.Fatalf("count after duplicate follow = %d, want 1 (idempotent)", count)
	}
	// Edge exists exactly once (plus the self-edge from CreateUser).
	fe, _ := s.ScanKeys([]byte(keys.FollowersPrefix("bob")), 0, false)
	if len(fe) != 2 {
		t.Errorf("followers edge count = %d, want 2 (alice + self)", len(fe))
	}
}

func TestRemoveFollowerConditional(t *testing.T) {
	s := newStore(t)
	_ = s.CreateUser(models.User{ID: "bob", Name: "Bob"})

	// Unfollow without an edge is a no-op (no decrement).
	if err := s.RemoveFollower("bob", "alice"); err != nil {
		t.Fatalf("RemoveFollower absent: %v", err)
	}
	if count := followerCount(t, s, "bob"); count != 0 {
		t.Fatalf("count after absent unfollow = %d, want 0", count)
	}
	// Follow then unfollow returns to baseline.
	_ = s.AddFollower("bob", "alice")
	if err := s.RemoveFollower("bob", "alice"); err != nil {
		t.Fatalf("RemoveFollower present: %v", err)
	}
	if count := followerCount(t, s, "bob"); count != 0 {
		t.Fatalf("count after unfollow = %d, want 0", count)
	}
	// Only the self-edge remains after unfollow.
	fe, _ := s.ScanKeys([]byte(keys.FollowersPrefix("bob")), 0, false)
	if len(fe) != 1 {
		t.Errorf("followers edge count = %d, want 1 (self only)", len(fe))
	}
}

func TestAddFollowerNonExistentFollowee(t *testing.T) {
	s := newStore(t)
	// "carol" has no user record.
	err := s.AddFollower("carol", "alice")
	if err != ErrNotFound {
		t.Fatalf("AddFollower to missing followee err = %v, want ErrNotFound", err)
	}
	fe, _ := s.ScanKeys([]byte(keys.FollowersPrefix("carol")), 0, false)
	if len(fe) != 0 {
		t.Errorf("no edge should be written for missing followee, got %d", len(fe))
	}
}

func TestConcurrentFollowsConverge(t *testing.T) {
	s := newStore(t)
	_ = s.CreateUser(models.User{ID: "celeb", Name: "Celeb"})

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.AddFollower("celeb", fmt.Sprintf("user%d", i))
		}(i)
	}
	wg.Wait()

	if count := followerCount(t, s, "celeb"); count != n {
		t.Fatalf("concurrent follows count = %d, want %d (conflict retry must converge)", count, n)
	}
}

// TestAddFollowerReplicatesAsOneUnit verifies the replication unit-of-work
// contract: a follow-edge transaction emits ONE write-set carrying BOTH the
// follower edge and the incremented count, so a replica can never observe the
// pair split (see design.md).
func TestAddFollowerReplicatesAsOneUnit(t *testing.T) {
	s := newStore(t)
	_ = s.CreateUser(models.User{ID: "bob", Name: "Bob"})

	var got WriteSet
	s.SetCommitHook(func(ws WriteSet) { got = ws })
	if err := s.AddFollower("bob", "alice"); err != nil {
		t.Fatal(err)
	}

	// Exactly one write-set, with both the edge and the user record.
	if len(got.Pairs) != 2 {
		t.Fatalf("write-set pairs = %d, want 2 (edge + count in one op)", len(got.Pairs))
	}
	keys_ := map[string]bool{}
	for _, p := range got.Pairs {
		keys_[string(p.Key)] = true
	}
	if !keys_[keys.FollowersEdgeKey("bob", "alice")] || !keys_[keys.UserKey("bob")] {
		t.Errorf("write-set missing edge or count: %v", keys_)
	}
}

// TestRemoveFollowerReplicatesEdgeAndDecrement verifies unfollow emits the user
// record (decremented) plus a delete for the edge in one write-set.
func TestRemoveFollowerReplicatesEdgeAndDecrement(t *testing.T) {
	s := newStore(t)
	_ = s.CreateUser(models.User{ID: "bob", Name: "Bob"})
	_ = s.AddFollower("bob", "alice")

	var got WriteSet
	s.SetCommitHook(func(ws WriteSet) { got = ws })
	if err := s.RemoveFollower("bob", "alice"); err != nil {
		t.Fatal(err)
	}
	if len(got.Pairs) != 1 || len(got.Deletes) != 1 {
		t.Fatalf("unfollow write-set: pairs=%d deletes=%d, want 1 each", len(got.Pairs), len(got.Deletes))
	}
	if string(got.Deletes[0]) != keys.FollowersEdgeKey("bob", "alice") {
		t.Errorf("unfollow delete = %q", got.Deletes[0])
	}
}

// TestApplyWriteSetAppliesAtomically verifies a replica applies pairs + deletes
// in one transaction.
func TestApplyWriteSetAppliesAtomically(t *testing.T) {
	s := newStore(t)
	if err := s.ApplyWriteSet(WriteSet{
		Pairs:   []KV{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte("2")}},
		Deletes: [][]byte{[]byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b"} {
		if _, found, _ := s.Get([]byte(k)); !found {
			t.Errorf("applied pair %q missing", k)
		}
	}
	if _, found, _ := s.Get([]byte("c")); found {
		t.Error("delete was not applied")
	}
}

func followerCount(t *testing.T, s *Store, id string) int64 {
	t.Helper()
	raw, found, err := s.Get([]byte(keys.UserKey(id)))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		return -1
	}
	var u models.User
	if err := s.codec.Decode(raw, &u); err != nil {
		t.Fatal(err)
	}
	return u.FollowerCount
}
