package keys

import (
	"strings"
	"testing"
)

func TestPadTimestampWidth(t *testing.T) {
	p := PadTimestamp(1718123456)
	if len(p) != 19 {
		t.Fatalf("PadTimestamp width = %d, want 19", len(p))
	}
	// 1718123456 is 10 digits -> 9 leading zeros, matching plan.txt's example.
	if p != "0000000001718123456" {
		t.Fatalf("expected 19-char zero-padded timestamp, got %q", p)
	}
}

func TestTimestampsSortChronologically(t *testing.T) {
	early := PadTimestamp(100)
	late := PadTimestamp(200)
	if !(early < late) {
		t.Fatalf("expected %q < %q", early, late)
	}
}

func TestNewIDFixedWidth(t *testing.T) {
	a := NewID()
	b := NewID()
	if len(a) != 2*postIDBytes {
		t.Fatalf("NewID len = %d, want %d", len(a), 2*postIDBytes)
	}
	if a == b {
		t.Fatal("two NewID calls returned the same id")
	}
}

func TestKeyShapes(t *testing.T) {
	ts := int64(1718123456)
	pid := "abcdef1234567890abcdef"

	cases := []struct {
		got  string
		want string
	}{
		{UserKey("u1"), "users:u1"},
		{PostKey(pid), "posts:" + pid},
		{AuthorIndexKey("alice", ts, pid), "posts_by_author:alice:0000000001718123456:" + pid},
		{AuthorIndexPrefix("alice"), "posts_by_author:alice:"},
		{FeedKey("u1", ts, pid), "feed:u1:0000000001718123456:" + pid},
		{FeedPrefix("u1"), "feed:u1:"},
		{FollowersEdgeKey("bob", "alice"), "graph:followers:bob:alice"},
		{FollowersPrefix("bob"), "graph:followers:bob:"},
		{FollowingEdgeKey("alice", "bob"), "graph:following:alice:bob"},
		{FollowingPrefix("alice"), "graph:following:alice:"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

func TestFeedKeyPrefixRoundTrip(t *testing.T) {
	// A feed key must fall under its own prefix so prefix scans find it.
	k := FeedKey("u1", 100, "pid1")
	if !strings.HasPrefix(k, FeedPrefix("u1")) {
		t.Errorf("feed key %q not under prefix %q", k, FeedPrefix("u1"))
	}
	// And a key for user "u1" must NOT collide with user "u1x".
	if strings.HasPrefix(FeedKey("u1x", 100, "pid1"), FeedPrefix("u1")) {
		t.Errorf("user u1x key collides with u1 prefix")
	}
}
