package keys

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// The keys package is the ONLY place key shapes are assembled and parsed.
// Handlers and the coordinator client use these helpers; raw key construction
// never appears outside this package. See design.md for the rationale.
//
// Timestamps are nanoseconds zero-padded to a fixed 19 chars so that
// lexicographic order == chronological order.
// Post IDs are fixed-width hex so that within a timestamp slot they also sort
// lexicographically. Both are the same invariant, stated once.

const (
	tsPadWidth = 19
	postIDBytes = 12 // 24 hex chars
)

// NewID returns a fixed-width random hex string for a post ID.
func NewID() string {
	b := make([]byte, postIDBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// PadTimestamp zero-pads a nanosecond timestamp to tsPadWidth chars.
func PadTimestamp(ts int64) string {
	return fmt.Sprintf("%0*d", tsPadWidth, ts)
}

// UserKey is the users:{id} record.
func UserKey(id string) string { return "users:" + id }

// PostKey is the posts:{id} record.
func PostKey(id string) string { return "posts:" + id }

// AuthorIndexKey is posts_by_author:{author}:{ts19}:{post_id}.
func AuthorIndexKey(author string, ts int64, postID string) string {
	return fmt.Sprintf("posts_by_author:%s:%s:%s", author, PadTimestamp(ts), postID)
}

// AuthorIndexPrefix is posts_by_author:{author}: — prefix scan for recent posts.
func AuthorIndexPrefix(author string) string {
	return "posts_by_author:" + author + ":"
}

// FeedKey is feed:{user}:{ts19}:{post_id}.
func FeedKey(user string, ts int64, postID string) string {
	return fmt.Sprintf("feed:%s:%s:%s", user, PadTimestamp(ts), postID)
}

// FeedPrefix is feed:{user}: — prefix scan for the user's feed.
func FeedPrefix(user string) string { return "feed:" + user + ":" }

// FollowersEdgeKey is graph:followers:{user}:{follower}.
func FollowersEdgeKey(user, follower string) string {
	return "graph:followers:" + user + ":" + follower
}

// FollowersPrefix is graph:followers:{user}: — "who follows user".
func FollowersPrefix(user string) string { return "graph:followers:" + user + ":" }

// FollowingEdgeKey is graph:following:{user}:{followee}.
func FollowingEdgeKey(user, followee string) string {
	return "graph:following:" + user + ":" + followee
}

// FollowingPrefix is graph:following:{user}: — "who user follows".
func FollowingPrefix(user string) string { return "graph:following:" + user + ":" }

// FollowersEdgeParts parses "graph:followers:{user}:{follower}". Because IDs
// are validated to contain no ':', the follower is always the last segment.
func FollowersEdgeParts(key string) (user, follower string, ok bool) {
	return parseEdge(key, "graph:followers:")
}

// FollowingEdgeParts parses "graph:following:{user}:{followee}".
func FollowingEdgeParts(key string) (user, followee string, ok bool) {
	return parseEdge(key, "graph:following:")
}

func parseEdge(key, prefix string) (string, string, bool) {
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	idx := strings.LastIndex(rest, ":")
	if idx <= 0 || idx == len(rest)-1 {
		return "", "", false
	}
	return rest[:idx], rest[idx+1:], true
}
