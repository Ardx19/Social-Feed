package models

import "time"

// User is a social account. FollowerCount is the number of real follower
// edges (excluding the self-edge) — it is the celebrity gate, so it must
// always equal exactly that.
type User struct {
	ID            string `msgpack:"id"`
	Name          string `msgpack:"name"`
	FollowerCount int64  `msgpack:"follower_count"`
}

// Post is the source of truth for a post. It is stored on the author's shard.
type Post struct {
	ID        string `msgpack:"id"`
	AuthorID  string `msgpack:"author_id"`
	Content   string `msgpack:"content"`
	Timestamp int64  `msgpack:"timestamp"` // ns since epoch, coordinator-stamped
}

// FeedEntry is the denormalized per-follower copy of a post. Content is
// duplicated into every follower's feed so a single range scan renders it.
type FeedEntry struct {
	PostID    string `msgpack:"post_id"`
	AuthorID  string `msgpack:"author_id"`
	Content   string `msgpack:"content"`
	Timestamp int64  `msgpack:"timestamp"`
}

// Edge is a follow relationship. The same struct stores both directions;
// the key prefix (graph:followers / graph:following) carries the direction.
type Edge struct {
	FollowerID string `msgpack:"follower_id"`
	FolloweeID string `msgpack:"followee_id"`
}

// NewPost builds a Post with a nanosecond timestamp from the coordinator,
// which is the single source of truth for ordering.
func NewPost(id, authorID, content string, ts int64) Post {
	return Post{ID: id, AuthorID: authorID, Content: content, Timestamp: ts}
}

// ToFeedEntry denormalizes a post for a follower's feed.
func (p Post) ToFeedEntry() FeedEntry {
	return FeedEntry{PostID: p.ID, AuthorID: p.AuthorID, Content: p.Content, Timestamp: p.Timestamp}
}

// NowNS returns the current time in nanoseconds, the unit used for key padding.
func NowNS() int64 { return time.Now().UnixNano() }
