package models

import "testing"

func TestPostToFeedEntry(t *testing.T) {
	p := Post{ID: "p1", AuthorID: "alice", Content: "hi", Timestamp: 42}
	fe := p.ToFeedEntry()
	if fe.PostID != p.ID || fe.AuthorID != p.AuthorID || fe.Content != p.Content || fe.Timestamp != p.Timestamp {
		t.Errorf("FeedEntry mismatch: %+v", fe)
	}
}

func TestNewPost(t *testing.T) {
	p := NewPost("p1", "alice", "hi", 42)
	if p.ID != "p1" || p.AuthorID != "alice" || p.Timestamp != 42 {
		t.Errorf("NewPost mismatch: %+v", p)
	}
}
