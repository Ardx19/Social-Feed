package codec

import (
	"strings"
	"testing"

	"socialfeed/internal/models"
)

func newCodec(t *testing.T) *Codec {
	t.Helper()
	c, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestRoundTripAllTypes(t *testing.T) {
	c := newCodec(t)
	users := []models.User{{ID: "alice", Name: "Alice", FollowerCount: 5}}
	posts := []models.Post{{ID: "p1", AuthorID: "alice", Content: "hello", Timestamp: 123}}
	edges := []models.Edge{{FollowerID: "alice", FolloweeID: "bob"}}
	for _, v := range users {
		assertRoundTrip(t, c, &v)
	}
	for _, v := range posts {
		assertRoundTrip(t, c, &v)
	}
	for _, v := range edges {
		assertRoundTrip(t, c, &v)
	}
}

func assertRoundTrip(t *testing.T, c *Codec, in any) {
	t.Helper()
	data, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode(%T): %v", in, err)
	}
	if len(data) == 0 {
		t.Fatalf("Encode(%T) produced empty bytes", in)
	}
	out := newEmpty(in)
	if err := c.Decode(data, out); err != nil {
		t.Fatalf("Decode(%T): %v", in, err)
	}
	// Re-encode to compare equality.
	outData, _ := c.Encode(out)
	if string(data) != string(outData) {
		t.Errorf("roundtrip mismatch for %T:\n in=%v\nout=%v", in, in, out)
	}
}

func newEmpty(v any) any {
	switch v.(type) {
	case *models.User:
		return &models.User{}
	case *models.Post:
		return &models.Post{}
	case *models.FeedEntry:
		return &models.FeedEntry{}
	case *models.Edge:
		return &models.Edge{}
	}
	return nil
}

func TestSmallValueUsesRawFlag(t *testing.T) {
	c := newCodec(t)
	edge := models.Edge{FollowerID: "alice", FolloweeID: "bob"}
	data, err := c.Encode(&edge)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != flagRaw {
		t.Errorf("small edge flag = %d, want %d (raw)", data[0], flagRaw)
	}
}

func TestLargeValueUsesZstdFlag(t *testing.T) {
	c := newCodec(t)
	long := strings.Repeat("distributed systems interview prep ", 20) // ~560 bytes
	fe := models.FeedEntry{PostID: "p1", AuthorID: "alice", Content: long, Timestamp: 1}
	data, err := c.Encode(&fe)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != flagZstd {
		t.Errorf("large feed flag = %d, want %d (zstd)", data[0], flagZstd)
	}
	var out models.FeedEntry
	if err := c.Decode(data, &out); err != nil {
		t.Fatalf("decode zstd value: %v", err)
	}
	if out.Content != long {
		t.Error("zstd roundtrip corrupted content")
	}
}

func TestDecodeEmptyAndBadFlag(t *testing.T) {
	c := newCodec(t)
	var u models.User
	if err := c.Decode(nil, &u); err == nil {
		t.Error("expected error decoding empty value")
	}
	if err := c.Decode([]byte{0xFF, 1, 2}, &u); err == nil {
		t.Error("expected error decoding unknown flag byte")
	}
}
