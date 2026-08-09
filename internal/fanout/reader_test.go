package fanout

import "testing"

func fe(postID, author string, ts int64) FeedEntry {
	return FeedEntry{PostID: postID, AuthorID: author, Content: "c", Timestamp: ts}
}

func TestMergeEmpty(t *testing.T) {
	page, next := Merge(nil, nil, 20, nil)
	if len(page) != 0 || next != nil {
		t.Fatalf("empty merge: page=%v next=%v", page, next)
	}
}

func TestMergeSortsNewestFirst(t *testing.T) {
	// Fan-out path has older posts; celebrity path has newer ones.
	fan := []FeedEntry{fe("p1", "bob", 100), fe("p2", "bob", 300)}
	celeb := []FeedEntry{fe("p3", "celeb", 500), fe("p4", "celeb", 200)}
	page, _ := Merge(fan, celeb, 20, nil)
	if len(page) != 4 {
		t.Fatalf("page len = %d, want 4", len(page))
	}
	// Newest first: p3(500), p2(300), p4(200), p1(100)
	want := []string{"p3", "p2", "p4", "p1"}
	for i, pid := range want {
		if page[i].PostID != pid {
			t.Errorf("page[%d] = %s, want %s", i, page[i].PostID, pid)
		}
	}
}

func TestMergeDedupesByPostID(t *testing.T) {
	// Same post appears via both the fan-out path and the celebrity path
	// (threshold crossing). It must appear once.
	fan := []FeedEntry{fe("dup", "alice", 100)}
	celeb := []FeedEntry{fe("dup", "alice", 100)}
	page, _ := Merge(fan, celeb, 20, nil)
	if len(page) != 1 {
		t.Fatalf("dedupe: page len = %d, want 1", len(page))
	}
	if page[0].PostID != "dup" {
		t.Errorf("dedupe kept %s", page[0].PostID)
	}
}

func TestMergeTruncatesToLimit(t *testing.T) {
	fan := []FeedEntry{fe("p1", "a", 1), fe("p2", "a", 2), fe("p3", "a", 3)}
	page, next := Merge(fan, nil, 2, nil)
	if len(page) != 2 {
		t.Fatalf("truncate: len = %d, want 2", len(page))
	}
	if next == nil || next.PostID != "p2" || next.Timestamp != 2 {
		t.Errorf("next cursor = %+v, want p2/2", next)
	}
}

func TestMergePaginationAcrossSources(t *testing.T) {
	// Two sources with interleaved timestamps; page size 2. The second page
	// (before = first page's cursor) must not repeat and must return the rest.
	fan := []FeedEntry{fe("f1", "bob", 1), fe("f2", "bob", 3), fe("f3", "bob", 5)}
	celeb := []FeedEntry{fe("c1", "celeb", 2), fe("c2", "celeb", 4), fe("c3", "celeb", 6)}

	page1, cur1 := Merge(fan, celeb, 2, nil)
	// Newest two: c3(6), f3(5).
	if page1[0].PostID != "c3" || page1[1].PostID != "f3" {
		t.Fatalf("page1 = %s,%s", page1[0].PostID, page1[1].PostID)
	}
	if cur1 == nil {
		t.Fatal("no cursor on page1")
	}

	page2, _ := Merge(fan, celeb, 2, cur1)
	// Next two (older): c2(4), f2(3).
	if len(page2) != 2 {
		t.Fatalf("page2 len = %d, want 2", len(page2))
	}
	if page2[0].PostID != "c2" || page2[1].PostID != "f2" {
		t.Fatalf("page2 = %s,%s", page2[0].PostID, page2[1].PostID)
	}

	page3, cur3 := Merge(fan, celeb, 2, cur1)
	_ = cur3
	// Sanity: same before -> same page (deterministic).
	if page3[0].PostID != page2[0].PostID {
		t.Error("pagination not deterministic with same cursor")
	}
}

func TestMergeBeforeFiltersOlder(t *testing.T) {
	fan := []FeedEntry{fe("p1", "a", 1), fe("p2", "a", 2), fe("p3", "a", 3)}
	// before = p2/2 means "older than p2"; p3(3) and p2(2) are excluded.
	page, _ := Merge(fan, nil, 20, &Cursor{Timestamp: 2, PostID: "p2"})
	if len(page) != 1 || page[0].PostID != "p1" {
		t.Fatalf("before-filter: %v", page)
	}
}
