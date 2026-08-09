package fanout

import (
	"sort"

	"socialfeed/internal/models"
)

// FeedEntry is the read-path normalized entry shape (PostID/AuthorID/Content/
// Timestamp), shared by the fan-out and celebrity paths.
type FeedEntry = models.FeedEntry

// Cursor is the logical pagination position (timestamp, post_id). It is NOT a
// physical BadgerDB key: a merged page spans feed:{user}:* and each celebrity's
// posts_by_author:* on possibly different shards, so each source reconstructs
// its own prefix's seek key from this tuple (see design.md).
type Cursor struct {
	Timestamp int64
	PostID    string
}

// Merge combines fan-out-path and celebrity-path feed entries, sorts them
// newest-first by (timestamp desc, post_id desc), dedupes by post_id, filters
// out entries already shown (>= before cursor), and truncates to limit.
// It returns the page and the next cursor (the last returned entry), or nil
// when there are no more entries.
//
// The sources are expected to be already newest-first and bounded; the caller
// passes a window from each source. Exactness of the window is the caller's
// concern (see the coordinator read path).
func Merge(fanoutEntries, celebrityEntries []FeedEntry, limit int, before *Cursor) ([]FeedEntry, *Cursor) {
	if limit <= 0 {
		return []FeedEntry{}, nil
	}
	all := make([]FeedEntry, 0, len(fanoutEntries)+len(celebrityEntries))
	all = append(all, fanoutEntries...)
	all = append(all, celebrityEntries...)

	sort.Slice(all, func(i, j int) bool {
		if all[i].Timestamp != all[j].Timestamp {
			return all[i].Timestamp > all[j].Timestamp
		}
		return all[i].PostID > all[j].PostID
	})

	seen := map[string]bool{}
	var page []FeedEntry
	for _, e := range all {
		// Skip entries at/after the cursor (already shown on a prior page).
		if before != nil && !beforeOlder(e, *before) {
			continue
		}
		if seen[e.PostID] {
			continue
		}
		seen[e.PostID] = true
		page = append(page, e)
		if len(page) >= limit {
			break
		}
	}
	if len(page) == 0 {
		return []FeedEntry{}, nil
	}
	last := page[len(page)-1]
	return page, &Cursor{Timestamp: last.Timestamp, PostID: last.PostID}
}

// beforeOlder reports whether entry e is strictly older than (before) cursor c
// under the newest-first ordering.
func beforeOlder(e FeedEntry, c Cursor) bool {
	if e.Timestamp != c.Timestamp {
		return e.Timestamp < c.Timestamp
	}
	return e.PostID < c.PostID
}
