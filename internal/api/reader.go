package api

import (
	"net/http"
	"strconv"
	"strings"
	"sync"

	"socialfeed/internal/fanout"
	"socialfeed/internal/keys"
)

// handleGetFeed implements GET /feed/{user_id}?limit=&before=.
//  1. 404 if the user does not exist.
//  2. reverse-scan the user's fan-out feed entries (denormalized content).
//  3. fetch the followee list; read each followee's FollowerCount; celebrities
//     (>= threshold) are served lazily.
//  4. reverse-scan each celebrity's author index.
//  5. merge both sources newest-first, dedupe, truncate to limit, cursor.
//
// The cursor is the logical (ts, post_id) tuple; each source reconstructs its
// own prefix's seek key from it (see design.md).
func (c *CoordinatorServer) handleGetFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user := strings.TrimPrefix(r.URL.Path, "/feed/")
	if !validateID(user) {
		writeError(w, http.StatusBadRequest, "invalid user ID")
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	var before *fanout.Cursor
	if b := q.Get("before"); b != "" {
		before = parseCursor(b)
	}

	if !c.userExists(user) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	// 1. Fan-out-path entries from the user's own shard.
	_, userAddr, err := c.ShardFor(user)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no shard")
		return
	}
	fanoutEntries, err := c.scanFeed(userAddr, keys.FeedPrefix(user), user, before, limit)
	if err != nil {
		// User's own shard failed -> 502 (replica retry handled in U10).
		writeError(w, http.StatusBadGateway, "feed unavailable")
		return
	}

	// 2. Followee list (who the user follows), from the user's shard.
	followees, err := c.client.Scan(userAddr, keys.FollowingPrefix(user), 0, false)
	if err != nil {
		writeError(w, http.StatusBadGateway, "feed unavailable")
		return
	}
	followeeIDs := make([]string, 0, len(followees))
	for _, e := range followees {
		_, id, ok := keys.FollowingEdgeParts(e.Key)
		// Keep the self-edge: a celebrity reads their own posts via this path
		// (self is a followee and qualifies as a celebrity). A normal user's
		// own posts arrive via fan-out, and self won't be a celebrity then, so
		// including it is harmless.
		if ok {
			followeeIDs = append(followeeIDs, id)
		}
	}

	// 3. Determine celebrities (FollowerCount >= threshold) and fetch their
	//    recent posts via the author index, in parallel.
	var celebrityEntries []fanout.FeedEntry
	if len(followeeIDs) > 0 {
		celebrityEntries = c.fetchCelebrityPosts(followeeIDs, before, limit)
	}

	// 4. Merge, dedupe, truncate.
	page, next := fanout.Merge(fanoutEntries, celebrityEntries, limit, before)
	out := make([]map[string]any, 0, len(page))
	for _, e := range page {
		out = append(out, map[string]any{
			"post_id":   e.PostID,
			"author_id": e.AuthorID,
			"content":   e.Content,
			"timestamp": e.Timestamp,
		})
	}
	resp := map[string]any{"entries": out}
	if next != nil {
		resp["next_cursor"] = cursorString(*next)
	}
	writeOK(w, resp)
}

// scanFeed reverse-scans a feed/author prefix from the logical cursor,
// reconstructing the seek key for this specific prefix.
func (c *CoordinatorServer) scanFeed(addr, prefix, entityID string, before *fanout.Cursor, limit int) ([]fanout.FeedEntry, error) {
	var from string
	if before != nil {
		// Reconstruct this prefix's seek key from the logical (ts, pid) cursor.
		if strings.HasPrefix(prefix, "feed:") {
			from = keys.FeedKey(entityID, before.Timestamp, before.PostID)
		} else {
			from = keys.AuthorIndexKey(entityID, before.Timestamp, before.PostID)
		}
	}
	pairs, err := c.client.ScanFrom(addr, prefix, from, limit)
	if err != nil {
		return nil, err
	}
	out := make([]fanout.FeedEntry, 0, len(pairs))
	for _, p := range pairs {
		var fe fanout.FeedEntry
		if err := c.decode(p.Value, &fe); err != nil {
			continue
		}
		out = append(out, fe)
	}
	return out, nil
}

// fetchCelebrityPosts scans each celebrity's author index in parallel and
// returns their recent posts. A failing celebrity shard is skipped (the rest
// of the feed still merges).
func (c *CoordinatorServer) fetchCelebrityPosts(followeeIDs []string, before *fanout.Cursor, limit int) []fanout.FeedEntry {
	type result struct {
		entries []fanout.FeedEntry
	}
	results := make([]result, len(followeeIDs))
	var wg sync.WaitGroup
	for i, id := range followeeIDs {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			if !c.isCelebrity(id) {
				return
			}
			_, addr, err := c.ShardFor(id)
			if err != nil {
				return
			}
			entries, err := c.scanFeed(addr, keys.AuthorIndexPrefix(id), id, before, limit)
			if err != nil {
				return // skip this celebrity
			}
			results[i].entries = entries
		}(i, id)
	}
	wg.Wait()

	var all []fanout.FeedEntry
	for _, res := range results {
		all = append(all, res.entries...)
	}
	return all
}

// isCelebrity reports whether a user's FollowerCount meets the threshold.
func (c *CoordinatorServer) isCelebrity(id string) bool {
	_, addr, err := c.ShardFor(id)
	if err != nil {
		return false
	}
	raw, found, err := c.client.Read(addr, keys.UserKey(id))
	if err != nil || !found {
		return false
	}
	var u userForCount
	if err := c.decode(raw, &u); err != nil {
		return false
	}
	return u.FollowerCount >= c.threshold
}

func parseCursor(s string) *fanout.Cursor {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil
	}
	return &fanout.Cursor{Timestamp: ts, PostID: parts[1]}
}

func cursorString(c fanout.Cursor) string {
	return strconv.FormatInt(c.Timestamp, 10) + ":" + c.PostID
}

// userForCount is a minimal view of the user record for count reads.
type userForCount struct {
	FollowerCount int64 `msgpack:"follower_count"`
}
