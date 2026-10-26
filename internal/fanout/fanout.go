package fanout

import (
	"log"
	"sync"

	"socialfeed/internal/codec"
	"socialfeed/internal/keys"
	"socialfeed/internal/models"
)

// KV is a raw key/value pair destined for one shard.
type KV struct {
	Key   string
	Value []byte
}

// Router routes an entity ID to its owning shard addr.
type Router interface {
	ShardFor(entityID string) (nodeID, addr string, err error)
}

// Store performs shard operations on raw bytes (values already encoded).
type Store interface {
	Read(addr, key string) ([]byte, bool, error)
	Scan(addr, prefix string, limit int, reverse bool) ([]KV, error)
	Write(addr string, pairs []KV) error
}

// CreatePost implements the write path (origin flow F1):
//  1. store the post + author index on the author's shard (one batch txn),
//  2. if the author is a celebrity (count >= threshold), stop — lazy read delivery,
//  3. otherwise fan out a denormalized FeedEntry to every follower's shard,
//     one batched write per shard, failures logged and skipped (R7).
//
// The post is durably stored before any fan-out work, so a fan-out failure
// never loses the post (AE4). Returns the new post ID.
func CreatePost(r Router, s Store, c *codec.Codec, author, content string, threshold int64) (string, error) {
	ts := models.NowNS()
	postID := keys.NewID()
	post := models.Post{ID: postID, AuthorID: author, Content: content, Timestamp: ts}
	fe := post.ToFeedEntry()

	postBytes, err := c.Encode(&post)
	if err != nil {
		return "", err
	}
	feBytes, err := c.Encode(&fe)
	if err != nil {
		return "", err
	}

	// 1. Store post + author index on the author's shard (one txn).
	_, authorAddr, err := r.ShardFor(author)
	if err != nil {
		return "", err
	}
	if err := s.Write(authorAddr, []KV{
		{Key: keys.PostKey(postID), Value: postBytes},
		{Key: keys.AuthorIndexKey(author, ts, postID), Value: feBytes},
	}); err != nil {
		return "", err
	}

	// 2. Celebrity gate.
	count, err := readFollowerCount(s, c, authorAddr, author)
	if err != nil {
		// Count unreadable -> do not fan out (conservative); post still stored.
		log.Printf("post %s by %s: follower count unreadable, stored without push", postID, author)
		return postID, nil
	}
	if count >= threshold {
		// Celebrity: lazy delivery on read. The log line deliberately avoids
		// the substring "fan-out" so demos can prove the absence of fan-out by
		// grepping the coordinator log (scripts/demo/run.sh, Part B).
		log.Printf("post %s by %s: celebrity lazy path (count %d >= threshold %d), no push",
			postID, author, count, threshold)
		return postID, nil
	}

	// 3. Fan out to followers, grouped by destination shard.
	followers, err := s.Scan(authorAddr, keys.FollowersPrefix(author), 0, false)
	if err != nil {
		return postID, nil // followers unreadable -> post stored, no fan-out
	}
	byShard := map[string][]KV{}
	for _, f := range followers {
		_, followerID, ok := keys.FollowersEdgeParts(f.Key)
		if !ok {
			continue
		}
		_, faddr, err := r.ShardFor(followerID)
		if err != nil {
			continue
		}
		byShard[faddr] = append(byShard[faddr], KV{Key: keys.FeedKey(followerID, ts, postID), Value: feBytes})
	}

	// One batched write per shard, in parallel; failures logged and skipped (R7).
	var wg sync.WaitGroup
	pushed := 0
	for addr, pairs := range byShard {
		pushed += len(pairs)
		wg.Add(1)
		go func(addr string, pairs []KV) {
			defer wg.Done()
			if err := s.Write(addr, pairs); err != nil {
				// Log-and-skip: partial fan-out accepted by design (R7).
				log.Printf("fanout: write to %s skipped: %v", addr, err)
			}
		}(addr, pairs)
	}
	wg.Wait()
	log.Printf("post %s by %s: fan-out pushed %d feed entries", postID, author, pushed)
	return postID, nil
}

// readFollowerCount reads and decodes a user record to return its count.
func readFollowerCount(s Store, c *codec.Codec, addr, userID string) (int64, error) {
	raw, found, err := s.Read(addr, keys.UserKey(userID))
	if err != nil || !found {
		return 0, err
	}
	var u models.User
	if err := c.Decode(raw, &u); err != nil {
		return 0, err
	}
	return u.FollowerCount, nil
}
