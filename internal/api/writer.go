package api

import (
	"encoding/base64"
	"net/http"
	"strings"
	"sync"

	"socialfeed/internal/fanout"
	"socialfeed/internal/keys"
	"socialfeed/internal/models"
)

// fanoutStore adapts CoordinatorServer to fanout.Store.
// Read matches directly; Scan/Write convert between fanout.KV and the wire form.
func (c *CoordinatorServer) Read(addr, key string) ([]byte, bool, error) {
	return c.client.Read(addr, key)
}

func (c *CoordinatorServer) Scan(addr, prefix string, limit int, reverse bool) ([]fanout.KV, error) {
	pairs, err := c.client.Scan(addr, prefix, limit, reverse)
	if err != nil {
		return nil, err
	}
	out := make([]fanout.KV, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, fanout.KV{Key: p.Key, Value: p.Value})
	}
	return out, nil
}

func (c *CoordinatorServer) Write(addr string, pairs []fanout.KV) error {
	wire := make([]byteKV, 0, len(pairs))
	for _, p := range pairs {
		wire = append(wire, byteKV{Key: p.Key, Value: b64(p.Value)})
	}
	return c.client.Write(addr, wire)
}

// postAuthors maps post ID -> author ID so GET /posts/{id} can route to the
// author's shard. It is in-memory (lost on coordinator restart; lookups for
// pre-restart posts 404 loudly — documented in design.md).
var postAuthors = struct {
	sync.RWMutex
	m map[string]string
}{m: map[string]string{}}

type createPostBody struct {
	AuthorID string `json:"author_id"`
	Content  string `json:"content"`
}

// handleCreatePost implements POST /posts.
func (c *CoordinatorServer) handleCreatePost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req createPostBody
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	if !validateID(req.AuthorID) {
		writeError(w, http.StatusBadRequest, "invalid author ID")
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		writeError(w, http.StatusBadRequest, "content required")
		return
	}
	if !c.userExists(req.AuthorID) {
		writeError(w, http.StatusNotFound, "author not found")
		return
	}

	postID, err := fanout.CreatePost(c, c, c.codec, req.AuthorID, req.Content, c.threshold)
	if err != nil {
		// Author's shard unreachable or write failed -> 502, never queued
		// (post fan-out cannot be materialized offline; see design.md).
		writeError(w, http.StatusBadGateway, "post creation failed")
		return
	}
	// Record the post->author mapping for GET /posts/{id} routing.
	postAuthors.Lock()
	postAuthors.m[postID] = req.AuthorID
	postAuthors.Unlock()

	writeOK(w, map[string]any{"post_id": postID})
}

// handleGetPost implements GET /posts/{id}. Routes via the author (from the
// in-memory map) so the random post ID finds its shard.
func (c *CoordinatorServer) handleGetPost(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/posts/")
	postAuthors.RLock()
	author, ok := postAuthors.m[id]
	postAuthors.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	_, addr, err := c.ShardFor(author)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no shard")
		return
	}
	raw, found, rerr := c.client.Read(addr, keys.PostKey(id))
	if rerr != nil {
		writeError(w, http.StatusServiceUnavailable, "read failed")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	var p models.Post
	if derr := c.decode(raw, &p); derr != nil {
		writeError(w, http.StatusInternalServerError, "decode failed")
		return
	}
	writeOK(w, map[string]any{"id": p.ID, "author_id": p.AuthorID, "content": p.Content, "timestamp": p.Timestamp})
}

// b64 encodes bytes for the JSON wire.
func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
