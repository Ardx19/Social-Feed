package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"socialfeed/internal/models"
	"socialfeed/internal/storage"
)

// ShardServer serves a shard node's internal HTTP API. Handlers are thin
// wrappers over the storage layer — no key assembly and no codec work happen
// here (see design.md).
type ShardServer struct {
	store *storage.Store
	id    string
	role  string
}

// NewShard builds a shard HTTP handler for a store.
func NewShard(store *storage.Store, id, role string) *ShardServer {
	return &ShardServer{store: store, id: id, role: role}
}

// Handler returns the shard's HTTP routes.
func (s *ShardServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/write", s.handleWrite)
	mux.HandleFunc("/read", s.handleRead)
	mux.HandleFunc("/scan", s.handleScan)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/create-user", s.handleCreateUser)
	mux.HandleFunc("/follow-edge", s.handleFollowEdge)
	mux.HandleFunc("/unfollow-edge", s.handleUnfollowEdge)
	mux.HandleFunc("/replicate", s.handleReplicate)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

// ---- request/response wire types ----

type keyValue struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // base64-encoded value
}

type writeRequest struct {
	Key   string     `json:"key,omitempty"`
	Value string     `json:"value,omitempty"`
	Pairs []keyValue `json:"pairs,omitempty"`
}

type scanResponse struct {
	Entries []keyValue `json:"entries"`
}

type edgeRequest struct {
	Followee string `json:"followee"`
	Follower string `json:"follower"`
}

type createUserRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type healthResponse struct {
	NodeID string `json:"node_id"`
	Role   string `json:"role"`
	Status string `json:"status"`
}

// ---- handlers ----

func (s *ShardServer) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req writeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	// Build the pair list from either the single-key form or the batch form.
	raw := req.Pairs
	if req.Key != "" {
		raw = append(raw, keyValue{Key: req.Key, Value: req.Value})
	}
	if len(raw) == 0 {
		writeError(w, http.StatusBadRequest, "no pairs to write")
		return
	}
	kvs := make([]storage.KV, 0, len(raw))
	for _, p := range raw {
		val, err := base64.StdEncoding.DecodeString(p.Value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "malformed value: "+err.Error())
			return
		}
		kvs = append(kvs, storage.KV{Key: []byte(p.Key), Value: val})
	}
	if err := s.store.Batch(kvs); err != nil {
		writeError(w, http.StatusInternalServerError, "write failed")
		return
	}
	writeOK(w, map[string]any{"written": len(kvs)})
}

func (s *ShardServer) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}
	val, found, err := s.store.Get([]byte(key))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read failed")
		return
	}
	if !found {
		writeOK(w, map[string]any{"value": nil}) // the "missing" case — not an error
		return
	}
	writeOK(w, map[string]any{"value": base64.StdEncoding.EncodeToString(val)})
}

func (s *ShardServer) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	prefix := q.Get("prefix")
	reverse, _ := strconv.ParseBool(q.Get("reverse"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	from := q.Get("from") // logical-cursor seek key (newest-first from this position)

	// A `from` key requests a reverse scan starting at that cursor position
	// (used by the read path's pagination). Otherwise use a plain scan.
	var kvs []storage.KV
	var err error
	if from != "" {
		kvs, err = s.store.ScanCursor([]byte(prefix), []byte(from), limit)
	} else if reverse {
		pairs, serr := s.store.ScanKeys([]byte(prefix), 0, true)
		if serr != nil {
			writeError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		for _, k := range pairs {
			val, _, _ := s.store.Get(k)
			kvs = append(kvs, storage.KV{Key: k, Value: val})
			if limit > 0 && len(kvs) >= limit {
				break
			}
		}
	} else {
		kvs, err = s.store.ScanPairs([]byte(prefix), limit, false)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "scan failed")
		return
	}
	entries := make([]keyValue, 0, len(kvs))
	for _, kv := range kvs {
		entry := keyValue{Key: string(kv.Key)}
		if kv.Value != nil {
			entry.Value = base64.StdEncoding.EncodeToString(kv.Value)
		}
		entries = append(entries, entry)
	}
	writeOK(w, scanResponse{Entries: entries})
}

func (s *ShardServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}
	if err := s.store.Delete([]byte(key)); err != nil {
		writeError(w, http.StatusInternalServerError, "delete failed")
		return
	}
	writeOK(w, map[string]any{"ok": true})
}

func (s *ShardServer) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	err := s.store.CreateUser(models.User{ID: req.ID, Name: req.Name})
	switch {
	case err == nil:
		writeOK(w, map[string]any{"ok": true})
	case errors.Is(err, storage.ErrExists):
		writeError(w, http.StatusConflict, "user already exists")
	default:
		writeError(w, http.StatusInternalServerError, "create user failed")
	}
}

func (s *ShardServer) handleFollowEdge(w http.ResponseWriter, r *http.Request) {
	s.handleEdge(w, r, false)
}

func (s *ShardServer) handleUnfollowEdge(w http.ResponseWriter, r *http.Request) {
	s.handleEdge(w, r, true)
}

func (s *ShardServer) handleEdge(w http.ResponseWriter, r *http.Request, unfollow bool) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req edgeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	var err error
	if unfollow {
		err = s.store.RemoveFollower(req.Followee, req.Follower)
	} else {
		err = s.store.AddFollower(req.Followee, req.Follower)
	}
	switch {
	case err == nil:
		writeOK(w, map[string]any{"ok": true})
	case errors.Is(err, storage.ErrNotFound):
		writeError(w, http.StatusNotFound, "user not found")
	default:
		writeError(w, http.StatusInternalServerError, "follow failed")
	}
}

func (s *ShardServer) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Pairs   []keyValue `json:"pairs"`
		Deletes []string   `json:"deletes"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	var ws storage.WriteSet
	for _, p := range req.Pairs {
		val, err := base64.StdEncoding.DecodeString(p.Value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "malformed value")
			return
		}
		ws.Pairs = append(ws.Pairs, storage.KV{Key: []byte(p.Key), Value: val})
	}
	for _, d := range req.Deletes {
		ws.Deletes = append(ws.Deletes, []byte(d))
	}
	if err := s.store.ApplyWriteSet(ws); err != nil {
		writeError(w, http.StatusInternalServerError, "apply failed")
		return
	}
	writeOK(w, map[string]any{"ok": true})
}

func (s *ShardServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeOK(w, healthResponse{NodeID: s.id, Role: s.role, Status: "ok"})
}

// ---- helpers ----

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

func writeOK(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": strings.TrimSpace(msg)})
}
