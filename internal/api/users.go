package api

import (
	"errors"
	"net/http"
	"strings"

	"socialfeed/internal/keys"
	"socialfeed/internal/models"
)

type createUserBody struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// handleCreateUser implements POST /users — create a user with self-follow
// edges. Existing ID -> 409 (no overwrite; keeps FollowerCount and seed
// idempotency intact).
func (c *CoordinatorServer) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req createUserBody
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	if !validateID(req.ID) {
		writeError(w, http.StatusBadRequest, "invalid user ID (no ':', control chars; max 64)")
		return
	}
	if !validName(req.Name) {
		writeError(w, http.StatusBadRequest, "invalid name")
		return
	}
	_, addr, err := c.ShardFor(req.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no shard for user")
		return
	}
	switch cerr := c.client.CreateUser(addr, req.ID, req.Name); {
	case cerr == nil:
		writeOK(w, map[string]any{"id": req.ID, "name": req.Name})
	case errors.Is(cerr, ErrConflict):
		writeError(w, http.StatusConflict, "user already exists")
	default:
		writeError(w, http.StatusServiceUnavailable, "create user failed")
	}
}

// handleGetUser implements GET /users/{user_id}. Missing -> 404 (distinct from
// the shard-level missing, which the coordinator maps to not-found for the
// public API).
func (c *CoordinatorServer) handleGetUser(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/users/")
	if !validateID(id) {
		writeError(w, http.StatusBadRequest, "invalid user ID")
		return
	}
	_, addr, err := c.ShardFor(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no shard for user")
		return
	}
	raw, found, rerr := c.client.Read(addr, keys.UserKey(id))
	switch {
	case rerr != nil:
		writeError(w, http.StatusServiceUnavailable, "read failed")
	case !found:
		writeError(w, http.StatusNotFound, "user not found")
	default:
		var u models.User
		if derr := c.decode(raw, &u); derr != nil {
			writeError(w, http.StatusInternalServerError, "decode failed")
			return
		}
		writeOK(w, map[string]any{"id": u.ID, "name": u.Name, "follower_count": u.FollowerCount})
	}
}
