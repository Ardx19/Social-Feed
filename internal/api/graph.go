package api

import (
	"log"
	"net/http"

	"socialfeed/internal/keys"
)

// edgeBody is the public follow/unfollow request.
type edgeBody struct {
	Follower string `json:"follower"`
	Followee string `json:"followee"`
}

// handleFollow implements POST /follow and DELETE /follow semantics via the
// cross-shard write contract: A-side following edge on the follower's shard
// (plain Set, naturally idempotent), then B-side count-bearing transaction on
// the followee's shard. Half-edges are accepted and logged (see origin: R8).
func (c *CoordinatorServer) handleFollow(w http.ResponseWriter, r *http.Request) {
	c.handleEdge(w, r, false)
}

// handleUnfollow implements POST /unfollow (and DELETE /follow): the mirror.
func (c *CoordinatorServer) handleUnfollow(w http.ResponseWriter, r *http.Request) {
	c.handleEdge(w, r, true)
}

func (c *CoordinatorServer) handleEdge(w http.ResponseWriter, r *http.Request, unfollow bool) {
	var req edgeBody
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return
	}
	if !validateID(req.Follower) || !validateID(req.Followee) {
		writeError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if req.Follower == req.Followee {
		writeError(w, http.StatusBadRequest, "cannot follow yourself")
		return
	}
	// Both IDs must resolve to existing users: a follow of a missing followee
	// must 404, and we must never create a dangling following edge for a
	// missing follower.
	if !c.userExists(req.Follower) {
		writeError(w, http.StatusNotFound, "follower not found")
		return
	}
	if !c.userExists(req.Followee) {
		writeError(w, http.StatusNotFound, "followee not found")
		return
	}

	// A-side: following edge on the follower's shard. Queue-eligible — its key/
	// value is materializable without reading the down shard, so if the
	// follower's primary is down we queue it (replayed on recovery) rather than
	// fail the follow. Post creation is the exception that is NEVER queued.
	followerNode, followerAddr, err := c.ShardFor(req.Follower)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no shard")
		return
	}
	akey := keys.FollowingEdgeKey(req.Follower, req.Followee)
	op := QueuedOp{Addr: followerAddr, Pairs: []byteKV{{Key: akey}}}
	queued, werr := c.writeToShard(followerNode, followerAddr, op, func() error {
		return c.client.Write(followerAddr, op.Pairs)
	})
	switch {
	case werr == ErrQueueFull:
		writeError(w, http.StatusServiceUnavailable, "write queue full")
		return
	case queued:
		log.Printf("follow: A-side queued (follower shard %s down); will replay on recovery", followerNode)
		// Accepted; proceed to B-side best-effort below.
	}

	// B-side: count-bearing transaction on the followee's shard.
	_, followeeAddr, err := c.ShardFor(req.Followee)
	if err != nil {
		log.Printf("follow: half-edge after A-side; followee shard error: %v", err)
		writeError(w, http.StatusInternalServerError, "no shard")
		return
	}
	var berr error
	if unfollow {
		berr = c.client.UnfollowEdge(followeeAddr, req.Followee, req.Follower)
	} else {
		berr = c.client.FollowEdge(followeeAddr, req.Followee, req.Follower)
	}
	if berr != nil {
		// A-side succeeded; B-side failed -> accepted half-edge, logged.
		log.Printf("follow: half-edge (A-side written, B-side failed): %v", berr)
		writeError(w, http.StatusServiceUnavailable, "followee edge write failed")
		return
	}
	writeOK(w, map[string]any{"ok": true})
}

// userExists reports whether a user record exists (via failover-aware read).
func (c *CoordinatorServer) userExists(id string) bool {
	nodeID, addr, err := c.readAddrFor(id)
	if err != nil {
		return false
	}
	_, found, err := c.readWithFailover(nodeID, addr, keys.UserKey(id))
	return err == nil && found
}
