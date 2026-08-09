package api

import (
	"log"
	"net/http"
	"sort"
	"strings"

	"socialfeed/internal/hash"
)

// CoordinatorServer routes requests to shards by entity ID and exposes the
// public API. It owns the ring and the topology (shards + their replicas).
// The public handlers for users/posts/feed/follow are stubbed here and wired
// in later units.
type CoordinatorServer struct {
	ring      *hash.Ring
	shards    map[string]string // nodeID -> addr
	replicas  map[string]string // nodeID -> replica addr
	threshold int64
	client    *ShardClient
}

// CoordinatorConfig wires the coordinator from parsed topology.
type CoordinatorConfig struct {
	Shards    map[string]string
	Replicas  map[string]string
	Threshold int64
	Client    *ShardClient
}

// NewCoordinator builds the coordinator, seeding the ring with every shard
// node ID. Ring membership is static — nodes are never removed for failure.
func NewCoordinator(cfg CoordinatorConfig) *CoordinatorServer {
	ring := hash.NewRing(150)
	ids := make([]string, 0, len(cfg.Shards))
	for id := range cfg.Shards {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic ring seeding order
	for _, id := range ids {
		ring.AddNode(id)
	}
	return &CoordinatorServer{
		ring:      ring,
		shards:    cfg.Shards,
		replicas:  cfg.Replicas,
		threshold: cfg.Threshold,
		client:    cfg.Client,
	}
}

// ShardFor routes an entity ID to its owning shard addr. The ring hashes the
// entity ID (never a full storage key) so all of an entity's keys land on one
// shard, which is what makes prefix scans possible.
func (c *CoordinatorServer) ShardFor(entityID string) (nodeID, addr string, err error) {
	nodeID, err = c.ring.GetNode(entityID)
	if err != nil {
		return "", "", err
	}
	return nodeID, c.shards[nodeID], nil
}

// ReplicaFor returns the replica addr for a shard node, if any.
func (c *CoordinatorServer) ReplicaFor(nodeID string) (string, bool) {
	r, ok := c.replicas[nodeID]
	return r, ok
}

// Threshold returns the celebrity gate.
func (c *CoordinatorServer) Threshold() int64 { return c.threshold }

// Client exposes the shard client for later units.
func (c *CoordinatorServer) Client() *ShardClient { return c.client }

// Handler returns the coordinator's HTTP routes.
func (c *CoordinatorServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", c.handleHealth)
	mux.HandleFunc("/cluster", c.handleCluster)

	// Public API — stubbed here, wired in later units.
	mux.HandleFunc("/users", c.stub)
	mux.HandleFunc("/users/", c.stub)
	mux.HandleFunc("/follow", c.stub)
	mux.HandleFunc("/posts", c.stub)
	mux.HandleFunc("/posts/", c.stub)
	mux.HandleFunc("/feed/", c.stub)
	return mux
}

func (c *CoordinatorServer) stub(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "handler wired in a later unit")
}

func (c *CoordinatorServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeOK(w, map[string]string{"status": "ok"})
}

// handleCluster reports each shard node and its replica with a live reachable
// check. A full heartbeat loop lands in U10; here the check is synchronous.
func (c *CoordinatorServer) handleCluster(w http.ResponseWriter, r *http.Request) {
	ids := make([]string, 0, len(c.shards))
	for id := range c.shards {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	type nodeStatus struct {
		ID        string `json:"id"`
		Role      string `json:"role"`
		Addr      string `json:"addr"`
		Reachable bool   `json:"reachable"`
	}
	var out []nodeStatus
	for _, id := range ids {
		reachable, err := c.client.Health(c.shards[id])
		if err != nil {
			log.Printf("cluster: %s unreachable: %v", id, err)
			reachable = false
		}
		out = append(out, nodeStatus{ID: id, Role: "primary", Addr: c.shards[id], Reachable: reachable})
		if rep, ok := c.replicas[id]; ok {
			repOk, err := c.client.Health(rep)
			if err != nil {
				repOk = false
			}
			out = append(out, nodeStatus{ID: id + "-replica", Role: "replica", Addr: rep, Reachable: repOk})
		}
	}
	writeOK(w, out)
}

// ParseTopology parses "nodeID:host:port,nodeID:host:port" into a map.
func ParseTopology(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Split on ':' limited to avoid splitting the host port delimiter.
		segs := strings.Split(part, ":")
		if len(segs) == 3 {
			out[segs[0]] = segs[1] + ":" + segs[2]
		}
	}
	return out
}
