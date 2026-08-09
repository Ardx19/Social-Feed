package api

import (
	"log"
	"sync"
	"time"
)

// HealthMonitor tracks each shard node's liveness via periodic pings. A node
// is marked down after `maxMiss` consecutive failed pings; a successful ping
// marks it up and fires onRecover. Ring membership stays static — recovery is
// signaled, never a re-hash (see design.md).
type HealthMonitor struct {
	client    *ShardClient
	interval  time.Duration
	maxMiss   int
	onRecover func(nodeID string)

	mu    sync.RWMutex
	state map[string]*nodeState
	done  chan struct{}
}

type nodeState struct {
	nodeID  string
	addr    string
	replica string
	misses  int
	down    bool
}

// NewHealthMonitor builds a monitor over the given topology.
func NewHealthMonitor(client *ShardClient, shards, replicas map[string]string, interval time.Duration, maxMiss int) *HealthMonitor {
	state := make(map[string]*nodeState, len(shards))
	for id, addr := range shards {
		state[id] = &nodeState{nodeID: id, addr: addr, replica: replicas[id]}
	}
	return &HealthMonitor{
		client:   client,
		interval: interval,
		maxMiss:  maxMiss,
		state:    state,
		done:     make(chan struct{}),
	}
}

// Start launches the heartbeat loop.
func (h *HealthMonitor) Start() { go h.loop() }

// Stop halts the heartbeat loop.
func (h *HealthMonitor) Stop() { close(h.done) }

func (h *HealthMonitor) loop() {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-h.done:
			return
		case <-t.C:
			h.checkOnce()
		}
	}
}

func (h *HealthMonitor) checkOnce() {
	h.mu.RLock()
	states := make([]*nodeState, 0, len(h.state))
	for _, s := range h.state {
		states = append(states, s)
	}
	h.mu.RUnlock()

	for _, s := range states {
		ok, _ := h.client.Health(s.addr)
		h.mu.Lock()
		if ok {
			wasDown := s.down
			s.misses = 0
			s.down = false
			if wasDown {
				log.Printf("heartbeat: %s recovered", s.nodeID)
				if h.onRecover != nil {
					h.onRecover(s.nodeID)
				}
			}
		} else {
			s.misses++
			if s.misses >= h.maxMiss && !s.down {
				s.down = true
				log.Printf("heartbeat: %s marked down after %d misses", s.nodeID, h.maxMiss)
			}
		}
		h.mu.Unlock()
	}
}

// IsDown reports whether a node is currently marked down.
func (h *HealthMonitor) IsDown(nodeID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if s := h.state[nodeID]; s != nil {
		return s.down
	}
	return false
}

// PrimaryAddr returns the node's primary addr (empty if unknown).
func (h *HealthMonitor) PrimaryAddr(nodeID string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if s := h.state[nodeID]; s != nil {
		return s.addr
	}
	return ""
}

// ReplicaFor returns the node's replica addr, if any.
func (h *HealthMonitor) ReplicaFor(nodeID string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s := h.state[nodeID]
	if s == nil {
		return "", false
	}
	return s.replica, s.replica != ""
}

// SetOnRecover registers the callback fired when a node comes back up.
func (h *HealthMonitor) SetOnRecover(fn func(nodeID string)) { h.onRecover = fn }
