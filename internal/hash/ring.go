package hash

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
)

// Ring is a consistent hash ring with virtual nodes, hand-implemented (no
// library — a stated learning goal). Keys hash onto a circle; each physical
// node owns the arc clockwise from its virtual positions until the next.
//
// Static membership: RemoveNode exists for API completeness, but failure
// handling never calls it — re-hashing would strand every key. Failure means
// failover to a replica, not ring mutation (see design.md).
type Ring struct {
	nodes        []uint32        // sorted virtual-node positions
	nodeMap      map[uint32]string // virtual position -> physical node
	virtualNodes int
}

// NewRing creates an empty ring with the given number of virtual nodes per
// physical node. 150 gives even distribution across 3 shards.
func NewRing(virtualNodes int) *Ring {
	return &Ring{
		nodeMap:      make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

// hash reduces a string to a well-distributed 32-bit position. MD5 has strong
// avalanche: correlated inputs (sequential keys, node#index salts) don't
// cluster, which is what makes the ring's distribution even. FNV was rejected
// here because its weak diffusion let sequential keys pile onto one node.
func (r *Ring) hash(s string) uint32 {
	sum := md5.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

// AddNode adds a physical node, giving it virtualNodes positions on the ring.
func (r *Ring) AddNode(nodeID string) {
	for i := 0; i < r.virtualNodes; i++ {
		pos := r.hash(nodeID + "#" + strconv.Itoa(i))
		if _, exists := r.nodeMap[pos]; exists {
			continue // collision with an existing virtual node; skip
		}
		r.nodeMap[pos] = nodeID
		r.nodes = append(r.nodes, pos)
	}
	sort.Slice(r.nodes, func(a, b int) bool { return r.nodes[a] < r.nodes[b] })
}

// RemoveNode removes every virtual position belonging to a physical node.
func (r *Ring) RemoveNode(nodeID string) {
	kept := make([]uint32, 0, len(r.nodes))
	for _, pos := range r.nodes {
		if r.nodeMap[pos] == nodeID {
			delete(r.nodeMap, pos)
		} else {
			kept = append(kept, pos)
		}
	}
	r.nodes = kept
}

// GetNode returns the physical node owning a key: hash the key, walk
// clockwise to the first virtual position at or after it, wrapping around.
func (r *Ring) GetNode(key string) (string, error) {
	if len(r.nodes) == 0 {
		return "", errors.New("hash ring: empty")
	}
	h := r.hash(key)
	idx := sort.Search(len(r.nodes), func(i int) bool { return r.nodes[i] >= h })
	if idx == len(r.nodes) {
		idx = 0 // wrap around
	}
	return r.nodeMap[r.nodes[idx]], nil
}
