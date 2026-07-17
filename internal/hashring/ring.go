// Package hashring implements consistent hashing with virtual nodes and
// preference lists for Dynamo-style key placement.
package hashring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// vnode is a single point on the hash ring owned by a physical node.
type vnode struct {
	token uint64
	node  string
}

// Ring is a consistent hash ring with virtual nodes. It is safe for
// concurrent use.
type Ring struct {
	mu     sync.RWMutex
	tokens []vnode         // sorted by token ascending
	nodes  map[string]bool // set of physical nodes
	vnodes int             // virtual nodes per physical node
}

// NewRing creates an empty ring with the given number of virtual nodes per
// physical node. If vnodes <= 0 it defaults to 128.
func NewRing(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = 128
	}
	return &Ring{
		nodes:  make(map[string]bool),
		vnodes: vnodes,
	}
}

// hashKey hashes a string to a uint64 using a stable non-cryptographic hash.
func hashKey(s string) uint64 {
	h := fnv.New64a()
	// Write never returns an error for the hash.Hash implementations.
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// Add inserts the vnodes tokens for nodeID and keeps the token slice sorted.
// Adding an existing node is a no-op.
func (r *Ring) Add(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}
	r.nodes[nodeID] = true

	for i := 0; i < r.vnodes; i++ {
		token := hashKey(fmt.Sprintf("%s#%d", nodeID, i))
		r.tokens = append(r.tokens, vnode{token: token, node: nodeID})
	}
	sort.Slice(r.tokens, func(i, j int) bool {
		return r.tokens[i].token < r.tokens[j].token
	})
}

// Remove deletes all tokens owned by nodeID. Removing an unknown node is a
// no-op.
func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}
	delete(r.nodes, nodeID)

	filtered := r.tokens[:0]
	for _, v := range r.tokens {
		if v.node != nodeID {
			filtered = append(filtered, v)
		}
	}
	r.tokens = filtered
}

// Nodes returns the sorted set of physical nodes.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// PreferenceList returns up to n distinct physical nodes responsible for key,
// walking the ring clockwise from the key's hash position. The result is
// deterministic for a given ring and key.
func (r *Ring) PreferenceList(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if n <= 0 || len(r.tokens) == 0 {
		return nil
	}

	// Cap n at the number of distinct physical nodes.
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := hashKey(key)
	// First token with token >= h (clockwise starting point).
	start := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].token >= h
	})
	if start == len(r.tokens) {
		start = 0 // wrap around
	}

	result := make([]string, 0, n)
	seen := make(map[string]bool, n)
	for i := 0; i < len(r.tokens); i++ {
		v := r.tokens[(start+i)%len(r.tokens)]
		if seen[v.node] {
			continue
		}
		seen[v.node] = true
		result = append(result, v.node)
		if len(result) == n {
			break
		}
	}
	return result
}
