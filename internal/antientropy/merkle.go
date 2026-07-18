// Package antientropy implements a Merkle tree used for anti-entropy
// key-diffing between two replicas. Instead of exchanging an entire key-value
// store, two replicas can compare Merkle roots and descend only into subtrees
// whose hashes differ, exchanging just the keys that actually diverge.
package antientropy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// emptyRootHash is the fixed root hash returned for an empty store. It is a
// constant so that two empty stores always compare equal.
const emptyRootHash = "empty:0000000000000000000000000000000000000000000000000000000000000000"

// Node is a node in the Merkle tree. Leaves carry the originating Key; internal
// nodes have Left/Right children and an empty Key.
type Node struct {
	Hash  string
	Left  *Node
	Right *Node
	Key   string // set on leaves only
}

// hashString returns the hex-encoded SHA-256 of s.
func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// leafHash hashes a "key=value" pair for a leaf node.
func leafHash(key, value string) string {
	return hashString(fmt.Sprintf("%s=%s", key, value))
}

// BuildTree builds a balanced binary Merkle tree over the keys of kv. Keys are
// sorted so the construction is deterministic: building over the same map twice
// yields equal root hashes. An empty map yields a single node whose Hash is a
// fixed constant.
func BuildTree(kv map[string]string) *Node {
	if len(kv) == 0 {
		return &Node{Hash: emptyRootHash}
	}

	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	leaves := make([]*Node, len(keys))
	for i, k := range keys {
		leaves[i] = &Node{
			Hash: leafHash(k, kv[k]),
			Key:  k,
		}
	}

	return buildLevel(leaves)
}

// buildLevel recursively combines a slice of nodes into a balanced binary tree.
func buildLevel(nodes []*Node) *Node {
	if len(nodes) == 1 {
		return nodes[0]
	}

	mid := len(nodes) / 2
	left := buildLevel(nodes[:mid])
	right := buildLevel(nodes[mid:])

	return &Node{
		Hash:  hashString(left.Hash + right.Hash),
		Left:  left,
		Right: right,
	}
}

// Diff walks trees a and b in parallel. Where subtree hashes match, the subtree
// is pruned; where they differ, it descends. It returns the sorted set of keys
// that differ between the two stores (present-on-one-side or different-value).
func Diff(a, b *Node) []string {
	diffs := make(map[string]struct{})
	diffNodes(a, b, diffs)

	out := make([]string, 0, len(diffs))
	for k := range diffs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// diffNodes recursively compares two subtrees, collecting differing keys.
//
// The trees are balanced by leaf count, so when the two stores hold a different
// number of keys their shapes diverge and positional alignment breaks down.
// diffNodes therefore prunes identical subtrees quickly (the anti-entropy fast
// path: equal hashes mean the whole subtree matches and is skipped), and once
// it reaches a region where the shapes no longer align by key it falls back to
// a key-based reconciliation of the leaves under both sides.
func diffNodes(a, b *Node, diffs map[string]struct{}) {
	// Both absent: nothing to compare.
	if a == nil && b == nil {
		return
	}

	// If both present and their hashes match, the subtrees are identical and
	// can be pruned. This is also the base case that stops descent.
	if a != nil && b != nil && a.Hash == b.Hash {
		return
	}

	// One side absent: every key under the present side differs.
	if a == nil {
		collectLeaves(b, diffs)
		return
	}
	if b == nil {
		collectLeaves(a, diffs)
		return
	}

	// Both sides internal nodes covering the same set of keys: descend, which
	// keeps pruning matching children. This is the common single-value-change
	// case and lets us skip untouched subtrees.
	if !a.isLeaf() && !b.isLeaf() && a.sameKeys(b) {
		diffNodes(a.Left, b.Left, diffs)
		diffNodes(a.Right, b.Right, diffs)
		return
	}

	// Shapes no longer align by key (different key sets on each side).
	// Reconcile the leaves under both subtrees by key.
	reconcile(a, b, diffs)
}

// reconcile compares the leaves under a and b by key, recording keys that are
// present on only one side or whose leaf hashes differ.
func reconcile(a, b *Node, diffs map[string]struct{}) {
	aLeaves := map[string]string{}
	bLeaves := map[string]string{}
	collectLeafHashes(a, aLeaves)
	collectLeafHashes(b, bLeaves)

	for k, h := range aLeaves {
		if bh, ok := bLeaves[k]; !ok || bh != h {
			diffs[k] = struct{}{}
		}
	}
	for k := range bLeaves {
		if _, ok := aLeaves[k]; !ok {
			diffs[k] = struct{}{}
		}
	}
}

// sameKeys reports whether a and b cover exactly the same set of keys. Because
// keys are sorted at build time, the minimum and maximum key under each subtree
// bound its range; equal key sets require equal spans and equal counts.
func (a *Node) sameKeys(b *Node) bool {
	ak := map[string]struct{}{}
	bk := map[string]struct{}{}
	collectKeySet(a, ak)
	collectKeySet(b, bk)
	if len(ak) != len(bk) {
		return false
	}
	for k := range ak {
		if _, ok := bk[k]; !ok {
			return false
		}
	}
	return true
}

// isLeaf reports whether n is a leaf (a real key-bearing leaf, not the empty
// root sentinel).
func (n *Node) isLeaf() bool {
	return n.Left == nil && n.Right == nil && n.Key != ""
}

// collectLeaves gathers all leaf keys under n into diffs.
func collectLeaves(n *Node, diffs map[string]struct{}) {
	if n == nil {
		return
	}
	if n.isLeaf() {
		diffs[n.Key] = struct{}{}
		return
	}
	collectLeaves(n.Left, diffs)
	collectLeaves(n.Right, diffs)
}

// collectKeySet gathers the set of leaf keys under n.
func collectKeySet(n *Node, out map[string]struct{}) {
	if n == nil {
		return
	}
	if n.isLeaf() {
		out[n.Key] = struct{}{}
		return
	}
	collectKeySet(n.Left, out)
	collectKeySet(n.Right, out)
}

// collectLeafHashes gathers key -> leaf hash for all leaves under n.
func collectLeafHashes(n *Node, out map[string]string) {
	if n == nil {
		return
	}
	if n.isLeaf() {
		out[n.Key] = n.Hash
		return
	}
	collectLeafHashes(n.Left, out)
	collectLeafHashes(n.Right, out)
}
