package antientropy

import (
	"testing"
)

// kvFromBytes deterministically builds a map[string]string from fuzz bytes.
// Every 3-byte group: b[0]%8 picks key 'k0'..'k7', b[1] is the value byte,
// b[2]%3 decides add-variant-A / add-variant-B / delete.
func kvFromBytes(b []byte) map[string]string {
	kv := make(map[string]string)
	for i := 0; i+2 < len(b); i += 3 {
		key := string([]byte{'k', '0' + b[i]%8})
		switch b[i+2] % 3 {
		case 0:
			kv[key] = string([]byte{b[i+1]})
		case 1:
			// Use XOR to avoid 0xFF+1 overflow wrapping back to 0x00.
			kv[key] = string([]byte{b[i+1] ^ 0xFF})
		case 2:
			delete(kv, key)
		}
	}
	return kv
}

func FuzzMerkleTreeDiff(f *testing.F) {
	// Seed corpus: empty maps, same key different value, different keys, subset.
	f.Add([]byte{}, []byte{})
	f.Add([]byte{0x01, 0x01, 0x00}, []byte{0x01, 0x02, 0x00})                   // same key, different value
	f.Add([]byte{0x01, 0x01, 0x00}, []byte{0x02, 0x02, 0x00})                   // different keys
	f.Add([]byte{0x01, 0x01, 0x00, 0x02, 0x02, 0x00}, []byte{0x01, 0x01, 0x00}) // extra key on left
	f.Add([]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00, 0x00})                   // same key, same value

	f.Fuzz(func(t *testing.T, ab, bb []byte) {
		a := kvFromBytes(ab)
		b := kvFromBytes(bb)

		ta := BuildTree(a)
		tb := BuildTree(b)
		diff := Diff(ta, tb)

		// Property 1: Reflexivity — Diff(T, T) is always empty.
		if selfDiff := Diff(ta, ta); len(selfDiff) != 0 {
			t.Fatalf("Diff(T,T) not empty: %v", selfDiff)
		}

		// Build membership lookup for remaining properties.
		diffSet := make(map[string]struct{}, len(diff))
		for _, k := range diff {
			diffSet[k] = struct{}{}
		}

		// Property 2: Soundness — every key in diff actually differs between a and b.
		// Check existence explicitly to avoid zero-value aliasing (missing key returns "").
		for _, k := range diff {
			va, aOk := a[k]
			vb, bOk := b[k]
			if !aOk && !bOk {
				t.Fatalf("unsound: key %q in diff but present in neither map", k)
			}
			if aOk && bOk && va == vb {
				t.Fatalf("unsound: key %q in diff but same value %q in both maps", k, va)
			}
		}

		// Property 3: Completeness — every key that truly differs must appear in diff.
		for k, va := range a {
			if vb, ok := b[k]; !ok || vb != va {
				if _, inDiff := diffSet[k]; !inDiff {
					t.Fatalf("incomplete: key %q differs (a=%q, b=%q) but missing from diff", k, va, vb)
				}
			}
		}
		for k, vb := range b {
			if va, ok := a[k]; !ok || va != vb {
				if _, inDiff := diffSet[k]; !inDiff {
					t.Fatalf("incomplete: key %q differs (a=%q, b=%q) but missing from diff", k, va, vb)
				}
			}
		}

		// Property 4: Determinism — calling Diff again yields the same sorted result.
		diff2 := Diff(ta, tb)
		if len(diff) != len(diff2) {
			t.Fatalf("non-deterministic: lengths %d vs %d", len(diff), len(diff2))
		}
		for i := range diff {
			if diff[i] != diff2[i] {
				t.Fatalf("non-deterministic: diff[%d]=%q vs diff2[%d]=%q", i, diff[i], i, diff2[i])
			}
		}
	})
}
