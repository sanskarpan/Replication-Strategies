package antientropy

import (
	"reflect"
	"testing"
)

func TestIdenticalMapsDiffEmpty(t *testing.T) {
	kv := map[string]string{"a": "1", "b": "2", "c": "3"}
	a := BuildTree(kv)
	b := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3"})

	if got := Diff(a, b); len(got) != 0 {
		t.Fatalf("expected empty diff for identical maps, got %v", got)
	}
}

func TestDeterministicRoot(t *testing.T) {
	kv := map[string]string{"z": "9", "a": "1", "m": "5"}
	h1 := BuildTree(kv).Hash
	h2 := BuildTree(kv).Hash
	if h1 != h2 {
		t.Fatalf("root hash not deterministic: %q != %q", h1, h2)
	}
}

func TestChangedValueDetected(t *testing.T) {
	a := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3"})
	b := BuildTree(map[string]string{"a": "1", "b": "CHANGED", "c": "3"})

	got := Diff(a, b)
	want := []string{"b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected diff %v, got %v", want, got)
	}
}

func TestRootChangesWhenValueChanges(t *testing.T) {
	a := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3"})
	b := BuildTree(map[string]string{"a": "1", "b": "2", "c": "999"})

	if a.Hash == b.Hash {
		t.Fatal("root hash should change when a value changes")
	}
}

func TestKeyPresentOnOneSide(t *testing.T) {
	a := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3"})
	b := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"})

	got := Diff(a, b)
	want := []string{"d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected diff %v for added key, got %v", want, got)
	}
}

func TestKeyRemovedDetected(t *testing.T) {
	a := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3"})
	b := BuildTree(map[string]string{"a": "1", "c": "3"})

	got := Diff(a, b)
	want := []string{"b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected diff %v for removed key, got %v", want, got)
	}
}

func TestMultipleDifferencesSorted(t *testing.T) {
	a := BuildTree(map[string]string{"a": "1", "b": "2", "c": "3", "e": "5"})
	b := BuildTree(map[string]string{"a": "X", "b": "2", "d": "4", "e": "5"})

	// a changed, c removed (only in a), d added (only in b).
	got := Diff(a, b)
	want := []string{"a", "c", "d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected diff %v, got %v", want, got)
	}
}

func TestEmptyMapRootConstant(t *testing.T) {
	e1 := BuildTree(map[string]string{})
	e2 := BuildTree(nil)

	if e1.Hash != emptyRootHash || e2.Hash != emptyRootHash {
		t.Fatalf("empty map should yield fixed root %q, got %q and %q", emptyRootHash, e1.Hash, e2.Hash)
	}
	if got := Diff(e1, e2); len(got) != 0 {
		t.Fatalf("two empty trees should diff empty, got %v", got)
	}
}

func TestEmptyVsNonEmpty(t *testing.T) {
	empty := BuildTree(map[string]string{})
	full := BuildTree(map[string]string{"a": "1", "b": "2"})

	got := Diff(empty, full)
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected all keys %v when diffing against empty, got %v", want, got)
	}
}
