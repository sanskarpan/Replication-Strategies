package checker

import "testing"

// TestSequentialLinearizable verifies that a clearly sequential history where a
// write completes before a read is invoked linearizes.
func TestSequentialLinearizable(t *testing.T) {
	ops := []Op{
		{ClientID: "c1", Kind: OpWrite, Key: "x", Value: "1", Invoke: 10, Complete: 20},
		{ClientID: "c1", Kind: OpRead, Key: "x", Value: "1", Invoke: 30, Complete: 40},
	}
	ok, bad := CheckRegister(ops)
	if !ok {
		t.Fatalf("expected sequential history to be linearizable, got violation at %+v", bad)
	}
	if bad != nil {
		t.Fatalf("expected nil offending op, got %+v", bad)
	}
}

// TestReadOfNeverWrittenValueNotLinearizable verifies that a read returning a
// value that was never written is rejected with the offending op returned.
func TestReadOfNeverWrittenValueNotLinearizable(t *testing.T) {
	ops := []Op{
		{ClientID: "c1", Kind: OpWrite, Key: "x", Value: "1", Invoke: 10, Complete: 20},
		{ClientID: "c1", Kind: OpRead, Key: "x", Value: "99", Invoke: 30, Complete: 40},
	}
	ok, bad := CheckRegister(ops)
	if ok {
		t.Fatalf("expected history with phantom read to be non-linearizable")
	}
	if bad == nil {
		t.Fatalf("expected an offending op, got nil")
	}
	if bad.Kind != OpRead || bad.Value != "99" {
		t.Fatalf("expected offending op to be the read of 99, got %+v", bad)
	}
}

// TestStaleReadAfterNewerCommitNotLinearizable verifies that reading an older
// value after a newer value committed non-concurrently is rejected.
func TestStaleReadAfterNewerCommitNotLinearizable(t *testing.T) {
	ops := []Op{
		{ClientID: "c1", Kind: OpWrite, Key: "x", Value: "1", Invoke: 10, Complete: 20},
		{ClientID: "c1", Kind: OpWrite, Key: "x", Value: "2", Invoke: 30, Complete: 40},
		// Read invoked strictly after both writes completed, but returns the
		// stale value "1".
		{ClientID: "c1", Kind: OpRead, Key: "x", Value: "1", Invoke: 50, Complete: 60},
	}
	ok, bad := CheckRegister(ops)
	if ok {
		t.Fatalf("expected stale read to be non-linearizable")
	}
	if bad == nil {
		t.Fatalf("expected an offending op, got nil")
	}
	if bad.Kind != OpRead || bad.Value != "1" {
		t.Fatalf("expected offending op to be the stale read of 1, got %+v", bad)
	}
}

// TestConcurrentReadEitherValueLinearizable verifies that a read concurrent
// with a write may legally observe either the old or new value.
func TestConcurrentReadEitherValueLinearizable(t *testing.T) {
	// Establish an initial value, then overlap a write and a read.
	base := []Op{
		{ClientID: "c1", Kind: OpWrite, Key: "x", Value: "old", Invoke: 0, Complete: 5},
	}

	// The read overlaps the write of "new"; either outcome is valid.
	seeOld := append([]Op{}, base...)
	seeOld = append(seeOld,
		Op{ClientID: "c2", Kind: OpWrite, Key: "x", Value: "new", Invoke: 10, Complete: 30},
		Op{ClientID: "c3", Kind: OpRead, Key: "x", Value: "old", Invoke: 12, Complete: 28},
	)
	if ok, bad := CheckRegister(seeOld); !ok {
		t.Fatalf("expected concurrent read seeing old value to be linearizable, violation at %+v", bad)
	}

	seeNew := append([]Op{}, base...)
	seeNew = append(seeNew,
		Op{ClientID: "c2", Kind: OpWrite, Key: "x", Value: "new", Invoke: 10, Complete: 30},
		Op{ClientID: "c3", Kind: OpRead, Key: "x", Value: "new", Invoke: 12, Complete: 28},
	)
	if ok, bad := CheckRegister(seeNew); !ok {
		t.Fatalf("expected concurrent read seeing new value to be linearizable, violation at %+v", bad)
	}
}

// TestMultipleKeysIndependent verifies that keys are checked independently and
// a violation on one key fails the whole history.
func TestMultipleKeysIndependent(t *testing.T) {
	ops := []Op{
		{ClientID: "c1", Kind: OpWrite, Key: "a", Value: "1", Invoke: 10, Complete: 20},
		{ClientID: "c1", Kind: OpRead, Key: "a", Value: "1", Invoke: 30, Complete: 40},
		{ClientID: "c2", Kind: OpWrite, Key: "b", Value: "1", Invoke: 10, Complete: 20},
		{ClientID: "c2", Kind: OpRead, Key: "b", Value: "2", Invoke: 30, Complete: 40},
	}
	ok, bad := CheckRegister(ops)
	if ok {
		t.Fatalf("expected violation on key b")
	}
	if bad == nil || bad.Key != "b" {
		t.Fatalf("expected offending op on key b, got %+v", bad)
	}
}

// TestHistoryRecordAndOps verifies the concurrency-safe History accessors.
func TestHistoryRecordAndOps(t *testing.T) {
	var h History
	h.Record(Op{ClientID: "c1", Kind: OpWrite, Key: "x", Value: "1", Invoke: 1, Complete: 2})
	h.Record(Op{ClientID: "c1", Kind: OpRead, Key: "x", Value: "1", Invoke: 3, Complete: 4})

	got := h.Ops()
	if len(got) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(got))
	}
	// Mutating the returned copy must not affect the history.
	got[0].Value = "mutated"
	if h.Ops()[0].Value != "1" {
		t.Fatalf("Ops() should return a copy, but underlying data was mutated")
	}
}
