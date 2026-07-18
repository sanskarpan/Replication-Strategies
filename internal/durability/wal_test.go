package durability

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rec(i int) []byte { return []byte(fmt.Sprintf("record-%d", i)) }

func TestBufferedCrashLosesAckedData(t *testing.T) {
	w := New(Buffered, 0)

	for i := 0; i < 5; i++ {
		durable := w.Append(rec(i))
		assert.False(t, durable, "buffered append must not be durable")
	}

	require.Equal(t, 5, w.Acked())
	require.Len(t, w.BufferedRecords(), 5)
	require.Empty(t, w.DurableRecords())

	w.Crash()

	assert.Greater(t, w.Lost(), 0, "acked-but-unflushed data must be lost")
	assert.Equal(t, 5, w.Lost())
	assert.Empty(t, w.DurableRecords(), "nothing was durable")
	assert.Empty(t, w.BufferedRecords(), "buffer discarded on crash")
}

func TestFsyncCrashLosesNothing(t *testing.T) {
	w := New(Fsync, 0)

	for i := 0; i < 5; i++ {
		durable := w.Append(rec(i))
		assert.True(t, durable, "fsync append must be durable immediately")
	}

	require.Equal(t, 5, w.Acked())
	require.Len(t, w.DurableRecords(), 5)
	require.Empty(t, w.BufferedRecords())

	w.Crash()

	assert.Equal(t, 0, w.Lost(), "fsync mode loses nothing")
	assert.Len(t, w.DurableRecords(), 5, "all records survive the crash")
}

func TestGroupCommitFlushesAtThreshold(t *testing.T) {
	w := New(GroupCommit, 3)

	// First two stay buffered.
	assert.False(t, w.Append(rec(0)))
	assert.False(t, w.Append(rec(1)))
	require.Len(t, w.BufferedRecords(), 2)
	require.Empty(t, w.DurableRecords())

	// Third one hits the threshold and flushes the whole batch.
	assert.True(t, w.Append(rec(2)), "reaching group size makes the batch durable")
	require.Empty(t, w.BufferedRecords(), "buffer drained by auto-flush")
	require.Len(t, w.DurableRecords(), 3)

	w.Crash()
	assert.Equal(t, 0, w.Lost(), "full batch was flushed before the crash")
	assert.Len(t, w.DurableRecords(), 3)
}

func TestGroupCommitCrashLosesPartialBatch(t *testing.T) {
	w := New(GroupCommit, 3)

	// One full batch of 3 auto-flushes.
	w.Append(rec(0))
	w.Append(rec(1))
	require.True(t, w.Append(rec(2)))

	// Two more start a partial batch that never reaches the threshold.
	assert.False(t, w.Append(rec(3)))
	assert.False(t, w.Append(rec(4)))
	require.Len(t, w.BufferedRecords(), 2)

	w.Crash()

	assert.Equal(t, 2, w.Lost(), "partial (unflushed) batch is lost")
	assert.Len(t, w.DurableRecords(), 3, "durable batch survives")
	assert.Equal(t, 5, w.Acked())
}

func TestExplicitFlushMakesBufferedDurable(t *testing.T) {
	w := New(Buffered, 0)

	w.Append(rec(0))
	w.Append(rec(1))
	require.Len(t, w.BufferedRecords(), 2)

	n := w.Flush()
	assert.Equal(t, 2, n, "flush reports records made durable")
	require.Empty(t, w.BufferedRecords())
	require.Len(t, w.DurableRecords(), 2)

	w.Crash()
	assert.Equal(t, 0, w.Lost(), "flushed records survive the crash")
	assert.Len(t, w.DurableRecords(), 2)
}

func TestFlushEmptyBufferIsNoop(t *testing.T) {
	w := New(GroupCommit, 3)
	assert.Equal(t, 0, w.Flush())
	assert.Empty(t, w.DurableRecords())
}

func TestGroupSizeClampedToOne(t *testing.T) {
	w := New(GroupCommit, 0)
	// group size 0 clamps to 1: every append flushes immediately.
	assert.True(t, w.Append(rec(0)))
	assert.Len(t, w.DurableRecords(), 1)
	assert.Empty(t, w.BufferedRecords())
}

func TestAppendCopiesRecord(t *testing.T) {
	w := New(Fsync, 0)
	buf := []byte("original")
	w.Append(buf)
	copy(buf, "MUTATED!")
	assert.Equal(t, "original", string(w.DurableRecords()[0]), "stored record must be defensively copied")
}

func TestConcurrentAppendIsSafe(t *testing.T) {
	w := New(Buffered, 0)
	const goroutines, perG = 16, 64

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				w.Append(rec(g*perG + i))
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * perG
	assert.Equal(t, total, w.Acked())
	assert.Len(t, w.BufferedRecords(), total)

	w.Crash()
	assert.Equal(t, total, w.Lost())
}

func TestModeString(t *testing.T) {
	assert.Equal(t, "buffered", Buffered.String())
	assert.Equal(t, "fsync", Fsync.String())
	assert.Equal(t, "group_commit", GroupCommit.String())
	assert.Equal(t, "Mode(9)", Mode(9).String())
}
