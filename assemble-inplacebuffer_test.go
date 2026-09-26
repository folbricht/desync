package desync

import (
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInPlaceBuffer(t *testing.T) {
	name := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(name, []byte("0123456789"), 0644))
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	overwrite := func() {
		_, err := f.WriteAt([]byte("xxxxx"), 0)
		require.NoError(t, err)
	}
	reset := func() {
		_, err := f.WriteAt([]byte("01234"), 0)
		require.NoError(t, err)
	}

	t.Run("taken without filling", func(t *testing.T) {
		// Nothing overwrote the source, the move reads it directly
		budget := &memoryBudget{available: 0}
		b := &inPlaceBuffer{offset: 2, size: 3, budget: budget}
		data, err := b.take(f)
		require.NoError(t, err)
		require.Equal(t, []byte("234"), data)

		// Steps overwriting the source later don't fill it anymore
		require.NoError(t, b.fill(f))
		require.False(t, b.pending())
		require.False(t, b.dropped())
	})

	t.Run("filled before being overwritten", func(t *testing.T) {
		defer reset()
		budget := &memoryBudget{available: 3}
		b := &inPlaceBuffer{offset: 2, size: 3, budget: budget}
		require.True(t, b.pending())
		require.NoError(t, b.fill(f))
		require.Equal(t, int64(0), budget.available)
		overwrite()

		data, err := b.take(f)
		require.NoError(t, err)
		require.Equal(t, []byte("234"), data)
		require.Equal(t, int64(3), budget.available, "memory returned to the budget")
	})

	t.Run("dropped without budget", func(t *testing.T) {
		defer reset()
		b := &inPlaceBuffer{offset: 2, size: 3, budget: &memoryBudget{available: 2}}
		require.NoError(t, b.fill(f))
		require.True(t, b.dropped())
		overwrite()

		data, err := b.take(f)
		require.NoError(t, err)
		require.Nil(t, data, "the move has to use the store")
	})
}

func TestStepQueue(t *testing.T) {
	pending := &inPlaceBuffer{budget: &memoryBudget{}}
	filled := &inPlaceBuffer{state: bufferFilled}
	var (
		releasing = &planStep{source: &inPlaceCopy{buffer: pending}}
		filling   = &planStep{source: &skipInPlace{}, fills: []*inPlaceBuffer{pending}}
		plain     = &planStep{source: &skipInPlace{}}
		refilling = &planStep{source: &skipInPlace{}, fills: []*inPlaceBuffer{filled}}
	)
	var q stepQueue
	for _, s := range []*planStep{filling, plain, refilling, releasing} {
		q.push(s)
	}
	var got []*planStep
	for s := q.peek(); s != nil; s = q.peek() {
		got = append(got, s)
		q.pop()
	}
	// Buffers are released first and new ones filled last. Filling a buffer
	// that's filled already doesn't hold more memory.
	require.Equal(t, []*planStep{releasing, plain, refilling, filling}, got)
}

func TestMemoryLimitBudget(t *testing.T) {
	orig := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(orig) })

	debug.SetMemoryLimit(math.MaxInt64)
	require.Equal(t, int64(math.MaxInt64), memoryLimitBudget())

	// With a limit, the budget is what's left under it
	const limit = 1 << 40
	debug.SetMemoryLimit(limit)
	budget := memoryLimitBudget()
	require.Greater(t, budget, int64(0))
	require.Less(t, budget, int64(limit))
}
