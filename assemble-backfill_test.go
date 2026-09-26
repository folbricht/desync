package desync

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeOldFile writes a file whose modification time lies well in the past,
// so only a later write can make it look changed.
func writeOldFile(t *testing.T, name string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(name, data, 0644))
	past := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(name, past, past))
}

func TestSeedFileStateChanged(t *testing.T) {
	// Record the file the way validation does, from the open file. On
	// Windows, the identity of a file stat'ed by name is looked up by name
	// again when it's compared, which would find the replacement.
	record := func(t *testing.T, name string) seedFileState {
		t.Helper()
		f, err := os.Open(name)
		require.NoError(t, err)
		defer f.Close()
		info, err := f.Stat()
		require.NoError(t, err)
		return newSeedFileState(info)
	}
	dir := t.TempDir()

	t.Run("unchanged", func(t *testing.T) {
		name := filepath.Join(dir, "unchanged")
		writeOldFile(t, name, []byte("seed data"))
		assert.False(t, record(t, name).changed(name))
	})
	t.Run("written to", func(t *testing.T) {
		name := filepath.Join(dir, "written")
		writeOldFile(t, name, []byte("seed data"))
		state := record(t, name)
		require.NoError(t, os.WriteFile(name, []byte("SEED DATA"), 0644))
		assert.True(t, state.changed(name))
	})
	t.Run("replaced", func(t *testing.T) {
		name := filepath.Join(dir, "replaced")
		writeOldFile(t, name, []byte("seed data"))
		state := record(t, name)
		other := filepath.Join(dir, "other")
		writeOldFile(t, other, []byte("seed data"))
		require.NoError(t, os.Chtimes(other, state.info.ModTime(), state.info.ModTime()))
		require.NoError(t, os.Rename(other, name))
		assert.True(t, state.changed(name), "same size and time, different file")
	})
	t.Run("removed", func(t *testing.T) {
		name := filepath.Join(dir, "removed")
		writeOldFile(t, name, []byte("seed data"))
		state := record(t, name)
		require.NoError(t, os.Remove(name))
		assert.True(t, state.changed(name))
	})
	t.Run("modified just before", func(t *testing.T) {
		// A write within the time resolution of the filesystem might not
		// change the modification time
		name := filepath.Join(dir, "recent")
		require.NoError(t, os.WriteFile(name, []byte("seed data"), 0644))
		assert.True(t, record(t, name).changed(name))
	})
}

// hookStore calls a function before handing out a chunk.
type hookStore struct {
	Store
	hook func()
}

func (s hookStore) GetChunk(id ChunkID) (*Chunk, error) {
	s.hook()
	return s.Store.GetChunk(id)
}

// A seed that's written to during assembly is copied into the target in its
// new state. The damaged chunks are found after assembly and taken from the
// store instead.
func TestAssembleSeedChangedDuringAssembly(t *testing.T) {
	chunks := randomChunks(1024, 768)
	fromStore, fromSeed := chunks[0], chunks[1]

	// The store chunk comes first. With one worker it's written before the
	// seed chunk, and fetching it overwrites the seed with other data of the
	// same size.
	setup := func(t *testing.T, store Store) (string, *ExtractStats, error) {
		t.Helper()
		dir := t.TempDir()
		seedFile := filepath.Join(dir, "seed")
		writeOldFile(t, seedFile, fromSeed.data)
		seed, err := NewFileSeed(filepath.Join(dir, "target"), seedFile, chunkIndex(fromSeed))
		require.NoError(t, err)

		// The hook runs on a worker, it can't stop the test
		var once sync.Once
		hooked := hookStore{Store: store, hook: func() {
			once.Do(func() {
				assert.NoError(t, os.WriteFile(seedFile, make([]byte, len(fromSeed.data)), 0644))
			})
		}}
		target := filepath.Join(dir, "target")
		stats, err := AssembleFile(context.Background(), target, chunkIndex(fromStore, fromSeed), hooked,
			[]Seed{seed}, AssembleOptions{N: 1, InvalidSeedAction: InvalidSeedActionBailOut})
		return target, stats, err
	}

	t.Run("backfilled from the store", func(t *testing.T) {
		target, stats, err := setup(t, chunkStore(fromStore, fromSeed))
		require.NoError(t, err)
		assert.Equal(t, uint64(1), stats.ChunksFromSeeds)
		assert.Equal(t, uint64(1), stats.ChunksBackfilled)

		got, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, chunkContent(fromStore, fromSeed), got)
	})

	t.Run("not in the store", func(t *testing.T) {
		_, _, err := setup(t, chunkStore(fromStore))
		require.ErrorContains(t, err, "came from a seed that changed during assembly")
	})
}

// Without a change, the output isn't checked again.
func TestAssembleSeedUnchanged(t *testing.T) {
	chunks := randomChunks(1024, 768)
	fromStore, fromSeed := chunks[0], chunks[1]
	dir := t.TempDir()
	seedFile := filepath.Join(dir, "seed")
	writeOldFile(t, seedFile, fromSeed.data)
	target := filepath.Join(dir, "target")
	seed, err := NewFileSeed(target, seedFile, chunkIndex(fromSeed))
	require.NoError(t, err)

	// Checking the output would fetch nothing, so count the chunks the
	// store hands out instead: only the one assembly needs.
	var fetched atomic.Int32
	store := hookStore{Store: chunkStore(fromStore, fromSeed), hook: func() { fetched.Add(1) }}
	stats, err := AssembleFile(context.Background(), target, chunkIndex(fromStore, fromSeed), store,
		[]Seed{seed}, testAssembleOptions)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), stats.ChunksBackfilled)
	assert.Equal(t, int32(1), fetched.Load())
}
