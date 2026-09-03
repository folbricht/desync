package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/require"
)

func TestChopCommand(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"simple chop",
			[]string{"testdata/blob1.caibx", "testdata/blob1"}},
		{"chop with ignore",
			[]string{"--ignore", "testdata/blob2.caibx", "testdata/blob1.caibx", "testdata/blob1"}},
	} {
		store := t.TempDir()

		args := []string{"-s", store}
		args = append(args, test.args...)

		cmd := newChopCommand(context.Background())
		cmd.SetArgs(args)

		// Redirect the command's output to turn off the progressbar and run it
		stderr = io.Discard
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		_, err := cmd.ExecuteC()
		require.NoError(t, err)

		// If the file was split right, we'll have chunks in the dir now
		dirs, err := os.ReadDir(store)
		require.NoError(t, err)
		require.NotEmpty(t, dirs)
	}
}

func TestChopErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"without store",
			[]string{"testdata/blob1.caibx", "testdata/blob1"}},
		{"invalid store",
			[]string{"-s", filepath.Join(t.TempDir(), "desync"), "testdata/blob1.caibx", "testdata/blob1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newChopCommand(context.Background())
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(test.args)
			_, err := cmd.ExecuteC()
			require.Error(t, err)
		})
	}
}

// ChopFile's workers seek to each chunk's offset, so the order the chunks are
// handed over in is the order the file gets read in. Filtering must not turn
// a sequential read into a random walk over the whole file.
func TestChopChunksToStore(t *testing.T) {
	// The options have to come from a flagset, they're consulted when a store
	// location is resolved.
	var storeOpt cmdStoreOptions
	flags := newTestOptionsCommand(&storeOpt)
	flags.SetArgs(nil)
	_, err := flags.ExecuteC()
	require.NoError(t, err)

	index, err := readCaibxFile("testdata/blob1.caibx", storeOpt)
	require.NoError(t, err)
	require.NotEmpty(t, index.Chunks)

	// Ignore a scattering of chunks, so what's left isn't contiguous
	var b strings.Builder
	for i, c := range index.Chunks {
		if i%3 == 0 {
			fmt.Fprintln(&b, c.ID)
		}
	}
	idFile := filepath.Join(t.TempDir(), "ignore.txt")
	require.NoError(t, os.WriteFile(idFile, []byte(b.String()), 0644))

	opt := chopOptions{cmdStoreOptions: storeOpt, ignoreChunks: []string{idFile}}

	// Map iteration order is randomized per run, so a single pass proves
	// little.
	for range 10 {
		chunks, err := chunksToStore(index.Chunks, opt)
		require.NoError(t, err)
		require.NotEmpty(t, chunks)
		require.Less(t, len(chunks), len(index.Chunks))

		starts := make([]uint64, 0, len(chunks))
		seen := make(map[desync.ChunkID]struct{}, len(chunks))
		for _, c := range chunks {
			starts = append(starts, c.Start)
			_, dup := seen[c.ID]
			require.False(t, dup, "chunk %s handed over more than once", c.ID)
			seen[c.ID] = struct{}{}
		}
		require.IsIncreasing(t, starts, "chunks must be in the order they appear in the index")
	}

	// Without anything to ignore the index is passed through untouched,
	// duplicate chunk IDs and all.
	chunks, err := chunksToStore(index.Chunks, chopOptions{cmdStoreOptions: storeOpt})
	require.NoError(t, err)
	require.Equal(t, index.Chunks, chunks)
}
