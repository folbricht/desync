package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPruneCommand(t *testing.T) {
	// Create a blank store
	store := t.TempDir()

	// Run a "chop" command to populate the store
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// Now prune the store. Using a different index that doesn't have the exact same chunks
	pruneCmd := newPruneCommand(context.Background())
	pruneCmd.SetArgs([]string{"-s", store, "testdata/blob2.caibx", "--yes"})
	_, err = pruneCmd.ExecuteC()
	require.NoError(t, err)
}

// The prompt offers "N" as the default, so an empty line has to decline
// rather than fail, and only an explicit yes may delete anything.
func TestPruneCommandConfirmation(t *testing.T) {
	for _, test := range []struct {
		name   string
		answer string
		pruned bool
	}{
		{name: "enter takes the default", answer: "\n"},
		{name: "no", answer: "n\n"},
		{name: "upper case no", answer: "N\n"},
		{name: "closed input", answer: ""},
		{name: "unanswerable, then closed input", answer: "maybe"},
		{name: "yes", answer: "y\n", pruned: true},
		{name: "yes after an unusable answer", answer: "maybe\ny\n", pruned: true},
		{name: "yes with surrounding space", answer: "  Y  \n", pruned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := chopBlob1Into(t)
			before := countChunks(t, store)

			oldStdin, oldStdout := stdin, stdout
			t.Cleanup(func() { stdin, stdout = oldStdin, oldStdout })
			stdin = strings.NewReader(test.answer)
			stdout = io.Discard

			// blob2 references different chunks, so a prune that goes ahead
			// empties the store built from blob1.
			cmd := newPruneCommand(context.Background())
			cmd.SetArgs([]string{"-s", store, "testdata/blob2.caibx"})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			if test.pruned {
				require.Less(t, countChunks(t, store), before)
			} else {
				require.Equal(t, before, countChunks(t, store))
			}
		})
	}
}

// An index read from STDIN leaves nothing for the prompt to read, so the
// command has to say so instead of asking.
func TestPruneCommandIndexFromStdin(t *testing.T) {
	cmd := newPruneCommand(context.Background())
	cmd.SetArgs([]string{"-s", t.TempDir(), "-"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	_, err := cmd.ExecuteC()
	require.ErrorContains(t, err, "--yes")
}

func chopBlob1Into(t *testing.T) string {
	t.Helper()
	store := t.TempDir()
	cmd := newChopCommand(context.Background())
	cmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)
	return store
}

func countChunks(t *testing.T, store string) int {
	t.Helper()
	var n int
	require.NoError(t, filepath.WalkDir(store, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".cacnk") {
			n++
		}
		return nil
	}))
	return n
}
