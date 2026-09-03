package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMtreeCommand(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"local directory", []string{"testdata"}},
		{"catar archive", []string{"testdata/tree.catar"}},
		{"index", []string{"-s", "testdata/tree.store", "-i", "testdata/tree.caidx"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := new(bytes.Buffer)
			oldStdout := stdout
			t.Cleanup(func() { stdout = oldStdout })
			stdout = b

			cmd := newMtreeCommand(context.Background())
			cmd.SetArgs(test.args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			require.True(t, strings.HasPrefix(b.String(), "#mtree v1.0\n"))
			require.Greater(t, strings.Count(b.String(), "\n"), 1, "no entries were written")
		})
	}
}

// Walking a directory runs the tar half in a goroutine. When the untar half
// stops first, as a cancelled context makes it, the two must not both touch
// the error it reports.
func TestMtreeCommandCancelled(t *testing.T) {
	oldStdout := stdout
	t.Cleanup(func() { stdout = oldStdout })
	stdout = io.Discard

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := newMtreeCommand(ctx)
	cmd.SetArgs([]string{"testdata"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	_, err := cmd.ExecuteC()
	require.Error(t, err)
}

// An index is a location like any other, so it can come from a store or from
// STDIN, the way every other command that reads one accepts them.
func TestMtreeCommandIndexLocations(t *testing.T) {
	addr, cancel := startIndexServer(t, "-s", "testdata")
	defer cancel()

	run := func(t *testing.T, setup func(t *testing.T), input string) {
		t.Helper()
		b := new(bytes.Buffer)
		oldStdout := stdout
		t.Cleanup(func() { stdout = oldStdout })
		stdout = b
		if setup != nil {
			setup(t)
		}

		cmd := newMtreeCommand(context.Background())
		cmd.SetArgs([]string{"-s", "testdata/tree.store", "-i", input})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		_, err := cmd.ExecuteC()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(b.String(), "#mtree v1.0\n"))
		require.Greater(t, strings.Count(b.String(), "\n"), 1, "no entries were written")
	}

	t.Run("from an index server", func(t *testing.T) {
		run(t, nil, fmt.Sprintf("http://%s/tree.caidx", addr))
	})

	t.Run("from STDIN", func(t *testing.T) {
		run(t, func(t *testing.T) {
			f, err := os.Open("testdata/tree.caidx")
			require.NoError(t, err)
			oldStdin := os.Stdin
			t.Cleanup(func() { os.Stdin = oldStdin; f.Close() })
			os.Stdin = f
		}, "-")
	})
}

// Nothing should reach stdout when the input can't be read, least of all a
// header with no entries under it.
func TestMtreeCommandMissingInput(t *testing.T) {
	b := new(bytes.Buffer)
	oldStdout := stdout
	t.Cleanup(func() { stdout = oldStdout })
	stdout = b

	cmd := newMtreeCommand(context.Background())
	cmd.SetArgs([]string{"-s", "testdata/tree.store", "-i", "testdata/does-not-exist.caidx"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	_, err := cmd.ExecuteC()
	require.Error(t, err)
	require.Empty(t, b.String())
}
