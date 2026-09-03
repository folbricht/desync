package main

import (
	"bytes"
	"context"
	"io"
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
