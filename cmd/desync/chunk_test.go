package main

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkCommand(t *testing.T) {
	b := new(bytes.Buffer)
	oldStdout := stdout
	t.Cleanup(func() { stdout = oldStdout })
	stdout = b

	cmd := newChunkCommand(context.Background())
	cmd.SetArgs([]string{"testdata/blob1"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n")
	require.NotEmpty(t, lines)

	// Every line is a start/length/hash triple, and the chunks are contiguous
	// from the starting position.
	var next int
	for _, line := range lines {
		fields := strings.Split(line, "\t")
		require.Len(t, fields, 3, "line %q", line)
		start, err := strconv.Atoi(fields[0])
		require.NoError(t, err)
		require.Equal(t, next, start)
		length, err := strconv.Atoi(fields[1])
		require.NoError(t, err)
		require.NotEmpty(t, fields[2])
		next = start + length
	}
}
