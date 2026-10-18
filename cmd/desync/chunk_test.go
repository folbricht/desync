package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func TestChunkCommandSegmentAwareStart(t *testing.T) {
	b := new(bytes.Buffer)
	oldStdout := stdout
	t.Cleanup(func() { stdout = oldStdout })
	stdout = b

	// Two segments, chunking starts inside the first one
	data, err := os.ReadFile("testdata/blob1")
	require.NoError(t, err)
	segments := filepath.Join(t.TempDir(), "segments.csv")
	require.NoError(t, os.WriteFile(segments, fmt.Appendf(nil, "1000\n%d\n", len(data)-1000), 0644))

	cmd := newChunkCommand(context.Background())
	cmd.SetArgs([]string{"--chunker", "segmentaware", "--chunker-options", "segmentsSource=" + segments, "--start", "500", "testdata/blob1"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	_, err = cmd.ExecuteC()
	require.NoError(t, err)

	// The chunks are contiguous from the starting position to the end of the file
	lines := strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n")
	next := 500
	for _, line := range lines {
		fields := strings.Split(line, "\t")
		require.Len(t, fields, 3, "line %q", line)
		start, err := strconv.Atoi(fields[0])
		require.NoError(t, err)
		require.Equal(t, next, start)
		length, err := strconv.Atoi(fields[1])
		require.NoError(t, err)
		next = start + length
	}
	require.Equal(t, len(data), next)
}
