//go:build !windows

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/require"
)

func TestTarCommandArchive(t *testing.T) {
	// Create an output dir
	out := t.TempDir()
	archive := filepath.Join(out, "tree.catar")

	// Run "tar" command to build the catar archive
	cmd := newTarCommand(context.Background())
	cmd.SetArgs([]string{archive, "testdata/tree"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)
}

func TestTarCommandIndex(t *testing.T) {
	// Create an output dir to function as chunk store and to hold the caidx
	out := t.TempDir()
	index := filepath.Join(out, "tree.caidx")

	// Run "tar" command to build a caidx index and store the chunks
	cmd := newTarCommand(context.Background())
	cmd.SetArgs([]string{"-s", out, "-i", index, "testdata/tree"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)
}

func TestTarCommandIndexSHA256(t *testing.T) {
	old := desync.Digest
	desync.Digest = desync.SHA256{}
	t.Cleanup(func() { desync.Digest = old })

	out := t.TempDir()
	index := filepath.Join(out, "tree.caidx")

	cmd := newTarCommand(context.Background())
	cmd.SetArgs([]string{"-s", out, "-i", index, "testdata/tree"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	// The index must be marked as SHA256 so it can be read back
	f, err := os.Open(index)
	require.NoError(t, err)
	defer f.Close()
	idx, err := desync.IndexFromReader(f)
	require.NoError(t, err)
	require.Zero(t, idx.Index.FeatureFlags&desync.CaFormatSHA512256)
}
