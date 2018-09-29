// +build !windows

package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTarCommandArchive(t *testing.T) {
	// Create an output dir
	out, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(out)
	archive := filepath.Join(out, "tree.catar")

	// Run "tar" command to build the catar archive
	cmd := newTarCommand(context.Background())
	cmd.SetArgs([]string{archive, "testdata/tree"})
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
}

func TestTarCommandIndex(t *testing.T) {
	// Create an output dir to function as chunk store and to hold the caidx
	out, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(out)
	index := filepath.Join(out, "tree.caidx")

	// Run "tar" command to build a caidx index and store the chunks
	cmd := newTarCommand(context.Background())
	cmd.SetArgs([]string{"-s", out, "-i", index, "testdata/tree"})
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
}
