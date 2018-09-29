// +build !windows

package main

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUntarCommandArchive(t *testing.T) {
	// Create an output dir to extract into
	out, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(out)

	// Run "untar" command to unpack an archive
	cmd := newUntarCommand(context.Background())
	cmd.SetArgs([]string{"--no-same-owner", "--no-same-permissions", "testdata/tree.catar", out})
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
}

func TestUntarCommandIndex(t *testing.T) {
	// Create an output dir to extract into
	out, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(out)

	// Run "untar" to extract from a caidx index
	cmd := newUntarCommand(context.Background())
	cmd.SetArgs([]string{"-s", "testdata/tree.store", "-i", "--no-same-owner", "--no-same-permissions", "testdata/tree.caidx", out})
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
}
