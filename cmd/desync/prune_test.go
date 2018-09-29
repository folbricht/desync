package main

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPruneCommand(t *testing.T) {
	// Create a blank store
	store, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(store)

	// Run a "chop" command to populate the store
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err = chopCmd.ExecuteC()
	require.NoError(t, err)

	// Now prune the store. Using a different index that doesn't have the exact same chunks
	pruneCmd := newPruneCommand(context.Background())
	pruneCmd.SetArgs([]string{"-s", store, "testdata/blob2.caibx", "--yes"})
	_, err = pruneCmd.ExecuteC()
	require.NoError(t, err)
}
