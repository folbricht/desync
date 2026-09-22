package main

import (
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Commands that write an index do so after chunking and uploading, so a name
// the destination can't represent has to be caught before that work.
func TestValidateIndexLocation(t *testing.T) {
	for _, location := range []string{
		"index.caibx",
		"/tmp/index.caibx",
		"oci+https://ghcr.io/user/repo/index.caibx",
		"s3+https://host/bucket/.hidden.caibx",
		"https://host/path/.hidden.caibx",
	} {
		assert.NoError(t, validateIndexLocation(location), location)
	}
	for _, location := range []string{
		"oci+https://ghcr.io/user/repo/.hidden.caibx",
		"oci+http://127.0.0.1:5000/user/repo/with space.caibx",
	} {
		assert.Error(t, validateIndexLocation(location), location)
	}
}

// With an adaptive concurrency, requests to remote stores go through an
// adaptive limiter. Local stores and stores opened for anything other than
// transfers, like pruning, are left alone.
func TestTransferStoreFromLocation(t *testing.T) {
	var cmdOpt cmdStoreOptions
	cmd := newTestOptionsCommand(&cmdOpt)
	cmd.SetArgs([]string{"-n", "-1"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	s, err := transferStoreFromLocation("http://localhost/store/", cmdOpt)
	require.NoError(t, err)
	assert.IsType(t, &desync.LimitedWriteStore{}, s)

	s, err = storeFromLocation("http://localhost/store/", cmdOpt)
	require.NoError(t, err)
	assert.IsType(t, &desync.RemoteHTTP{}, s)

	// On Windows, local stores come wrapped in a dedup queue
	s, err = transferStoreFromLocation(t.TempDir(), cmdOpt)
	require.NoError(t, err)
	switch s.(type) {
	case desync.LocalStore, *desync.WriteDedupQueue:
	default:
		assert.Failf(t, "local store was wrapped", "got %T", s)
	}

	// Without an adaptive concurrency, nothing is wrapped
	cmd = newTestOptionsCommand(&cmdOpt)
	cmd.SetArgs([]string{"-n", "10"})
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
	s, err = transferStoreFromLocation("http://localhost/store/", cmdOpt)
	require.NoError(t, err)
	assert.IsType(t, &desync.RemoteHTTP{}, s)
}

// When an adaptive store raised the number of workers above the concurrency
// on the command line, remote stores with a fixed concurrency are held to it.
func TestTransferStoreFromLocationFixedLimit(t *testing.T) {
	var cmdOpt cmdStoreOptions
	cmd := newTestOptionsCommand(&cmdOpt)
	cmd.SetArgs([]string{"-n", "10"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	// As many workers as the command line asked for, nothing to limit
	cmdOpt.workers = 10
	s, err := transferStoreFromLocation("http://localhost/store/", cmdOpt)
	require.NoError(t, err)
	assert.IsType(t, &desync.RemoteHTTP{}, s)

	// More workers than that
	cmdOpt.workers = desync.MaxAdaptiveConcurrency
	s, err = transferStoreFromLocation("http://localhost/store/", cmdOpt)
	require.NoError(t, err)
	assert.IsType(t, &desync.LimitedWriteStore{}, s)

	// Local stores aren't limited either way
	s, err = transferStoreFromLocation(t.TempDir(), cmdOpt)
	require.NoError(t, err)
	switch s.(type) {
	case desync.LocalStore, *desync.WriteDedupQueue:
	default:
		assert.Failf(t, "local store was wrapped", "got %T", s)
	}
}
