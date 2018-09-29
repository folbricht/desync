package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyCommand(t *testing.T) {
	// Create a blank store
	store, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(store)

	// Run a "chop" command to populate the store
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err = chopCmd.ExecuteC()
	require.NoError(t, err)

	// Place an invalid chunk in the store
	invalidChunkID := "1234567890000000000000000000000000000000000000000000000000000000"
	invalidChunkFile := filepath.Join(store, "1234", invalidChunkID+".cacnk")
	err = os.MkdirAll(filepath.Dir(invalidChunkFile), 0755)
	require.NoError(t, err)
	err = ioutil.WriteFile(invalidChunkFile, []byte("invalid"), 0600)
	require.NoError(t, err)

	// Now run verify on the store. There should be an invalid one in there that should
	// be reported by not removed (without -r).
	verifyCmd := newVerifyCommand(context.Background())
	verifyCmd.SetArgs([]string{"-s", store})
	b := new(bytes.Buffer)
	stderr = b
	_, err = verifyCmd.ExecuteC()
	require.NoError(t, err)
	require.Contains(t, b.String(), invalidChunkID)

	// Run the verify again, this time dropping the bad chunk(s)
	verifyCmd = newVerifyCommand(context.Background())
	verifyCmd.SetArgs([]string{"-s", store, "-r"})
	b = new(bytes.Buffer)
	stderr = b
	_, err = verifyCmd.ExecuteC()
	require.NoError(t, err)
	require.Contains(t, b.String(), invalidChunkID)

	// Confirm sure the bad chunk file is gone from the store
	_, err = os.Stat(invalidChunkFile)
	require.True(t, os.IsNotExist(err))
}
