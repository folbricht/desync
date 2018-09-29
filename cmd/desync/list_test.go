package main

import (
	"bufio"
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/require"
)

func TestListCommand(t *testing.T) {
	cmd := newListCommand(context.Background())
	cmd.SetArgs([]string{"testdata/blob1.caibx"})
	b := new(bytes.Buffer)

	// Redirect the command's output
	stdout = b
	cmd.SetOutput(ioutil.Discard)
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	// Make sure we have some data, and that it's all valid chunk IDs
	require.NotZero(t, b.Len())
	scanner := bufio.NewScanner(b)
	for scanner.Scan() {
		_, err := desync.ChunkIDFromString(scanner.Text())
		require.NoError(t, err)
	}
	require.NoError(t, scanner.Err())
}
