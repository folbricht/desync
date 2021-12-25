package main

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVerifyIndexCommand(t *testing.T) {
	// Validate the index of blob1, we expect it to complete without any error
	verifyIndex := newVerifyIndexCommand(context.Background())
	verifyIndex.SetArgs([]string{"testdata/blob1.caibx", "testdata/blob1"})
	b := new(bytes.Buffer)
	stderr = b
	_, err := verifyIndex.ExecuteC()
	require.NoError(t, err)
	require.Contains(t, b.String(), "")

	// Do the same for blob2
	verifyIndex = newVerifyIndexCommand(context.Background())
	verifyIndex.SetArgs([]string{"testdata/blob2.caibx", "testdata/blob2"})
	b = new(bytes.Buffer)
	stderr = b
	_, err = verifyIndex.ExecuteC()
	require.NoError(t, err)
	require.Contains(t, b.String(), "")

	// Run again against the wrong blob
	verifyIndex = newVerifyIndexCommand(context.Background())
	verifyIndex.SetArgs([]string{"testdata/blob2.caibx", "testdata/blob1"})
	_, err = verifyIndex.ExecuteC()
	require.Error(t, err)
}
