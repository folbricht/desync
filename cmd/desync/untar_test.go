// +build !windows

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
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

// Check that we repair broken chunks in chache
func TestUntarCommandRepair(t *testing.T) {
	// Create an output dir to extract into
	out, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(out)

	// Create cache with invalid chunk
	cache, err := ioutil.TempDir("", "brokencache")
	require.NoError(t, err)
	defer os.RemoveAll(cache)

	chunkId := "0589328ff916d08f5fe59a9aa0731571448e91341f37ca5484a85b9f0af14de3"
	badChunkHash := "c672b8d1ef56ed28ab87c3622c5114069bdd3ad7b8f9737498d0c01ecef0967a"
	err = os.Mkdir(path.Join(cache, chunkId[:4]), os.ModePerm)
	require.NoError(t, err)
	err = ioutil.WriteFile(path.Join(cache, chunkId[:4], chunkId+".cacnk"), []byte("42"), os.ModePerm)
	require.NoError(t, err)

	// Run "untar" with "--repair=false" -> get error
	cmd := newUntarCommand(context.Background())
	cmd.SetArgs([]string{"-s", "testdata/tree.store", "-c", cache, "--cache-repair=false", "-i", "--no-same-owner", "--no-same-permissions", "testdata/tree.caidx", out})
	_, err = cmd.ExecuteC()
	require.EqualError(t, err, fmt.Sprintf("chunk id %s does not match its hash %s", chunkId, badChunkHash))

	// Now run "untar" with "--repair=true" -> no error
	cmd = newUntarCommand(context.Background())
	cmd.SetArgs([]string{"-s", "testdata/tree.store", "-c", cache, "--cache-repair=true", "-i", "--no-same-owner", "--no-same-permissions", "testdata/tree.caidx", out})
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
}
