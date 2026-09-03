//go:build !windows

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUntarCommandArchive(t *testing.T) {
	// Create an output dir to extract into
	out := t.TempDir()

	// Run "untar" command to unpack an archive
	cmd := newUntarCommand(context.Background())
	cmd.SetArgs([]string{"--no-same-owner", "--no-same-permissions", "testdata/tree.catar", out})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)
}

func TestUntarCommandIndex(t *testing.T) {
	// Create an output dir to extract into
	out := t.TempDir()

	// Run "untar" to extract from a caidx index
	cmd := newUntarCommand(context.Background())
	cmd.SetArgs([]string{"-s", "testdata/tree.store", "-i", "--no-same-owner", "--no-same-permissions", "testdata/tree.caidx", out})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)
}

// Check that we repair broken chunks in cache
func TestUntarCommandRepair(t *testing.T) {
	// Create an output dir to extract into
	out := t.TempDir()

	// Create cache with invalid chunk by reading a chunk from another store, and writing it to the cache with the wrong id
	cache := t.TempDir()

	chunkId := "0589328ff916d08f5fe59a9aa0731571448e91341f37ca5484a85b9f0af14de3"
	badChunkHash := "0b2a199263ffb2600b6f8be2e03b7439ffb0ad05a00b867f427a716e3e386c2d"
	err := os.Mkdir(path.Join(cache, chunkId[:4]), os.ModePerm)
	require.NoError(t, err)
	b, err := os.ReadFile(path.Join("testdata/blob1.store", badChunkHash[:4], badChunkHash+".cacnk"))
	require.NoError(t, err)
	err = os.WriteFile(path.Join(cache, chunkId[:4], chunkId+".cacnk"), b, os.ModePerm)
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

// The end-of-archive blocks are written when the tar writer is closed, so an
// archive that is missing them was reported as complete without ever being so.
func TestUntarCommandGnuTar(t *testing.T) {
	out := filepath.Join(t.TempDir(), "tree.tar")

	cmd := newUntarCommand(context.Background())
	cmd.SetArgs([]string{"--output-format", "gnu-tar", "testdata/tree.catar", out})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	b, err := os.ReadFile(out)
	require.NoError(t, err)

	// The archive has to be readable...
	r := tar.NewReader(bytes.NewReader(b))
	var names []string
	for {
		hdr, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		names = append(names, hdr.Name)
	}
	require.NotEmpty(t, names)

	// ...and end with the two zero-filled blocks that mark the end of one.
	// Readers treat those as optional, so nothing above notices them missing.
	require.Greater(t, len(b), 1024)
	require.Equal(t, make([]byte, 1024), b[len(b)-1024:])
}

func TestCloseInto(t *testing.T) {
	closeErr := errors.New("close failed")
	earlier := errors.New("something earlier")

	for _, test := range []struct {
		name  string
		close error
		err   error
		want  error
	}{
		{"a clean close changes nothing", nil, nil, nil},
		{"a failed close is reported", closeErr, nil, closeErr},
		{"an earlier failure is kept", closeErr, earlier, earlier},
		{"a clean close keeps an earlier failure", nil, earlier, earlier},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.err
			closeInto(closerFunc(func() error { return test.close }), &err)
			require.Equal(t, test.want, err)
		})
	}
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }
