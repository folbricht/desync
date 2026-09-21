//go:build !windows

package desync

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTar(t *testing.T) {
	// First make a tempdir and create a few dirs and files in it
	base := t.TempDir()

	dirs := []string{
		"dir1/sub11",
		"dir1/sub12",
		"dir2/sub21",
		"dir2/sub22",
	}
	for _, d := range dirs {
		require.NoError(t, os.MkdirAll(filepath.Join(base, d), 0755))
	}

	files := []string{
		"dir1/sub11/f11",
		"dir1/sub11/f12",
	}
	for i, name := range files {
		require.NoError(t, os.WriteFile(filepath.Join(base, name), fmt.Appendf(nil, "filecontent%d", i), 0644))
	}

	require.NoError(t, os.Symlink("dir1", filepath.Join(base, "symlink")))

	// Encode it all into a buffer
	fs := NewLocalFS(base, LocalFSOptions{})
	b := new(bytes.Buffer)
	require.NoError(t, Tar(context.Background(), b, fs))

	// Decode it again
	d := NewFormatDecoder(b)

	// Define an array of what is expected in the test file
	expected := []any{
		FormatEntry{},
		FormatFilename{}, // "dir1"
		FormatEntry{},
		FormatFilename{}, // "sub11"
		FormatEntry{},
		FormatFilename{}, // "f11"
		FormatEntry{},
		FormatPayload{},
		FormatFilename{}, // "f12"
		FormatEntry{},
		FormatPayload{},
		FormatGoodbye{},
		FormatFilename{}, // "sub12"
		FormatEntry{},
		FormatGoodbye{},
		FormatGoodbye{},
		FormatFilename{}, // "dir2"
		FormatEntry{},
		FormatFilename{}, // "sub21"
		FormatEntry{},
		FormatGoodbye{},
		FormatFilename{}, // "sub22"
		FormatEntry{},
		FormatGoodbye{},
		FormatGoodbye{},
		FormatFilename{}, // "symlink"
		FormatEntry{},
		FormatSymlink{},
		FormatGoodbye{},
		nil,
	}

	for _, exp := range expected {
		v, err := d.Next()
		require.NoError(t, err)
		require.IsType(t, exp, v)
	}
}

// TestTarDigestFlag verifies every entry in a catar written by Tar records the
// digest algorithm in use.
func TestTarDigestFlag(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "dir"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(base, "dir", "file"), []byte("content"), 0644))

	tests := map[string]struct {
		digest HashAlgorithm
		flag   uint64
	}{
		"sha512-256": {SHA512256{}, CaFormatSHA512256},
		"sha256":     {SHA256{}, 0},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			withDigest(t, test.digest)

			b := new(bytes.Buffer)
			require.NoError(t, Tar(context.Background(), b, NewLocalFS(base, LocalFSOptions{})))

			d := NewFormatDecoder(b)
			var entries int
			for {
				e, err := d.Next()
				require.NoError(t, err)
				if e == nil {
					break
				}
				if entry, ok := e.(FormatEntry); ok {
					assert.Equal(t, test.flag, entry.FeatureFlags&CaFormatSHA512256)
					entries++
				}
			}
			assert.Equal(t, 3, entries)
		})
	}
}
