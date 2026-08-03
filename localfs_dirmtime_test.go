package desync

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tarDir archives a directory tree into a catar in memory.
func tarDir(t *testing.T, dir string) *bytes.Buffer {
	t.Helper()
	b := new(bytes.Buffer)
	require.NoError(t, Tar(context.Background(), b, NewLocalFS(dir, LocalFSOptions{})))
	return b
}

// TestUnTarDirMTime confirms that directory timestamps survive the writing of
// their contents, which would otherwise update them. Runs on all platforms
// since the deferral this relies on has to work with either path separator.
func TestUnTarDirMTime(t *testing.T) {
	src := t.TempDir()

	// Deep enough for the decoder to walk back up more than one level
	require.NoError(t, os.MkdirAll(filepath.Join(src, "dir/sub/deep"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "dir/sub/file"), []byte("content"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "dir/file"), []byte("content"), 0644))

	mtime := time.Unix(1577872800, 0) // 2020-01-01
	names := []string{"dir/sub/deep", "dir/sub/file", "dir/sub", "dir/file", "dir", "."}
	for _, name := range names {
		require.NoError(t, os.Chtimes(filepath.Join(src, name), mtime, mtime))
	}

	b := tarDir(t, src)

	dst := filepath.Join(t.TempDir(), "out")
	fs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer fs.Close()
	require.NoError(t, UnTar(context.Background(), b, fs))

	for _, name := range names {
		info, err := os.Stat(filepath.Join(dst, name))
		require.NoError(t, err)
		assert.Equal(t, mtime.Unix(), info.ModTime().Unix(), "mtime of %s", name)
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		dir, name string
		want      bool
	}{
		{".", "dir", true},
		{".", "dir/sub", true},
		{"dir", "dir/sub", true},
		{"dir", "dir/sub/file", true},
		{"dir", "dir", false},
		{"dir", "dirx/file", false},
		{"dir/sub", "dir/file", false},
		{"dir/sub", "other", false},
	}
	for _, test := range tests {
		assert.Equal(t, test.want, contains(test.dir, test.name), "contains(%q, %q)", test.dir, test.name)
	}
}
