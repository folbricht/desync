//go:build !windows

package desync

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeWritableOnCleanup ensures a tree containing read-only directories can be
// removed again when the test ends.
func makeWritableOnCleanup(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() {
		filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				os.Chmod(path, 0755)
			}
			return nil
		})
	})
}

// tarDir archives a directory tree into a catar in memory.
func tarDir(t *testing.T, dir string) *bytes.Buffer {
	t.Helper()
	b := new(bytes.Buffer)
	require.NoError(t, Tar(context.Background(), b, NewLocalFS(dir, LocalFSOptions{})))
	return b
}

// TestUnTarReadOnlyDir extracts an archive containing read-only directories
// that hold further directories and files. The mode from the archive can only
// be applied once a directory has been populated, see issue #376.
func TestUnTarReadOnlyDir(t *testing.T) {
	src := t.TempDir()
	makeWritableOnCleanup(t, src)

	for _, d := range []string{"ro/sub1", "ro/sub2/deep"} {
		require.NoError(t, os.MkdirAll(filepath.Join(src, d), 0755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(src, "ro/sub1/file"), []byte("content"), 0644))
	require.NoError(t, os.Symlink("file", filepath.Join(src, "ro/sub1/link")))
	// Every directory in the tree is read-only, including the root
	require.NoError(t, os.Chmod(filepath.Join(src, "ro/sub2/deep"), 0555))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro/sub2"), 0555))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro/sub1"), 0555))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro"), 0555))
	require.NoError(t, os.Chmod(src, 0555))

	b := tarDir(t, src)

	dst := filepath.Join(t.TempDir(), "out")
	makeWritableOnCleanup(t, dst)
	fs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer fs.Close()
	require.NoError(t, UnTar(context.Background(), b, fs))

	// The archive permissions need to be in place afterwards
	for _, d := range []string{".", "ro", "ro/sub1", "ro/sub2", "ro/sub2/deep"} {
		info, err := os.Stat(filepath.Join(dst, d))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0555), info.Mode().Perm(), "mode of dir %s", d)
	}

	content, err := os.ReadFile(filepath.Join(dst, "ro/sub1/file"))
	require.NoError(t, err)
	assert.Equal(t, "content", string(content))

	target, err := os.Readlink(filepath.Join(dst, "ro/sub1/link"))
	require.NoError(t, err)
	assert.Equal(t, "file", target)
}

// TestUnTarDirMTime confirms that directory timestamps survive the writing of
// their contents, which would otherwise update them.
func TestUnTarDirMTime(t *testing.T) {
	src := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(src, "dir/sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "dir/file"), []byte("content"), 0644))

	mtime := time.Unix(1577872800, 0) // 2020-01-01
	for _, name := range []string{"dir/file", "dir/sub", "dir", "."} {
		require.NoError(t, os.Chtimes(filepath.Join(src, name), mtime, mtime))
	}

	b := tarDir(t, src)

	dst := filepath.Join(t.TempDir(), "out")
	fs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer fs.Close()
	require.NoError(t, UnTar(context.Background(), b, fs))

	for _, name := range []string{".", "dir", "dir/sub", "dir/file"} {
		info, err := os.Stat(filepath.Join(dst, name))
		require.NoError(t, err)
		assert.Equal(t, mtime.Unix(), info.ModTime().Unix(), "mtime of %s", name)
	}
}

// TestUnTarReadOnlyDirNoSamePermissions extracts the same archive while
// ignoring the permissions in it. That has to work as well, and must not apply
// the archive mode to the directories.
func TestUnTarReadOnlyDirNoSamePermissions(t *testing.T) {
	src := t.TempDir()
	makeWritableOnCleanup(t, src)

	require.NoError(t, os.MkdirAll(filepath.Join(src, "ro/sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "ro/sub/file"), []byte("content"), 0644))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro/sub"), 0555))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro"), 0555))

	b := tarDir(t, src)

	dst := filepath.Join(t.TempDir(), "out")
	fs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true, NoSamePermissions: true})
	defer fs.Close()
	require.NoError(t, UnTar(context.Background(), b, fs))

	for _, d := range []string{"ro", "ro/sub"} {
		info, err := os.Stat(filepath.Join(dst, d))
		require.NoError(t, err)
		assert.NotEqual(t, os.FileMode(0555), info.Mode().Perm(), "mode of dir %s", d)
	}
}

// TestUnTarIntoReadOnlyDir extracts into a target directory that exists
// already and isn't writable.
func TestUnTarIntoReadOnlyDir(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "file"), []byte("content"), 0644))

	b := tarDir(t, src)

	dst := filepath.Join(t.TempDir(), "out")
	makeWritableOnCleanup(t, dst)
	require.NoError(t, os.Mkdir(dst, 0555))

	fs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer fs.Close()
	require.NoError(t, UnTar(context.Background(), b, fs))

	content, err := os.ReadFile(filepath.Join(dst, "file"))
	require.NoError(t, err)
	assert.Equal(t, "content", string(content))
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
