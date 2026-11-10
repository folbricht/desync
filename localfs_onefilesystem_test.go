//go:build !windows

package desync

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLocalFSOneFileSystemStatError walks a tree with an entry that can't be
// stat'ed while OneFileSystem is set. The walk has no file info to offer for
// such an entry, only the error, which Next() has to return.
func TestLocalFSOneFileSystemStatError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can stat the entries of any directory")
	}
	root := t.TempDir()
	makeWritableOnCleanup(t, root)

	dir := filepath.Join(root, "dir")
	require.NoError(t, os.Mkdir(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file"), nil, 0644))
	// Without the search permission, the entries of a directory can still be
	// listed, but not stat'ed.
	require.NoError(t, os.Chmod(dir, 0400))

	lfs := NewLocalFS(root, LocalFSOptions{OneFileSystem: true})
	var err error
	for err == nil {
		var f *File
		if f, err = lfs.Next(); err == nil {
			require.NoError(t, f.Close())
		}
	}
	require.ErrorIs(t, err, fs.ErrPermission)
}

// TestLocalFSOneFileSystemSkipsMount archives a tree that has another
// filesystem mounted in the middle of it with OneFileSystem set. The mount
// point and everything below it is left out, while everything around it,
// including what comes after it in the walk, has to be in the archive.
func TestLocalFSOneFileSystemSkipsMount(t *testing.T) {
	src := t.TempDir()
	for _, d := range []string{"keep", "mnt/sub", "zzz"} {
		require.NoError(t, os.MkdirAll(filepath.Join(src, d), 0755))
	}
	for _, f := range []string{"keep/a", "mnt/file", "mnt/sub/deep", "zfile", "zzz/b"} {
		require.NoError(t, os.WriteFile(filepath.Join(src, f), []byte(f), 0644))
	}

	// Mounting needs privileges, report the directories below the mount
	// point as being on another device instead.
	original := deviceOf
	t.Cleanup(func() { deviceOf = original })
	deviceOf = func(info os.FileInfo) (uint64, bool) {
		if info.Name() == "mnt" || info.Name() == "sub" {
			return 2, true
		}
		return 1, true
	}

	b := new(bytes.Buffer)
	require.NoError(t, Tar(context.Background(), b, NewLocalFS(src, LocalFSOptions{OneFileSystem: true})))

	dst := filepath.Join(t.TempDir(), "out")
	lfs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer lfs.Close()
	require.NoError(t, UnTar(context.Background(), b, lfs))

	for _, f := range []string{"keep/a", "zfile", "zzz/b"} {
		content, err := os.ReadFile(filepath.Join(dst, f))
		require.NoError(t, err)
		assert.Equal(t, f, string(content))
	}
	assert.NoDirExists(t, filepath.Join(dst, "mnt"))
}
