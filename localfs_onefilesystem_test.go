//go:build !windows

package desync

import (
	"io"
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

// TestLocalFSOneFileSystemSkipsMount reads a tree that has another filesystem
// mounted in the middle of it with OneFileSystem set. The mount point and
// everything below it is left out, while everything around it, including what
// comes after it in the walk, has to be returned.
func TestLocalFSOneFileSystemSkipsMount(t *testing.T) {
	src := t.TempDir()
	for _, d := range []string{"keep", "mnt/sub", "zzz"} {
		require.NoError(t, os.MkdirAll(filepath.Join(src, d), 0755))
	}
	for _, f := range []string{"keep/a", "mnt/file", "mnt/sub/deep", "zfile", "zzz/b"} {
		require.NoError(t, os.WriteFile(filepath.Join(src, f), []byte(f), 0644))
	}

	// Mounting needs privileges, report the mount point and everything below
	// it as being on another device instead.
	var mounted []os.FileInfo
	require.NoError(t, filepath.Walk(filepath.Join(src, "mnt"), func(_ string, info os.FileInfo, err error) error {
		mounted = append(mounted, info)
		return err
	}))
	lfs := NewLocalFS(src, LocalFSOptions{OneFileSystem: true})
	lfs.deviceOf = func(info os.FileInfo) (uint64, bool) {
		for _, m := range mounted {
			if os.SameFile(info, m) {
				return 2, true
			}
		}
		return 1, true
	}

	var paths []string
	for {
		f, err := lfs.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NoError(t, f.Close())
		rel, err := filepath.Rel(src, f.Path)
		require.NoError(t, err)
		paths = append(paths, rel)
	}
	assert.Equal(t, []string{".", "keep", "keep/a", "zfile", "zzz", "zzz/b"}, paths)
}
