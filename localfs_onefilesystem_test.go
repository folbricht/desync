//go:build !windows

package desync

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

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
