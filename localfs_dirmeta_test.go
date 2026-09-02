//go:build !windows

package desync

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeWritableOnCleanup ensures a tree containing read-only directories can be
// removed again when the test ends.
func makeWritableOnCleanup(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				_ = os.Chmod(path, 0755)
			}
			return nil
		})
	})
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
	lfs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer lfs.Close()
	require.NoError(t, UnTar(context.Background(), b, lfs))

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

// TestLocalFSDirModeWhilePopulated confirms a directory is never more
// permissive than the archive says, not even in the window between its
// creation and the point where it's complete. Only the permissions needed to
// write the contents are added.
func TestLocalFSDirModeWhilePopulated(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "out")
	makeWritableOnCleanup(t, dst)

	lfs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer lfs.Close()

	require.NoError(t, lfs.CreateDir(NodeDirectory{Name: ".", Mode: 0755 | os.ModeDir}))
	require.NoError(t, lfs.CreateDir(NodeDirectory{Name: "priv", Mode: 0700 | os.ModeDir}))
	require.NoError(t, lfs.CreateDir(NodeDirectory{Name: "priv/ro", Mode: 0500 | os.ModeDir}))

	// Not world-readable at any point, and 0500 only gained what it takes to
	// write into it
	info, err := os.Stat(filepath.Join(dst, "priv"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0700), info.Mode().Perm(), "mode of priv while being populated")

	info, err = os.Stat(filepath.Join(dst, "priv/ro"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0700), info.Mode().Perm(), "mode of priv/ro while being populated")

	require.NoError(t, lfs.CreateFile(NodeFile{Name: "priv/ro/file", Mode: 0600, Data: strings.NewReader("content")}))
	require.NoError(t, lfs.Finalize())

	for d, want := range map[string]os.FileMode{"priv": 0700, "priv/ro": 0500} {
		info, err := os.Stat(filepath.Join(dst, d))
		require.NoError(t, err)
		assert.Equal(t, want, info.Mode().Perm(), "final mode of %s", d)
	}
}

// TestLocalFSDirSetgidWhilePopulated confirms the setgid bit is in place while
// the contents of a directory are written, so they inherit its group.
func TestLocalFSDirSetgidWhilePopulated(t *testing.T) {
	// Some build sandboxes prevent us from setting setgid bit
	probe := filepath.Join(t.TempDir(), "setgid")
	require.NoError(t, os.Mkdir(probe, 0755))
	if err := os.Chmod(probe, 0775|os.ModeSetgid); err != nil {
		t.Skipf("setgid on a directory is not permitted here: %v", err)
	}
	// Some environments do not preserve setgid, e.g. macOS with TMPDIR=/tmp
	info, err := os.Stat(probe)
	require.NoError(t, err)
	if info.Mode()&os.ModeSetgid == 0 {
		t.Skip("setgid on a directory is not preserved here")
	}

	dst := filepath.Join(t.TempDir(), "out")

	lfs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer lfs.Close()

	require.NoError(t, lfs.CreateDir(NodeDirectory{Name: ".", Mode: 0755 | os.ModeDir}))
	require.NoError(t, lfs.CreateDir(NodeDirectory{Name: "shared", Mode: 0775 | os.ModeDir | os.ModeSetgid}))

	info, err = os.Stat(filepath.Join(dst, "shared"))
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSetgid, "setgid while the directory is populated")

	require.NoError(t, lfs.Finalize())

	info, err = os.Stat(filepath.Join(dst, "shared"))
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSetgid, "setgid after finalizing")
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
	lfs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true, NoSamePermissions: true})
	defer lfs.Close()
	require.NoError(t, UnTar(context.Background(), b, lfs))

	// Writable by the current user, i.e. not what the archive says
	for _, d := range []string{"ro", "ro/sub"} {
		info, err := os.Stat(filepath.Join(dst, d))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0700), info.Mode().Perm()&0700, "mode of dir %s", d)
	}
}

// TestUnTarNoSamePermissionsOverReadOnlyTree extracts over a tree that was
// written by an earlier extraction and contains read-only directories.
func TestUnTarNoSamePermissionsOverReadOnlyTree(t *testing.T) {
	src := t.TempDir()
	makeWritableOnCleanup(t, src)

	require.NoError(t, os.MkdirAll(filepath.Join(src, "ro/sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "ro/sub/file"), []byte("content"), 0644))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro/sub"), 0555))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro"), 0555))

	dst := filepath.Join(t.TempDir(), "out")
	makeWritableOnCleanup(t, dst)

	// First pass, applying the archive permissions
	first := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	require.NoError(t, UnTar(context.Background(), tarDir(t, src), first))
	require.NoError(t, first.Close())

	// Second pass over the now read-only tree, ignoring the archive permissions
	second := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true, NoSamePermissions: true})
	defer second.Close()
	require.NoError(t, UnTar(context.Background(), tarDir(t, src), second))

	content, err := os.ReadFile(filepath.Join(dst, "ro/sub/file"))
	require.NoError(t, err)
	assert.Equal(t, "content", string(content))
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

	lfs := NewLocalFS(dst, LocalFSOptions{NoSameOwner: true})
	defer lfs.Close()
	require.NoError(t, UnTar(context.Background(), b, lfs))

	content, err := os.ReadFile(filepath.Join(dst, "file"))
	require.NoError(t, err)
	assert.Equal(t, "content", string(content))
}
