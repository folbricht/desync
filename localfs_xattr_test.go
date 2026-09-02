//go:build !windows

package desync

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/xattr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestXattrUnsupported checks that only a filesystem's lack of extended
// attribute support is waved through, and that a real failure to read them
// still surfaces. Walking a tree on NetBSD's tmpfs used to fail on the first
// entry because the two were not told apart.
func TestXattrUnsupported(t *testing.T) {
	listErr := func(err error) error {
		return &xattr.Error{Op: "xattr.list", Path: "/tmp/x", Err: err}
	}

	assert.True(t, xattrUnsupported(listErr(unix.EOPNOTSUPP)))
	assert.True(t, xattrUnsupported(listErr(unix.ENOTSUP)))

	assert.False(t, xattrUnsupported(listErr(unix.EPERM)))
	assert.False(t, xattrUnsupported(listErr(unix.EACCES)))
	assert.False(t, xattrUnsupported(errors.New("boom")))
	assert.False(t, xattrUnsupported(nil))
}

// TestSkipXattrs covers the decision the two setters share: what to apply,
// what to leave alone, and when to refuse rather than drop attributes an
// archive is carrying. Deliberately free of any actual xattr call, since
// whether one succeeds depends on the filesystem under the test's temp dir.
func TestSkipXattrs(t *testing.T) {
	var (
		none = Xattrs{}
		some = Xattrs{"user.test": "value"}
	)

	t.Run("nothing to apply", func(t *testing.T) {
		fs := NewLocalFS(t.TempDir(), LocalFSOptions{})
		skip, err := fs.skipXattrs("f", none)
		require.NoError(t, err)
		assert.True(t, skip)
	})

	t.Run("opted out", func(t *testing.T) {
		fs := NewLocalFS(t.TempDir(), LocalFSOptions{NoSameXattrs: true})
		skip, err := fs.skipXattrs("f", some)
		require.NoError(t, err)
		assert.True(t, skip, "NoSameXattrs must skip even when the archive carries some")
	})

	t.Run("attributes present", func(t *testing.T) {
		fs := NewLocalFS(t.TempDir(), LocalFSOptions{})
		skip, err := fs.skipXattrs("f", some)
		if xattr.XATTR_SUPPORTED {
			require.NoError(t, err)
			assert.False(t, skip)
		} else {
			// OpenBSD and DragonFly, where pkg/xattr is a set of no-op stubs.
			// Applying them would report success and write nothing.
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not supported on this platform")
		}
	})
}

// TestCreateFileNoSameXattrs checks the flag end to end: a node carrying
// attributes is written without error and without them, on every platform,
// including those where applying them could not have worked.
func TestCreateFileNoSameXattrs(t *testing.T) {
	dir := t.TempDir()
	fs := NewLocalFS(dir, LocalFSOptions{NoSameXattrs: true})
	// Owned by the caller so the chown alongside the xattrs succeeds without
	// root, and NoSameOwner left off because the setters are only reached when
	// ownership is being applied.
	require.NoError(t, fs.CreateFile(NodeFile{
		Name:   "f",
		Mode:   0644,
		UID:    os.Getuid(),
		GID:    os.Getgid(),
		Data:   strings.NewReader("content"),
		Xattrs: Xattrs{"user.test": "value"},
	}))
	require.NoError(t, fs.Close())

	b, err := os.ReadFile(filepath.Join(dir, "f"))
	require.NoError(t, err)
	assert.Equal(t, "content", string(b))
}
