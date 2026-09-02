//go:build !windows

package desync

import (
	"errors"
	"testing"

	"github.com/pkg/xattr"
	"github.com/stretchr/testify/assert"
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
