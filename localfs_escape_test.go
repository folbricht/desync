//go:build !windows

package desync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLocalFS(root string) *LocalFS {
	// NoSameOwner/NoSamePermissions keep the tests runnable as a non-root
	// user; the escape protection under test lives in the os.Root-backed
	// Create* methods, not in the permission handling.
	return NewLocalFS(root, LocalFSOptions{NoSameOwner: true, NoSamePermissions: true})
}

// TestLocalFSSymlinkWriteEscape reproduces the reported attack: an archive
// plants a symlink inside the extraction root pointing outside it, then a
// later entry with a clean name ("evil/passwd", no "..") writes through it.
// The write must be refused and the out-of-root file left untouched.
func TestLocalFSSymlinkWriteEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "passwd")
	const orig = "original-contents\n"
	require.NoError(t, os.WriteFile(sentinel, []byte(orig), 0644))

	fs := newTestLocalFS(root)
	defer fs.Close()

	require.NoError(t, fs.CreateSymlink(NodeSymlink{Name: "evil", Target: outside}))

	require.Error(t, fs.CreateFile(NodeFile{Name: "evil/passwd", Data: strings.NewReader("PWNED")}),
		"CreateFile through escaping symlink succeeded, want error")

	got, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, orig, string(got), "file outside root was modified")
}

// TestLocalFSLexicalEscape covers the plain ".." traversal at the LocalFS
// layer (independent of the decoder hardening).
func TestLocalFSLexicalEscape(t *testing.T) {
	root := t.TempDir()
	fs := newTestLocalFS(root)
	defer fs.Close()

	assert.Error(t, fs.CreateFile(NodeFile{Name: "../escape", Data: strings.NewReader("x")}),
		"CreateFile(../escape) succeeded, want error")
	assert.Error(t, fs.CreateDir(NodeDirectory{Name: "../evildir"}),
		"CreateDir(../evildir) succeeded, want error")
	_, err := os.Lstat(filepath.Join(filepath.Dir(root), "escape"))
	assert.True(t, os.IsNotExist(err), "escape file created outside root: err=%v", err)
}

// TestLocalFSAbsoluteSymlinkTargetVerbatim confirms an absolute symlink target
// is created verbatim (legitimate, matches GNU tar/casync) but cannot be
// followed during extraction.
func TestLocalFSAbsoluteSymlinkTargetVerbatim(t *testing.T) {
	root := t.TempDir()
	fs := newTestLocalFS(root)
	defer fs.Close()

	require.NoError(t, fs.CreateSymlink(NodeSymlink{Name: "abs", Target: "/etc"}))
	tgt, err := os.Readlink(filepath.Join(root, "abs"))
	require.NoError(t, err)
	require.Equal(t, "/etc", tgt, "symlink target should be created verbatim")

	require.Error(t, fs.CreateFile(NodeFile{Name: "abs/desync-should-not-exist", Data: strings.NewReader("x")}),
		"write through absolute symlink succeeded, want error")
	_, err = os.Lstat("/etc/desync-should-not-exist")
	require.True(t, os.IsNotExist(err), "file created under /etc: err=%v", err)
}

// TestLocalFSBenignRelativeSymlink is a regression guard: a relative symlink
// that stays within the root must still be followed so legitimate archives
// continue to extract correctly.
func TestLocalFSBenignRelativeSymlink(t *testing.T) {
	root := t.TempDir()
	fs := newTestLocalFS(root)
	defer fs.Close()

	require.NoError(t, fs.CreateDir(NodeDirectory{Name: "sub"}))
	require.NoError(t, fs.CreateSymlink(NodeSymlink{Name: "link", Target: "sub"}))
	require.NoError(t, fs.CreateFile(NodeFile{Name: "link/g", Data: strings.NewReader("hello")}),
		"CreateFile through in-root symlink")
	got, err := os.ReadFile(filepath.Join(root, "sub", "g"))
	require.NoError(t, err, "expected file via in-root symlink")
	require.Equal(t, "hello", string(got))
}
