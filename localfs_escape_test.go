//go:build !windows

package desync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if err := os.WriteFile(sentinel, []byte(orig), 0644); err != nil {
		t.Fatal(err)
	}

	fs := newTestLocalFS(root)
	defer fs.Close()

	if err := fs.CreateSymlink(NodeSymlink{Name: "evil", Target: outside}); err != nil {
		t.Fatalf("CreateSymlink: %v", err)
	}

	if err := fs.CreateFile(NodeFile{Name: "evil/passwd", Data: strings.NewReader("PWNED")}); err == nil {
		t.Fatal("CreateFile through escaping symlink succeeded, want error")
	}

	got, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != orig {
		t.Fatalf("file outside root was modified: got %q, want %q", got, orig)
	}
}

// TestLocalFSLexicalEscape covers the plain ".." traversal at the LocalFS
// layer (independent of the decoder hardening).
func TestLocalFSLexicalEscape(t *testing.T) {
	root := t.TempDir()
	fs := newTestLocalFS(root)
	defer fs.Close()

	if err := fs.CreateFile(NodeFile{Name: "../escape", Data: strings.NewReader("x")}); err == nil {
		t.Error("CreateFile(../escape) succeeded, want error")
	}
	if err := fs.CreateDir(NodeDirectory{Name: "../evildir"}); err == nil {
		t.Error("CreateDir(../evildir) succeeded, want error")
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(root), "escape")); !os.IsNotExist(err) {
		t.Errorf("escape file created outside root: err=%v", err)
	}
}

// TestLocalFSAbsoluteSymlinkTargetVerbatim confirms an absolute symlink target
// is created verbatim (legitimate, matches GNU tar/casync) but cannot be
// followed during extraction.
func TestLocalFSAbsoluteSymlinkTargetVerbatim(t *testing.T) {
	root := t.TempDir()
	fs := newTestLocalFS(root)
	defer fs.Close()

	if err := fs.CreateSymlink(NodeSymlink{Name: "abs", Target: "/etc"}); err != nil {
		t.Fatalf("CreateSymlink: %v", err)
	}
	tgt, err := os.Readlink(filepath.Join(root, "abs"))
	if err != nil {
		t.Fatal(err)
	}
	if tgt != "/etc" {
		t.Fatalf("symlink target = %q, want /etc (verbatim)", tgt)
	}

	if err := fs.CreateFile(NodeFile{Name: "abs/desync-should-not-exist", Data: strings.NewReader("x")}); err == nil {
		t.Fatal("write through absolute symlink succeeded, want error")
	}
	if _, err := os.Lstat("/etc/desync-should-not-exist"); !os.IsNotExist(err) {
		t.Fatalf("file created under /etc: err=%v", err)
	}
}

// TestLocalFSBenignRelativeSymlink is a regression guard: a relative symlink
// that stays within the root must still be followed so legitimate archives
// continue to extract correctly.
func TestLocalFSBenignRelativeSymlink(t *testing.T) {
	root := t.TempDir()
	fs := newTestLocalFS(root)
	defer fs.Close()

	if err := fs.CreateDir(NodeDirectory{Name: "sub"}); err != nil {
		t.Fatalf("CreateDir: %v", err)
	}
	if err := fs.CreateSymlink(NodeSymlink{Name: "link", Target: "sub"}); err != nil {
		t.Fatalf("CreateSymlink: %v", err)
	}
	if err := fs.CreateFile(NodeFile{Name: "link/g", Data: strings.NewReader("hello")}); err != nil {
		t.Fatalf("CreateFile through in-root symlink: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "sub", "g"))
	if err != nil {
		t.Fatalf("expected file via in-root symlink: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("content = %q, want hello", got)
	}
}
