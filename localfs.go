package desync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// LocalFS uses the local filesystem for tar/untar operations.
type LocalFS struct {
	// Base directory
	Root string

	opts LocalFSOptions

	dev     uint64
	once    sync.Once
	entries chan walkEntry
	sErr    error

	// Writer side. All write/metadata operations are performed through an
	// os.Root handle which confines them to Root and refuses to follow any
	// symlink (planted by the archive itself) that would escape it.
	wonce    sync.Once
	wroot    *os.Root
	wErr     error
	rootReal string

	// Directories whose metadata (owner, xattrs, permissions and timestamps)
	// hasn't been applied yet because they may still be populated. Held in
	// the order they were created, i.e. a directory is always preceded by its
	// parent.
	pending []NodeDirectory
}

// LocalFSOptions influence the behavior of the filesystem when reading from or writing to it.
type LocalFSOptions struct {
	// Only used when reading from the filesystem. Will only return
	// files from the same device as the first read operation.
	OneFileSystem bool

	// When writing files, use the current owner and don't try to apply the original owner.
	NoSameOwner bool

	// Ignore the incoming permissions when writing files. Use the current default instead.
	NoSamePermissions bool

	// Reads all timestamps as zero. Used in tar operations to avoid unnecessary changes.
	NoTime bool
}

var _ FilesystemWriter = &LocalFS{}
var _ FilesystemReader = &LocalFS{}
var _ FilesystemFinalizer = &LocalFS{}

// writeRoot lazily creates the extraction root directory and opens an os.Root
// handle anchored to it. Every write/metadata operation goes through the
// returned handle so that no path component (including symlinks created earlier
// by the same archive) can be used to escape Root. The handle is opened only
// once; the result (or error) is cached for the lifetime of the LocalFS.
func (fs *LocalFS) writeRoot() (*os.Root, error) {
	fs.wonce.Do(func() {
		if err := os.MkdirAll(fs.Root, 0777); err != nil {
			fs.wErr = err
			return
		}
		// Resolved real path of the root, used for the rare symlink/device
		// xattr fallback that has no fd-based equivalent.
		if real, err := filepath.EvalSymlinks(fs.Root); err == nil {
			fs.rootReal = real
		} else {
			fs.rootReal = fs.Root
		}
		fs.wroot, fs.wErr = os.OpenRoot(fs.Root)
	})
	return fs.wroot, fs.wErr
}

// Close applies any outstanding directory metadata on a best-effort basis and
// releases the os.Root handle used for writing. It is safe to call even if no
// write operation was ever performed. On a successful untar the metadata has
// normally been applied by Finalize() already.
func (fs *LocalFS) Close() error {
	err := fs.Finalize()
	if fs.wroot != nil {
		if cerr := fs.wroot.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// Finalize applies the deferred metadata of all directories that are still
// pending, deepest first. It's called at the end of an untar operation, once
// no further entries can be written. Safe to call more than once.
func (fs *LocalFS) Finalize() error {
	var err error
	for _, n := range slices.Backward(fs.pending) {
		if e := fs.applyDirMetadata(n); e != nil && err == nil {
			err = e
		}
	}
	fs.pending = nil
	return err
}

// contains reports whether name refers to something inside the directory dir.
// Both are clean slash-separated paths relative to the extraction root, with
// "." being the root itself.
func contains(dir, name string) bool {
	return dir == "." || strings.HasPrefix(name, dir+"/")
}

// completeDirs applies the deferred metadata of every pending directory that
// doesn't contain name, deepest first. Those directories are complete since
// entries arrive depth-first.
func (fs *LocalFS) completeDirs(name string) error {
	for len(fs.pending) > 0 {
		n := fs.pending[len(fs.pending)-1]
		if contains(n.Name, name) {
			return nil
		}
		// Only drop it once it's done, so a failure here can still be retried
		// by the best-effort Finalize() in Close().
		if err := fs.applyDirMetadata(n); err != nil {
			return err
		}
		fs.pending = fs.pending[:len(fs.pending)-1]
	}
	return nil
}

// applyDirMetadata sets owner, xattrs, permissions and timestamps of a
// directory that has been fully populated.
func (fs *LocalFS) applyDirMetadata(n NodeDirectory) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}
	if err := fs.SetDirPermissions(n); err != nil {
		return err
	}
	if n.MTime == time.Unix(0, 0) {
		return nil
	}
	return r.Chtimes(n.Name, n.MTime, n.MTime)
}

func (fs *LocalFS) CreateDir(n NodeDirectory) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	// Everything that isn't an ancestor of this new directory is complete now.
	if err := fs.completeDirs(n.Name); err != nil {
		return err
	}

	// Let's see if there is a dir with the same name already
	var (
		created  bool
		existing os.FileMode
	)
	if info, err := r.Lstat(n.Name); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", n.Name)
		}
		existing = info.Mode().Perm()
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		// (n.Name == "." is the extraction root itself, which already exists.)
		// The mode from the archive is the upper bound of what the directory
		// gets here, so its contents are never written into a directory more
		// permissive than the archive says. The exact mode, including any
		// setuid/setgid/sticky bits, is applied by prepareDirWrite below and
		// again by applyDirMetadata once the directory is complete.
		mode := os.FileMode(0777)
		if !fs.opts.NoSamePermissions {
			mode = n.Mode.Perm() | 0700
		}
		if err := r.Mkdir(n.Name, mode); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
		created = true
	}

	fs.prepareDirWrite(r, n, existing, created)

	// The remaining metadata is applied once the directory is complete. Doing
	// it now would not only prevent writing into a read-only directory, it'd
	// also see the timestamps overwritten by those very writes.
	fs.pending = append(fs.pending, n)
	return nil
}

func (fs *LocalFS) CreateFile(n NodeFile) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	if err := fs.completeDirs(n.Name); err != nil {
		return err
	}

	if err := r.RemoveAll(n.Name); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%s: %w", n.Name, err)
	}
	f, err := r.OpenFile(n.Name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("%s: %w", n.Name, err)
	}
	defer f.Close()
	if _, err = io.Copy(f, n.Data); err != nil {
		return err
	}

	if err := fs.SetFilePermissions(n); err != nil {
		return err
	}

	if n.MTime == time.Unix(0, 0) {
		return nil
	}
	return r.Chtimes(n.Name, n.MTime, n.MTime)
}

func (fs *LocalFS) CreateSymlink(n NodeSymlink) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	if err := fs.completeDirs(n.Name); err != nil {
		return err
	}

	if err := r.Remove(n.Name); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%s: %w", n.Name, err)
	}
	// The target is stored verbatim (an archive may legitimately contain
	// absolute or relative symlinks, same as GNU tar/casync). It is never
	// followed during extraction: subsequent operations go through os.Root,
	// which refuses to traverse a symlink that escapes the root.
	if err := r.Symlink(n.Target, n.Name); err != nil {
		return fmt.Errorf("%s: %w", n.Name, err)
	}

	if err := fs.SetSymlinkPermissions(n); err != nil {
		return err
	}

	return nil
}

type walkEntry struct {
	path string
	info os.FileInfo
	err  error
}
