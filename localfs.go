package desync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// Close releases the os.Root handle used for writing. It is safe to call even
// if no write operation was ever performed.
func (fs *LocalFS) Close() error {
	if fs.wroot != nil {
		return fs.wroot.Close()
	}
	return nil
}

func (fs *LocalFS) CreateDir(n NodeDirectory) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	// Let's see if there is a dir with the same name already
	if info, err := r.Lstat(n.Name); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", n.Name)
		}
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		// (n.Name == "." is the extraction root itself, which already exists.)
		if err := r.Mkdir(n.Name, 0777); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
	}

	if err := fs.SetDirPermissions(n); err != nil {
		return err
	}

	if n.MTime == time.Unix(0, 0) {
		return nil
	}
	return r.Chtimes(n.Name, n.MTime, n.MTime)
}

func (fs *LocalFS) CreateFile(n NodeFile) error {
	r, err := fs.writeRoot()
	if err != nil {
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
