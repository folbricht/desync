// +build windows

package desync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
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
}

// LocalFSOptions influence the behavior of the filesystem when reading from or writing too it.
type LocalFSOptions struct {
	// Only used when reading from the filesystem. Will only return
	// files from the same device as the first read operation.
	OneFileSystem bool

	// When writing files, use the current owner and don't try to apply the original owner.
	NoSameOwner bool

	// Ignore the incoming permissions when writing files. Use the current default instead.
	NoSamePermissions bool

	// Reads all timestamps as zero. Used in tar operations to avoid unneccessary changes.
	NoTime bool
}

var _ FilesystemWriter = &LocalFS{}
var _ FilesystemReader = &LocalFS{}

// NewLocalFS initializes a new instance of a local filesystem that
// can be used for tar/untar operations.
func NewLocalFS(root string, opts LocalFSOptions) *LocalFS {
	return &LocalFS{
		Root:    root,
		opts:    opts,
		entries: nil,
	}
}

func (fs *LocalFS) CreateDir(n NodeDirectory) error {
	dst := filepath.Join(fs.Root, n.Name)

	// Let's see if there is a dir with the same name already
	if info, err := os.Lstat(dst); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", dst)
		}
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		if err := os.Mkdir(dst, 0777); err != nil {
			return err
		}
	}
	if n.MTime == time.Unix(0, 0) {
		return nil
	}
	return os.Chtimes(dst, n.MTime, n.MTime)
}

func (fs *LocalFS) CreateFile(n NodeFile) error {
	dst := filepath.Join(fs.Root, n.Name)

	if err := os.RemoveAll(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(f, n.Data); err != nil {
		return err
	}
	if n.MTime == time.Unix(0, 0) {
		return nil
	}
	return os.Chtimes(dst, n.MTime, n.MTime)
}

func (fs *LocalFS) CreateSymlink(n NodeSymlink) error {
	dst := filepath.Join(fs.Root, n.Name)

	if err := syscall.Unlink(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Symlink(n.Target, dst); err != nil {
		return err
	}
	return nil
}

func (fs *LocalFS) CreateDevice(n NodeDevice) error {
	return errors.New("Not available on this platform")
}

type walkEntry struct {
	path string
	info os.FileInfo
	err  error
}

// Next returns the next filesystem entry or io.EOF when done. The caller is responsible
// for closing the returned File object.
func (fs *LocalFS) Next() (*File, error) {
	return nil, errors.New("Not available on this platform")
}
