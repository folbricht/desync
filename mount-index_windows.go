package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/billziss-gh/cgofuse/fuse"
)

// IndexMountFS is used to FUSE mount an index file (as a blob, not an archive).
// It present a single file underneath the mountpoint.
type IndexMountFS struct {
	fuse.FileSystemBase

	FName string // File name in the mountpoint
	Idx   Index  // Index of the blob
	Store Store

	mu            sync.Mutex
	handles       map[uint64]*indexFileHandle
	handleCounter uint64
}

// NewIndexMountFS initializes a FUSE filesystem mount based on an index and a chunk store.
func NewIndexMountFS(idx Index, name string, s Store) *IndexMountFS {
	return &IndexMountFS{
		FName:   name,
		Idx:     idx,
		Store:   s,
		handles: make(map[uint64]*indexFileHandle),
	}
}

func (fs *IndexMountFS) Open(path string, flags int) (errc int, fh uint64) {
	if path != "/"+fs.FName {
		return -fuse.ENOENT, ^uint64(0)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.handleCounter++
	fs.handles[fs.handleCounter] = newIndexFileHandle(fs.Idx, fs.Store)
	return 0, fs.handleCounter
}

func (fs *IndexMountFS) Release(path string, fh uint64) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, ok := fs.handles[fh]
	if !ok {
		return -fuse.ENOSYS
	}
	delete(fs.handles, fh)
	return 0
}

func (fs *IndexMountFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	switch path {
	case "/":
		stat.Mode = fuse.S_IFDIR | 0555
		return 0
	case "/" + fs.FName:
		stat.Mode = fuse.S_IFREG | 0444
		stat.Size = fs.Idx.Length()
		return 0
	default:
		return -fuse.ENOENT
	}
}

func (fs *IndexMountFS) Read(path string, b []byte, offset int64, fh uint64) (n int) {
	fs.mu.Lock()
	f, ok := fs.handles[fh]
	fs.mu.Unlock()
	if !ok {
		return 0 // apparently this means error??? The method has no dedicated error
	}
	n, err := f.read(b, offset)
	if err != nil { // don't ignore the error
		fmt.Fprintf(os.Stderr, "error reading: %v", err)
	}
	return n
}

func (fs *IndexMountFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, offset int64, fh uint64) (errc int) {
	fill(".", nil, 0)
	fill("..", nil, 0)
	fill(fs.FName, nil, 0)
	return 0
}

// indexFileHandle represents a (read-only) file handle on a blob in a FUSE mounted filesystem
type indexFileHandle struct {
	r *IndexPos

	// perhaps not needed, but in case something is trying to use the same filehandle concurrently
	mu sync.Mutex
}

// NewIndexMountFile initializes a blob file opened in a FUSE mount.
func newIndexFileHandle(idx Index, s Store) *indexFileHandle {
	return &indexFileHandle{
		r: NewIndexReadSeeker(idx, s),
	}
}

// read from a blob file in a FUSE mount.
func (f *indexFileHandle) read(dest []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.r.Seek(off, io.SeekStart); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 0, err
	}
	n, err := f.r.Read(dest)
	if err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, err)
		return 0, err
	}
	return n, nil
}

// MountIndex mounts an index file under a FUSE mount point. The mount will only expose a single
// blob file as represented by the index.
func MountIndex(ctx context.Context, idx Index, path, name string, s Store, n int) error {
	ifs := NewIndexMountFS(idx, name, s)
	host := fuse.NewFileSystemHost(ifs)
	host.Mount(path, nil)
	return nil
}
