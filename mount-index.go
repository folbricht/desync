// +build !windows

package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// IndexMountFS is used to FUSE mount an index file (as a blob, not an archive).
// It present a single file underneath the mountpoint.
type IndexMountFS struct {
	fs.Inode

	FName string // File name in the mountpoint
	Idx   Index  // Index of the blob
	Store Store
}

var _ fs.NodeOnAdder = &IndexMountFS{}

// NewIndexMountFS initializes a FUSE filesystem mount based on an index and a chunk store.
func NewIndexMountFS(idx Index, name string, s Store) *IndexMountFS {
	return &IndexMountFS{
		FName: name,
		Idx:   idx,
		Store: s,
	}
}

// OnAdd is used to build the static filesystem structure at the start of the mount.
func (r *IndexMountFS) OnAdd(ctx context.Context) {
	n := &indexFile{
		idx:   r.Idx,
		store: r.Store,
		mtime: time.Now(),
	}
	ch := r.NewPersistentInode(ctx, n, fs.StableAttr{Mode: fuse.S_IFREG})
	r.AddChild(r.FName, ch, false)
}

var _ fs.NodeGetattrer = &indexFile{}
var _ fs.NodeOpener = &indexFile{}

type indexFile struct {
	fs.Inode

	idx   Index // Index of the blob
	store Store

	mtime time.Time
}

func (n *indexFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	fh := newIndexFileHandle(n.idx, n.store)
	return fh, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (n *indexFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f := fh.(*indexFileHandle)
	return f.read(dest, off)
}

func (n *indexFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = uint64(n.idx.Length())
	out.Mtime = uint64(n.mtime.Unix())
	return fs.OK
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
func (f *indexFileHandle) read(dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.r.Seek(off, io.SeekStart); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, syscall.EIO
	}
	n, err := f.r.Read(dest)
	if err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), fs.OK
}

// MountIndex mounts an index file under a FUSE mount point. The mount will only expose a single
// blob file as represented by the index.
func MountIndex(ctx context.Context, idx Index, path, name string, s Store, n int) error {
	ifs := NewIndexMountFS(idx, name, s)
	opts := &fs.Options{}
	server, err := fs.Mount(path, ifs, opts)
	if err != nil {
		return err
	}
	go func() { // Unmount the server when the contex expires
		<-ctx.Done()
		server.Unmount()
	}()
	server.Wait()
	return nil
}
