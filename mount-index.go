// +build !windows

package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

// IndexMountFS is used to FUSE mount an index file (as a blob, not an archive).
// It present a single file underneath the mountpoint.
type IndexMountFS struct {
	FName string // File name in the mountpoint
	Idx   Index  // Index of the blob
	Store Store
	pathfs.FileSystem
}

// NewIndexMountFS initializes a FUSE filesystem mount based on an index and a chunk store.
func NewIndexMountFS(idx Index, name string, s Store) *IndexMountFS {
	return &IndexMountFS{
		FName:      name,
		Idx:        idx,
		Store:      s,
		FileSystem: pathfs.NewDefaultFileSystem(),
	}
}

// GetAttr returns file attributes of a file or directory in a FUSE mount.
func (me *IndexMountFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	switch name {
	case me.FName:
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0444, Size: uint64(me.Idx.Length()),
		}, fuse.OK
	case "":
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	return nil, fuse.ENOENT
}

// OpenDir is called when a directory is opened in FUSE.
func (me *IndexMountFS) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	if name == "" {
		c = []fuse.DirEntry{{Name: me.FName, Mode: fuse.S_IFREG}}
		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

// Open a file in a FUSE mount.
func (me *IndexMountFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if name != me.FName {
		return nil, fuse.ENOENT
	}
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	fh := NewIndexMountFile(me.Idx, me.Store)
	return fh, fuse.OK
}

// IndexMountFile represents a (read-only) file handle on a blob in a FUSE
// mounted filesystem
type IndexMountFile struct {
	r *IndexPos
	nodefs.File

	// perhaps not needed, but in case something is trying to use the same
	// filehandle concurrently
	mu sync.Mutex
}

// NewIndexMountFile initializes a blob file opened in a FUSE mount.
func NewIndexMountFile(idx Index, s Store) *IndexMountFile {
	return &IndexMountFile{
		r:    NewIndexReadSeeker(idx, s),
		File: nodefs.NewReadOnlyFile(nodefs.NewDefaultFile()),
	}
}

// Read from a blob file in a FUSE mount.
func (f *IndexMountFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.r.Seek(off, io.SeekStart); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, fuse.EIO
	}
	n, err := f.r.Read(dest)
	if err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, err)
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

// GetAttr returns attributes of a blob file in a FUSE mount.
func (f *IndexMountFile) GetAttr(out *fuse.Attr) fuse.Status {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = uint64(f.r.Length)
	return fuse.OK
}

// MountIndex mounts an index file under a FUSE mount point. The mount will only expose a single
// blob file as represented by the index.
func MountIndex(ctx context.Context, idx Index, path, name string, s Store, n int) error {
	ifs := NewIndexMountFS(idx, name, s)
	fs := pathfs.NewPathNodeFs(ifs, nil)
	server, _, err := nodefs.MountRoot(path, fs.Root(), nil)
	if err != nil {
		return err
	}
	server.Serve()
	return nil
}
