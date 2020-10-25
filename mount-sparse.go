// +build !windows

package desync

import (
	"context"
	"io"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// SparseMountFS is used to FUSE mount an index file (as a blob, not an archive).
// It uses a (local) sparse file as cache to improve performance. Every chunk that
// is being read is written into the sparse file
type SparseMountFS struct {
	fs.Inode

	FName string // File name in the mountpoint
	sf    *SparseFile
}

var _ fs.NodeOnAdder = &SparseMountFS{}
var _ MountFS = &SparseMountFS{}

type SparseMountOptions struct {
	// Optional, save the state of the sparse file on exit or SIGHUP. The state file
	// contains information which chunks from the index have been read and are
	// populated in the sparse file. If the state and sparse file exist and match,
	// the sparse file is used as is (not re-populated).
	StateSaveFile string

	// Optional, load all chunks that are marked as read in this state file. It is used
	// to pre-populate a new sparse file if the sparse file or the save state file aren't
	// present or don't match the index. SaveStateFile and StateInitFile can be the same.
	StateInitFile string

	// Optional, number of goroutines to preload chunks from StateInitFile.
	StateInitConcurrency int
}

// NewSparseMountFS initializes a FUSE filesystem mount based on an index, a sparse file and a chunk store.
func NewSparseMountFS(idx Index, name string, s Store, sparseFile string, opt SparseMountOptions) (*SparseMountFS, error) {
	sf, err := NewSparseFile(sparseFile, idx, s, opt)
	if err != nil {
		return nil, err
	}
	return &SparseMountFS{
		FName: name,
		sf:    sf,
	}, err
}

// OnAdd is used to build the static filesystem structure at the start of the mount.
func (r *SparseMountFS) OnAdd(ctx context.Context) {
	n := &sparseIndexFile{
		sf:    r.sf,
		mtime: time.Now(),
		size:  r.sf.Length(),
	}
	ch := r.NewPersistentInode(ctx, n, fs.StableAttr{Mode: fuse.S_IFREG})
	r.AddChild(r.FName, ch, false)
}

// Save the state of the sparse file.
func (r *SparseMountFS) WriteState() error {
	return r.sf.WriteState()
}

// Close the sparse file and save its state.
func (r *SparseMountFS) Close() error {
	return r.sf.WriteState()
}

var _ fs.NodeGetattrer = &indexFile{}
var _ fs.NodeOpener = &indexFile{}

type sparseIndexFile struct {
	fs.Inode
	sf    *SparseFile
	size  int64
	mtime time.Time
}

func (n *sparseIndexFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	fh, err := n.sf.Open()
	if err != nil {
		Log.WithError(err).Error("failed to open sparse file")
		return fh, fuse.FOPEN_KEEP_CACHE, syscall.EIO
	}
	return fh, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (n *sparseIndexFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f := fh.(*SparseFileHandle)
	length, err := f.ReadAt(dest, off)
	if err != nil {
		if err == io.EOF {
			return fuse.ReadResultData(dest[:length]), fs.OK
		}
		Log.WithError(err).Error("failed to read sparse file")
		return fuse.ReadResultData(dest[:length]), syscall.EIO
	}
	return fuse.ReadResultData(dest[:length]), fs.OK
}

func (n *sparseIndexFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = uint64(n.size)
	out.Mtime = uint64(n.mtime.Unix())
	return fs.OK
}
