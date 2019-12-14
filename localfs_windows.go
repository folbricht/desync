package desync

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

// NewLocalFS initializes a new instance of a local filesystem that
// can be used for tar/untar operations.
func NewLocalFS(root string, opts LocalFSOptions) *LocalFS {
	return &LocalFS{
		Root:    root,
		opts:    opts,
		entries: make(chan walkEntry),
	}
}

func (fs *LocalFS) SetDirPermissions(n NodeDirectory) error {
	// Permission attributes are ignored on Windows
	return nil
}

func (fs *LocalFS) SetFilePermissions(n NodeFile) error {
	// Permission attributes are ignored on Windows
	return nil
}

func (fs *LocalFS) SetSymlinkPermissions(n NodeSymlink) error {
	// Permission attributes are ignored on Windows
	return nil
}

func (fs *LocalFS) CreateDevice(n NodeDevice) error {
	return errors.New("Device nodes not supported on this platform")
}

// Next returns the next filesystem entry or io.EOF when done. The caller is responsible
// for closing the returned File object.
func (fs *LocalFS) Next() (*File, error) {
	fs.once.Do(func() {
		fs.startSerializer()
	})

	entry, ok := <-fs.entries
	if !ok {
		return nil, fs.sErr
	}
	if entry.err != nil {
		return nil, entry.err
	}

	// If it's a file, open it and return a ReadCloser
	var r io.ReadCloser
	if entry.info.Mode().IsRegular() {
		data, err := os.Open(entry.path)
		if err != nil {
			return nil, err
		}
		r = data
	}

	// If this is a symlink we need to get the link target
	var (
		linkTarget string
		err        error
	)
	if entry.info.Mode()&os.ModeSymlink != 0 {
		linkTarget, err = os.Readlink(entry.path)
		if err != nil {
			return nil, err
		}
	}

	mtime := entry.info.ModTime()
	if fs.opts.NoTime {
		mtime = time.Unix(0, 0)
	}

	f := &File{
		Name:       entry.info.Name(),
		Path:       filepath.ToSlash(filepath.Clean(entry.path)),
		Mode:       entry.info.Mode(),
		ModTime:    mtime,
		Size:       uint64(entry.info.Size()),
		LinkTarget: filepath.ToSlash(linkTarget),
		Data:       r,
	}

	return f, nil
}

func (fs *LocalFS) startSerializer() {
	go func() {
		err := filepath.Walk(fs.Root, func(path string, info os.FileInfo, err error) error {
			fs.entries <- walkEntry{path, info, err}
			return nil
		})
		fs.sErr = err
		if err == nil {
			fs.sErr = io.EOF
		}
		close(fs.entries)
	}()
}
