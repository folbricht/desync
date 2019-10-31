package desync

import (
	"github.com/pkg/errors"
)

// NewLocalFS initializes a new instance of a local filesystem that
// can be used for tar/untar operations.
func NewLocalFS(root string, opts LocalFSOptions) *LocalFS {
	return &LocalFS{
		Root:    root,
		opts:    opts,
		entries: nil,
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
	return nil, errors.New("Filesystem iteration is not supported on this platform")
}
