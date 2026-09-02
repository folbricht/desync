//go:build linux || netbsd || openbsd || dragonfly

package desync

import (
	"os"
	"path"

	"golang.org/x/sys/unix"
)

// createDeviceNode creates a device node confined to the extraction root.
// The parent directory is opened through the os.Root handle (which refuses to
// traverse any symlink escaping the root) and the node is created relative to
// that directory fd with a base name that has no path separators, so it
// cannot escape Root.
//
// This covers every platform whose mknodat(2) wrapper takes the device number
// as an int. FreeBSD has mknodat too but types it as uint64, so it needs its
// own copy; Darwin has no mknodat at all and falls back to a resolve-then-
// mknod dance.
func (fs *LocalFS) createDeviceNode(r *os.Root, n NodeDevice) error {
	dev, err := mkdev(n.Major, n.Minor)
	if err != nil {
		return err
	}
	df, err := r.Open(path.Dir(n.Name))
	if err != nil {
		return err
	}
	defer df.Close()
	return unix.Mknodat(int(df.Fd()), path.Base(n.Name), FilemodeToStatMode(n.Mode)|0666, int(dev))
}
