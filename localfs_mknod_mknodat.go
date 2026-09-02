//go:build linux || openbsd || dragonfly

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
// This covers the platforms whose mknodat(2) both takes the device number as
// an int and actually carries it through. FreeBSD has mknodat too but types
// the argument uint64, so it needs its own copy. Darwin has no mknodat at all,
// and NetBSD's drops the device number, so both go through plain mknod
// instead.
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
