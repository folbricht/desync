//go:build linux

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
func (fs *LocalFS) createDeviceNode(r *os.Root, n NodeDevice) error {
	df, err := r.Open(path.Dir(n.Name))
	if err != nil {
		return err
	}
	defer df.Close()
	return unix.Mknodat(int(df.Fd()), path.Base(n.Name), FilemodeToStatMode(n.Mode)|0666, int(mkdev(n.Major, n.Minor)))
}
