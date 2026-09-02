//go:build !linux && !freebsd && !windows && !openbsd && !dragonfly

package desync

import (
	"os"
	"path"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// createDeviceNode creates a device node confined to the extraction root.
// Darwin has no mknodat(2) to use, and NetBSD's takes a uint32_t where every
// other BSD takes a dev_t and loses the device number as a result - nodes come
// back with an rdev of 0 - while its plain mknod(2) is the versioned
// __mknod50, which carries the full dev_t. So both create the node with mknod
// instead: the parent directory is first resolved through the os.Root handle -
// which refuses to traverse any symlink that escapes the root - to validate
// confinement, and the node is then created on the corresponding real path.
// Extraction is single-threaded, so there is no concurrent attacker able to
// swap a component between this check and the mknod call.
func (fs *LocalFS) createDeviceNode(r *os.Root, n NodeDevice) error {
	dev, err := mkdev(n.Major, n.Minor)
	if err != nil {
		return err
	}
	df, err := r.Open(path.Dir(n.Name))
	if err != nil {
		return err
	}
	df.Close()
	dst := filepath.Join(fs.rootReal, n.Name)
	return unix.Mknod(dst, FilemodeToStatMode(n.Mode)|0666, int(dev))
}
