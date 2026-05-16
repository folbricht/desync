//go:build !linux && !windows

package desync

import (
	"os"
	"path"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// createDeviceNode creates a device node confined to the extraction root.
// Darwin (and other non-Linux Unix) has no mknodat(2), so the parent
// directory is first resolved through the os.Root handle - which refuses to
// traverse any symlink that escapes the root - to validate confinement, and
// the node is then created on the corresponding real path. Extraction is
// single-threaded, so there is no concurrent attacker able to swap a
// component between this check and the mknod call.
func (fs *LocalFS) createDeviceNode(r *os.Root, n NodeDevice) error {
	df, err := r.Open(path.Dir(n.Name))
	if err != nil {
		return err
	}
	df.Close()
	dst := filepath.Join(fs.rootReal, n.Name)
	return unix.Mknod(dst, FilemodeToStatMode(n.Mode)|0666, int(mkdev(n.Major, n.Minor)))
}
