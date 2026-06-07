//go:build darwin

package desync

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// preallocateFile physically allocates disk blocks and sets the file size.
// On APFS, a plain Truncate creates sparse holes. When concurrent workers
// call WriteAt on adjacent regions, copy-on-write of sparse blocks can
// cause non-deterministic data corruption. Pre-allocating real blocks
// avoids this.
func preallocateFile(name string, size int64) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	// F_PREALLOCATE with F_PEOFPOSMODE allocates relative to the current
	// end of file, so only request the difference. Nothing to allocate if
	// the file is already large enough or no growth is needed.
	if extra := size - info.Size(); extra > 0 {
		store := unix.Fstore_t{
			Flags:   unix.F_ALLOCATEALL,
			Posmode: unix.F_PEOFPOSMODE,
			Offset:  0,
			Length:  extra,
		}
		if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &store); err != nil {
			// Not all filesystems support F_PREALLOCATE (e.g. SMB or FUSE
			// mounts). The sparse-hole issue is specific to APFS, so fall
			// back to a plain truncate there.
			if !errors.Is(err, unix.ENOTSUP) {
				return fmt.Errorf("F_PREALLOCATE %s: %w", name, err)
			}
		}
	}

	return f.Truncate(size)
}
