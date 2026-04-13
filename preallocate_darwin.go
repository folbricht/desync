//go:build darwin

package desync

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type fstore_t struct {
	Flags      uint32
	Posmode    int32
	Offset     int64
	Length     int64
	Bytesalloc int64
}

const (
	fAllocateAll = 0x00000004
	fPeofPosmode = 3
	fPreallocate = 42
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

	store := fstore_t{
		Flags:   fAllocateAll,
		Posmode: fPeofPosmode,
		Offset:  0,
		Length:  size,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
		uintptr(f.Fd()),
		uintptr(fPreallocate),
		uintptr(unsafe.Pointer(&store)))
	if errno != 0 {
		return fmt.Errorf("F_PREALLOCATE: %w", errno)
	}

	return f.Truncate(size)
}
