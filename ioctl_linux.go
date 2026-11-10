//go:build linux

package desync

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// CanClone tries to determine if the filesystem allows cloning of blocks between
// two files. It'll create two tempfiles in the same dirs and attempt to perform
// a 0-byte long block clone. If that's successful it'll return true.
func CanClone(dstFile, srcFile string) bool {
	dst, err := os.CreateTemp(filepath.Dir(dstFile), ".tmp")
	if err != nil {
		return false
	}
	defer func() { _ = os.Remove(dst.Name()) }()
	defer dst.Close()
	src, err := os.CreateTemp(filepath.Dir(srcFile), ".tmp")
	if err != nil {
		return false
	}
	defer func() { _ = os.Remove(src.Name()) }()
	defer src.Close()
	err = CloneRange(dst, src, 0, 0, 0)
	return err == nil
}

// CloneRange uses the FICLONERANGE ioctl to de-dupe blocks between two files
// when using XFS or btrfs. Only works at block-boundaries.
func CloneRange(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
	value := &unix.FileCloneRange{
		Src_fd:      int64(src.Fd()),
		Src_offset:  srcOffset,
		Src_length:  srcLength,
		Dest_offset: dstOffset,
	}
	if err := unix.IoctlFileCloneRange(int(dst.Fd()), value); err != nil {
		return fmt.Errorf("failure cloning blocks from %s to %s: %w", src.Name(), dst.Name(), err)
	}
	return nil
}

// GetFileSize determines the size, in Bytes, of the file located at the given
// fileName.
func GetFileSize(fileName string) (size uint64, err error) {
	info, err := os.Stat(fileName)
	if err != nil {
		return 0, err
	}
	fm := info.Mode()
	if isDevice(fm) {
		// When we are working with block devices, we can't simply use `Size()`, because it
		// will return zero instead of the expected device size.
		f, err := os.Open(fileName)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		return unix.IoctlGetUint64(int(f.Fd()), unix.BLKGETSIZE64)
	}
	return uint64(info.Size()), nil
}
