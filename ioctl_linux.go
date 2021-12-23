// +build linux

package desync

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
)

// BLKGETSIZE64 ioctl
const blkGetSize64 = 0x80081272

// FICLONERANGE ioctl
const fiCloneRange = 0x4020940d

// CanClone tries to determine if the filesystem allows cloning of blocks between
// two files. It'll create two tempfiles in the same dirs and attempt to perfom
// a 0-byte long block clone. If that's successful it'll return true.
func CanClone(dstFile, srcFile string) bool {
	dst, err := ioutil.TempFile(filepath.Dir(dstFile), ".tmp")
	if err != nil {
		return false
	}
	defer os.Remove(dst.Name())
	defer dst.Close()
	src, err := ioutil.TempFile(filepath.Dir(srcFile), ".tmp")
	if err != nil {
		return false
	}
	defer os.Remove(src.Name())
	defer src.Close()
	err = CloneRange(dst, src, 0, 0, 0)
	return err == nil
}

// CloneRange uses the FICLONERANGE ioctl to de-dupe blocks between two files
// when using XFS or btrfs. Only works at block-boundaries.
func CloneRange(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
	// Build a structure to hold the argument for this IOCTL
	// struct file_clone_range {
	//     __s64 src_fd;
	//     __u64 src_offset;
	//     __u64 src_length;
	//     __u64 dest_offset;
	// };
	arg := new(bytes.Buffer)
	binary.Write(arg, binary.LittleEndian, uint64(src.Fd()))
	binary.Write(arg, binary.LittleEndian, srcOffset)
	binary.Write(arg, binary.LittleEndian, srcLength)
	binary.Write(arg, binary.LittleEndian, dstOffset)
	err := ioctl(dst.Fd(), fiCloneRange, uintptr(unsafe.Pointer(&arg.Bytes()[0])))
	return errors.Wrapf(err, "failure cloning blocks from %s to %s", src.Name(), dst.Name())
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
		err = ioctl(f.Fd(), blkGetSize64, uintptr(unsafe.Pointer(&size)))
		if err != nil {
			return 0, err
		}
		return size, nil
	} else {
		return uint64(info.Size()), nil
	}
}

func ioctl(fd, operation, argp uintptr) error {
	_, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, operation, argp)
	if e != 0 {
		return syscall.Errno(e)
	}
	return nil
}
