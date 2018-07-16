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

// FICLONERANGE ioctl
const fiCloneRange = 0x4020940d

// CanClone tries to determine if the filesystem allows cloning of blocks between
// two files. If the files don't (yet) exits, it'll create a tempfile in the
// same dirs and attempt to perfom a 0-byte long block clone. If that's successful
// it'll return true.
func CanClone(dstFile string, srcFile string) bool {
	dst, err := os.OpenFile(dstFile, os.O_WRONLY, 0)
	if err != nil {
		dst, err = ioutil.TempFile(filepath.Dir(dstFile), ".tmp")
		if err != nil {
			return false
		}
		defer dst.Close()
		defer os.Remove(dst.Name())
	} else {
		defer dst.Close()
	}
	src, err := os.Open(srcFile)
	if err != nil {
		src, err = ioutil.TempFile(filepath.Dir(srcFile), ".tmp")
		if err != nil {
			return false
		}
		defer src.Close()
		defer os.Remove(src.Name())
	} else {
		defer src.Close()
	}
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

func ioctl(fd, operation, argp uintptr) error {
	_, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, operation, argp)
	if e != 0 {
		return syscall.Errno(e)
	}
	return nil
}
