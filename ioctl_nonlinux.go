//go:build !linux

package desync

import (
	"errors"
	"fmt"
	"io"
	"os"
)

func CanClone(dstFile string, srcFile string) bool {
	return false
}

func CloneRange(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
	return errors.New("Not available on this platform")
}

// GetFileSize determines the size, in Bytes, of the file located at the given
// fileName.
func GetFileSize(fileName string) (size uint64, err error) {
	info, err := os.Stat(fileName)
	if err != nil {
		return 0, err
	}
	if !isDevice(info.Mode()) {
		return uint64(info.Size()), nil
	}

	// Stat reports zero for a device, so the size has to come from the device
	// itself. Linux has an ioctl for it; everywhere else, seeking to the end
	// is what's portable across the BSDs and macOS. It reports the same value
	// the ioctl does where both are available.
	f, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("determining the size of %s: %w", fileName, err)
	}
	if n <= 0 {
		// A character device that isn't a disk, /dev/zero and friends, has no
		// size to report. Returning the zero silently would produce an index
		// for an empty blob.
		return 0, fmt.Errorf("unable to determine the size of device %s", fileName)
	}
	return uint64(n), nil
}
