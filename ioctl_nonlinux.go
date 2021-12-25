// +build !linux

package desync

import (
	"errors"
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
	fm := info.Mode()
	if isDevice(fm) {
		// TODO we probably should do something platform specific here to get the correct size
	}
	return uint64(info.Size()), nil
}
