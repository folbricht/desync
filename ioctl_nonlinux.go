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
