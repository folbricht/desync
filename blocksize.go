// +build !windows

package desync

import (
	"os"
	"syscall"
)

func blocksizeOfFile(name string) uint64 {
	stat, err := os.Stat(name)
	if err != nil {
		return DefaultBlockSize
	}
	switch sys := stat.Sys().(type) {
	case *syscall.Stat_t:
		return uint64(sys.Blksize)
	default:
		return DefaultBlockSize
	}
}
