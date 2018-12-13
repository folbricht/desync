package desync

import "os"

func isSymlink(m os.FileMode) bool {
	return m&os.ModeSymlink != 0
}

func isDevice(m os.FileMode) bool {
	return m&os.ModeDevice != 0
}
