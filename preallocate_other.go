//go:build !darwin

package desync

import "os"

// preallocateFile truncates the file to the given size.
// On Linux (ext4) and other platforms, Truncate produces a file that
// reads back as zeros without sparse-hole issues, so no special
// preallocation is needed.
func preallocateFile(name string, size int64) error {
	return os.Truncate(name, size)
}
