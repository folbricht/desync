//go:build !darwin

package desync

import "os"

// preallocateFile truncates the file to the given size, creating it if
// it doesn't exist. On Linux (ext4) and other platforms, Truncate
// produces a file that reads back as zeros without sparse-hole issues,
// so no special preallocation is needed.
func preallocateFile(name string, size int64) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}
