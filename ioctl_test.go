//go:build !windows

package desync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFileSizeRegularFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "blob")
	require.NoError(t, os.WriteFile(name, make([]byte, 4096), 0644))

	size, err := GetFileSize(name)
	require.NoError(t, err)
	assert.EqualValues(t, 4096, size)
}

func TestGetFileSizeMissingFile(t *testing.T) {
	_, err := GetFileSize(filepath.Join(t.TempDir(), "nope"))
	require.Error(t, err)
}

// TestGetFileSizeSizelessDevice covers a character device that isn't a disk.
// It has no size to report and has to say so: returning the zero quietly, which
// is what the non-Linux implementation used to do for every device, produces an
// index for an empty blob instead of failing.
func TestGetFileSizeSizelessDevice(t *testing.T) {
	info, err := os.Stat("/dev/zero")
	if err != nil {
		t.Skip("no /dev/zero on this system")
	}
	require.True(t, isDevice(info.Mode()), "/dev/zero is expected to be a device")

	_, err = GetFileSize("/dev/zero")
	assert.Error(t, err)
}
