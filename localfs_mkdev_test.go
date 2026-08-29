//go:build !windows

package desync

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The major/minor pairs used here stay within 8 bits so that they survive a
// round-trip on every supported platform. The encodings differ in how many
// bits each component gets: FreeBSD gives the minor 8, Darwin gives the major
// 8, Linux is wider than both.
var devTestCases = []struct {
	major, minor uint64
}{
	{0, 0},
	{1, 3},
	{5, 1},
	{8, 0},
	{10, 229},
	{253, 255},
}

// TestMkdev checks that a major/minor pair encoded for the running platform is
// split back into the same pair by that platform's own accessors. desync used
// to encode with Linux's layout everywhere, which produced wrong device
// numbers on Darwin and FreeBSD.
func TestMkdev(t *testing.T) {
	for _, tc := range devTestCases {
		dev, err := mkdev(tc.major, tc.minor)
		require.NoError(t, err)
		assert.Equal(t, tc.major, uint64(unix.Major(dev)), "major of %d:%d", tc.major, tc.minor)
		assert.Equal(t, tc.minor, uint64(unix.Minor(dev)), "minor of %d:%d", tc.major, tc.minor)
	}
}

// TestMkdevOutOfRange checks that a major or minor too large to encode is
// rejected rather than silently truncated.
func TestMkdevOutOfRange(t *testing.T) {
	_, err := mkdev(math.MaxUint32+1, 0)
	require.Error(t, err)

	_, err = mkdev(0, math.MaxUint32+1)
	require.Error(t, err)

	_, err = mkdev(math.MaxUint32, math.MaxUint32)
	require.NoError(t, err)
}

// TestCreateDeviceRoundTrip creates real device nodes and reads them back
// through LocalFS, confirming that the major/minor written to disk are the
// ones the archive asked for. Requires root, so it only runs where the tests
// are executed as root.
func TestCreateDeviceRoundTrip(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("creating device nodes requires root")
	}
	for _, tc := range devTestCases {
		dir := t.TempDir()
		fs := NewLocalFS(dir, LocalFSOptions{})
		n := NodeDevice{
			Name:  "dev",
			Mode:  os.ModeDevice | os.ModeCharDevice | 0666,
			Major: tc.major,
			Minor: tc.minor,
		}
		require.NoError(t, fs.CreateDevice(n))
		require.NoError(t, fs.Close())

		var st unix.Stat_t
		require.NoError(t, unix.Lstat(filepath.Join(dir, "dev"), &st))
		assert.Equal(t, tc.major, uint64(unix.Major(uint64(st.Rdev))), "major of %d:%d", tc.major, tc.minor)
		assert.Equal(t, tc.minor, uint64(unix.Minor(uint64(st.Rdev))), "minor of %d:%d", tc.major, tc.minor)

		// Read it back the way tar does, which must report the same pair.
		src := NewLocalFS(dir, LocalFSOptions{})
		var found *File
		for {
			f, err := src.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			if f.IsDevice() {
				found = f
			}
		}
		require.NotNil(t, found, "device not returned by LocalFS")
		assert.Equal(t, tc.major, found.DevMajor, "DevMajor of %d:%d", tc.major, tc.minor)
		assert.Equal(t, tc.minor, found.DevMinor, "DevMinor of %d:%d", tc.major, tc.minor)
	}
}
