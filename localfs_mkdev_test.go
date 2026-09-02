//go:build !windows

package desync

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
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
	// On DragonFly mknod reports success but the node reads back with an Rdev
	// of NODEV (0xffffffff) for every pair tried here, 0:0 included, so the
	// major/minor can't survive the trip through the kernel. TestMkdev above
	// still passes there, which places the problem in mknod rather than in
	// desync's encoding. CreateDevice now rejects such a node outright, which
	// TestCreateDeviceRejectsUnrecordedNumber covers.
	if runtime.GOOS == "dragonfly" {
		t.Skip("dragonfly does not preserve device numbers through mknod")
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

// TestCreateDeviceRejectsUnrecordedNumber checks that a node the filesystem
// didn't record the device number for is reported rather than left in place
// carrying the wrong device. DragonFly is the platform where that actually
// happens, so it is the only one that can exercise it - should its mknod
// start recording the number, this fails and the skip above can go too.
func TestCreateDeviceRejectsUnrecordedNumber(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("creating device nodes requires root")
	}
	if runtime.GOOS != "dragonfly" {
		t.Skip("no other platform is known to accept a device number without recording it")
	}

	fs := NewLocalFS(t.TempDir(), LocalFSOptions{})
	err := fs.CreateDevice(NodeDevice{
		Name:  "dev",
		Mode:  os.ModeDevice | os.ModeCharDevice | 0666,
		Major: 1,
		Minor: 3,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recorded device")
}

// TestCreateDeviceVerifiesNumber checks that a node whose recorded major/minor
// doesn't match what was asked for is reported rather than accepted. Creating
// the node with one pair and verifying it against another stands in for a
// filesystem that doesn't record the number it was given, which is otherwise
// only reproducible on DragonFly.
func TestCreateDeviceVerifiesNumber(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("creating device nodes requires root")
	}
	if runtime.GOOS == "dragonfly" {
		t.Skip("dragonfly does not preserve device numbers through mknod")
	}

	dir := t.TempDir()
	fs := NewLocalFS(dir, LocalFSOptions{})
	require.NoError(t, fs.CreateDevice(NodeDevice{
		Name:  "dev",
		Mode:  os.ModeDevice | os.ModeCharDevice | 0666,
		Major: 1,
		Minor: 3,
	}))
	require.NoError(t, fs.Close())

	r, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer r.Close()

	err = verifyDeviceNode(r, NodeDevice{Name: "dev", Major: 9, Minor: 9})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recorded device 1:3, not 9:9")
}
