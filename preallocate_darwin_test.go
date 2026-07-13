//go:build darwin

package desync

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestPreallocateTightDisk re-preallocates an existing file to its current
// size on a nearly-full volume. This must not require additional disk
// space: F_PREALLOCATE with F_PEOFPOSMODE allocates relative to the
// existing end of file, so requesting the full size instead of only the
// missing difference over-allocates and fails with ENOSPC.
func TestPreallocateTightDisk(t *testing.T) {
	if _, err := exec.LookPath("hdiutil"); err != nil {
		t.Skip("hdiutil not available")
	}
	dir := t.TempDir()
	img := filepath.Join(dir, "small.dmg")
	mount := filepath.Join(dir, "mnt")
	require.NoError(t, os.Mkdir(mount, 0755))

	out, err := exec.Command("hdiutil", "create", "-size", "32m", "-fs", "APFS", "-volname", "desync-test", img).CombinedOutput()
	require.NoError(t, err, string(out))
	out, err = exec.Command("hdiutil", "attach", img, "-mountpoint", mount, "-nobrowse").CombinedOutput()
	require.NoError(t, err, string(out))
	t.Cleanup(func() { _ = exec.Command("hdiutil", "detach", mount, "-force").Run() })

	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(mount, &st))
	free := int64(st.Bavail) * int64(st.Bsize)

	// Fill most of the volume with an existing file, leaving less free
	// space than the size of the file itself
	size := free * 2 / 3
	name := filepath.Join(mount, "existing")
	require.NoError(t, os.WriteFile(name, bytes.Repeat([]byte{0xab}, int(size)), 0666))

	// The file already has the right size, nothing should be allocated
	require.NoError(t, preallocateFile(name, size))
}
