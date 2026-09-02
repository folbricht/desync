package desync

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMountIndex(t *testing.T) {
	_, err := os.Stat("/dev/fuse")
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("FUSE is not available here: %v", err)
	}
	require.NoError(t, err)

	// Create the mount point
	mnt := t.TempDir()

	// Define the store
	s, err := NewLocalStore("testdata/blob1.store", StoreOptions{})
	require.NoError(t, err)
	defer s.Close()

	// Read the index
	f, err := os.Open("testdata/blob1.caibx")
	require.NoError(t, err)
	defer f.Close()
	index, err := IndexFromReader(f)
	require.NoError(t, err)

	// Calculate the expected hash
	b, err := os.ReadFile("testdata/blob1")
	require.NoError(t, err)
	wantHash := sha256.Sum256(b)

	// Make sure that the unmount happens on exit
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Start the Fuse mount
	c := make(chan error, 1)
	go func() {
		ifs := NewIndexMountFS(index, "blob1", s)
		c <- MountIndex(ctx, index, ifs, mnt, s, 10)
		wg.Done()
	}()

	select {
	case err = <-c:
		// go-fuse reports a missing fusermount as the error from looking it
		// up, before anything of ours has run.
		var lookup *exec.Error
		if errors.As(err, &lookup) && errors.Is(err, fs.ErrNotExist) {
			t.Skipf("FUSE is not available here: %v", err)
		}
		require.FailNow(t, "mount exited early", "%v", err)
	case <-time.After(time.Second):
	}

	// Calculate the hash of the file in the mount point
	b, err = os.ReadFile(filepath.Join(mnt, "blob1"))
	require.NoError(t, err)
	gotHash := sha256.Sum256(b)

	// Compare the checksums
	require.Equal(t, wantHash, gotHash)
}
