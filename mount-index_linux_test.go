package desync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestMountIndex(t *testing.T) {
	// Create the mount point
	mnt, err := ioutil.TempDir("", "mount-index-store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(mnt)

	// Define the store
	s, err := NewLocalStore("testdata/blob1.store", StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Read the index
	f, err := os.Open("testdata/blob1.caibx")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	index, err := IndexFromReader(f)
	if err != nil {
		t.Fatal(err)
	}

	// Calculate the expected hash
	b, err := ioutil.ReadFile("testdata/blob1")
	if err != nil {
		t.Fatal(err)
	}
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
	go func() {
		ifs := NewIndexMountFS(idx, "blob1", s)
		MountIndex(ctx, index, ifs, mnt, s, 10)
		wg.Done()
	}()

	time.Sleep(time.Second)

	// Calculate the hash of the file in the mount point
	b, err = ioutil.ReadFile(filepath.Join(mnt, "blob1"))
	if err != nil {
		t.Fatal(err)
	}
	gotHash := sha256.Sum256(b)

	// Compare the checksums
	if !bytes.Equal(gotHash[:], wantHash[:]) {
		t.Fatalf("unexpected hash of mounted file. Want %x, got %x", gotHash, wantHash)
	}
}
