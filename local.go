package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/folbricht/tempfile"
)

const chunkFileExt = ".cacnk"

// LocalStore casync store
type LocalStore struct {
	Base string

	// When accessing chunks, should mtime be updated? Useful when this is
	// a cache. Old chunks can be identified and removed from the store that way
	UpdateTimes bool
}

// NewLocalStore creates an instance of a local castore, it only checks presence
// of the store
func NewLocalStore(dir string) (LocalStore, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return LocalStore{}, err
	}
	if !info.IsDir() {
		return LocalStore{}, fmt.Errorf("%s is not a directory", dir)
	}
	return LocalStore{Base: dir}, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s LocalStore) GetChunk(id ChunkID) ([]byte, error) {
	sID := id.String()
	p := filepath.Join(s.Base, sID[0:4], sID) + chunkFileExt

	b, err := ioutil.ReadFile(p)
	if os.IsNotExist(err) {
		err = ChunkMissing{id}
	}
	return b, err
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s LocalStore) RemoveChunk(id ChunkID) error {
	sID := id.String()
	p := filepath.Join(s.Base, sID[0:4], sID) + chunkFileExt
	if _, err := os.Stat(p); err != nil {
		return ChunkMissing{id}
	}
	return os.Remove(p)
}

// StoreChunk adds a new chunk to the store
func (s LocalStore) StoreChunk(id ChunkID, b []byte) error {
	sID := id.String()
	d := filepath.Join(s.Base, sID[0:4])
	if err := os.MkdirAll(d, 0755); err != nil {
		return err
	}
	tmp, err := tempfile.NewMode(d, ".tmp-cacnk", 0644)
	if err != nil {
		return err
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name()) // in case we don't get to the rename, clean up
	if _, err = tmp.Write(b); err != nil {
		return err
	}
	p := filepath.Join(d, sID) + chunkFileExt
	return os.Rename(tmp.Name(), p)
}

// Verify all chunks in the store. If repair is set true, bad chunks are deleted.
// n determines the number of concurrent operations.
func (s LocalStore) Verify(ctx context.Context, n int, repair bool) error {
	var wg sync.WaitGroup
	ids := make(chan ChunkID)

	// Start the workers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for id := range ids {
				err := s.verifyChunk(id)
				switch err.(type) {
				case ChunkInvalid: // bad chunk, report and delete (if repair=true)
					msg := err.Error()
					if repair {
						if err = s.RemoveChunk(id); err != nil {
							msg = msg + ":" + err.Error()
						} else {
							msg = msg + ": removed"
						}
					}
					fmt.Fprintln(os.Stderr, msg)
				case nil: // all good, move to the next
				default: // unexpected, print the error and carry on
					fmt.Fprintln(os.Stderr, err)
				}
			}
			wg.Done()
		}()
	}

	// Go trough all chunks underneath Base, filtering out other files, then feed
	// the IDs to the workers
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil { // failed to walk? => fail
			return err
		}
		if info.IsDir() { // Skip dirs
			return nil
		}
		if !strings.HasSuffix(path, chunkFileExt) { // Skip files without chunk extension
			return nil
		}
		// Convert the name into a checksum, if that fails we're probably not looking
		// at a chunk file and should skip it.
		id, err := ChunkIDFromString(strings.TrimSuffix(filepath.Base(path), ".cacnk"))
		if err != nil {
			return nil
		}
		// Feed the workers
		ids <- id
		return nil
	})
	close(ids)
	wg.Wait()
	return err
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s LocalStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	// Go trough all chunks underneath Base, filtering out other directories and files
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil { // failed to walk? => fail
			return err
		}
		if info.IsDir() { // Skip dirs
			return nil
		}
		if !strings.HasSuffix(path, chunkFileExt) { // Skip files without chunk extension
			return nil
		}
		// Convert the name into a checksum, if that fails we're probably not looking
		// at a chunk file and should skip it.
		id, err := ChunkIDFromString(strings.TrimSuffix(filepath.Base(path), ".cacnk"))
		if err != nil {
			return nil
		}
		// See if the chunk we're looking at is in the list we want to keep, if not
		// remove it.
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Unpack a chunk, calculate the checksum of its content and return nil if
// they match.
func (s LocalStore) verifyChunk(id ChunkID) error {
	b, err := s.GetChunk(id)
	if err != nil {
		return err
	}
	// The the chunk is compressed. Decompress it here
	db, err := Decompress(nil, b)
	if err != nil {
		return err
	}
	// Verify the checksum of the chunk matches the ID
	sum := sha512.Sum512_256(db)
	if sum != id {
		return ChunkInvalid{ID: id, Sum: sum}
	}
	return nil
}

// HasChunk returns true if the chunk is in the store
func (s LocalStore) HasChunk(id ChunkID) bool {
	sID := id.String()
	p := filepath.Join(s.Base, sID[0:4], sID) + chunkFileExt
	if _, err := os.Stat(p); err == nil {
		return true
	}
	return false
}

func (s LocalStore) String() string {
	return s.Base
}

func (s LocalStore) Close() error { return nil }
