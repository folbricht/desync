package casync

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

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
	p := filepath.Join(s.Base, sID[0:4], sID) + ".cacnk"
	if _, err := os.Stat(p); err != nil {
		return nil, ChunkMissing{id}
	}
	if s.UpdateTimes {
		now := time.Now()
		if err := os.Chtimes(p, now, now); err != nil {
			return nil, err
		}
	}
	return ioutil.ReadFile(p)
}

// StoreChunk adds a new chunk to the store
func (s LocalStore) StoreChunk(id ChunkID, b []byte) error {
	sID := id.String()
	d := filepath.Join(s.Base, sID[0:4])
	if err := os.MkdirAll(d, 0755); err != nil {
		return err
	}
	tmpfile, err := ioutil.TempFile(d, ".tmp-cacnk")
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name()) // in case we don't get to the rename, clean up
	if _, err := tmpfile.Write(b); err != nil {
		return err
	}
	if err := tmpfile.Close(); err != nil {
		return err
	}
	p := filepath.Join(d, sID) + ".cacnk"
	return os.Rename(tmpfile.Name(), p)
}
