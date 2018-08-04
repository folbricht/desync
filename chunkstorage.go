package desync

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type ChunkStorage struct {
	sync.Mutex
	ws        WriteStore
	processed map[ChunkID]struct{}
}

// Stores chunks passed in the input channel asynchronously. Wait() will wait for until the input channel is closed or
// until there's an error, in which case it will return it.
func NewChunkStorage(ws WriteStore) *ChunkStorage {
	s := &ChunkStorage{
		ws:        ws,
		processed: make(map[ChunkID]struct{}),
	}
	return s
}

// Mark a chunk in the in-memory cache as having been processed and returns true
// if it was already marked, and is therefore presumably already stored.
func (s *ChunkStorage) markProcessed(id ChunkID) bool {
	s.Lock()
	defer s.Unlock()
	_, ok := s.processed[id]
	s.processed[id] = struct{}{}
	return ok
}

// Stores a single chunk in a synchronous manner.
func (s *ChunkStorage) StoreChunk(id ChunkID, b []byte) error {

	// Mark this chunk as done so no other goroutine will attempt to store it
	// at the same time. If this is the first time this chunk is marked, it'll
	// return false and we need to continue processing/storing the chunk below.
	if s.markProcessed(id) {
		return nil
	}

	// Skip this chunk if the store already has it
	if s.ws.HasChunk(id) {
		return nil
	}

	var retried bool
retry:
	// Compress the chunk
	cb, err := Compress(b)
	if err != nil {
		return err
	}

	// The zstd library appears to fail to compress correctly in some cases, to
	// avoid storing invalid chunks, verify the chunk again by decompressing
	// and comparing. See https://github.com/folbricht/desync/issues/37.
	// Ideally the code below should be removed once zstd library can be trusted
	// again.
	db, err := Decompress(nil, cb)
	if err != nil {
		return errors.Wrap(err, id.String())
	}

	if !bytes.Equal(b, db) {
		if !retried {
			fmt.Fprintln(os.Stderr, "zstd compression error detected, retrying")
			retried = true
			goto retry
		}
		return errors.New("too many zstd compression errors, aborting")
	}

	// Store the compressed chunk
	return s.ws.StoreChunk(id, cb)
}
