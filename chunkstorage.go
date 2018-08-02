package desync

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type ChunkStorage struct {
	sync.RWMutex
	cancel context.CancelFunc
	n      int
	ws     WriteStore
	pb     ProgressBar
	wg     sync.WaitGroup
	pErr   error
	stored map[ChunkID]struct{}
}

// Stores chunks passed in the input channel asynchronously. Wait() will wait for until the input channel is closed or
// until there's an error, in which case it will return it.
func NewChunkStorage(ws WriteStore) *ChunkStorage {
	s := &ChunkStorage{
		ws:     ws,
		stored: make(map[ChunkID]struct{}),
	}
	return s
}

func (s *ChunkStorage) isChunkStored(id ChunkID) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.stored[id]
	return ok
}

func (s *ChunkStorage) markAsStored(id ChunkID) {
	s.Lock()
	defer s.Unlock()
	s.stored[id] = struct{}{}
}

// Stores a single chunk in a synchronous manner.
func (s *ChunkStorage) StoreChunk(id ChunkID, b []byte) error {

	// Check in-memory cache to see if chunk has been stored, if so, skip it
	if s.isChunkStored(id) {
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
		return err
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
	if err = s.ws.StoreChunk(id, cb); err != nil {
		return err
	}

	s.markAsStored(id)
	return nil
}
