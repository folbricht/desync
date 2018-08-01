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
	sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	n      int
	ws     WriteStore
	in     <-chan Chunk
	pb     ProgressBar

	wg     sync.WaitGroup
	pErr   error
	stored map[ChunkID]bool
}

// Stores chunks passed in the input channel asynchronously. Wait() will wait for until the input channel is closed or
// until there's an error, in which case it will return it.
func NewChunkStorage(ctx context.Context, cancel context.CancelFunc, n int, ws WriteStore, in <-chan Chunk, pb ProgressBar) *ChunkStorage {
	s := &ChunkStorage{
		ctx:    ctx,
		cancel: cancel,
		n:      n,
		ws:     ws,
		in:     in,
		pb:     pb,
		stored: make(map[ChunkID]bool),
	}
	if in != nil {
		s.init()
	}
	return s
}

// Initializes the chunk storage workers
func (s *ChunkStorage) init() {

	// Update progress bar if any
	if s.pb != nil {
		s.pb.Start()
	}

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		s.Lock()
		defer s.Unlock()
		if s.pErr == nil {
			s.pErr = err
		}
		s.cancel()
	}

	// Start the workers responsible for checksum calculation, compression and
	// storage (if required). Each job comes with a chunk number for sorting later
	for i := 0; i < s.n; i++ {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()
			for c := range s.in {

				// Update progress bar if any
				if s.pb != nil {
					s.pb.Add(1)
				}

				if err := s.StoreChunk(c); err != nil {
					recordError(err)
					continue
				}
			}
		}()
	}
}

// Waits until the input channel is closed or an error occurs.
func (s *ChunkStorage) Wait() error {
	s.wg.Wait()

	// Update progress bar if any
	if s.pb != nil {
		s.pb.Stop()
	}

	return s.pErr
}

func (s *ChunkStorage) isChunkStored(c Chunk) bool {
	s.Lock()
	defer s.Unlock()
	return s.stored[c.ID]
}

func (s *ChunkStorage) markAsStored(c Chunk) {
	s.Lock()
	defer s.Unlock()
	s.stored[c.ID] = true
}

// Stores a single chunk in a synchronous manner.
func (s *ChunkStorage) StoreChunk(c Chunk) error {

	// Check in-memory cache to see if chunk has been stored, if so, skip it
	if s.isChunkStored(c) {
		return nil
	}

	// Skip this chunk if the store already has it
	if s.ws.HasChunk(c.ID) {
		return nil
	}

	var retried bool
retry:
	// Compress the chunk
	cb, err := Compress(c.Data)
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

	if !bytes.Equal(c.Data, db) {
		if !retried {
			fmt.Fprintln(os.Stderr, "zstd compression error detected, retrying")
			retried = true
			goto retry
		}
		return errors.New("too many zstd compression errors, aborting")
	}

	// Store the compressed chunk
	if err = s.ws.StoreChunk(c.ID, cb); err != nil {
		return err
	}

	s.markAsStored(c)
	return nil
}
