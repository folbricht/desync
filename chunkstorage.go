package desync

import (
	"bytes"
	"context"
	"crypto/sha512"
	"fmt"
	"os"
	"sync"

	"io"

	"github.com/pkg/errors"
)

type ChunkJob struct {
	num   int
	chunk IndexChunk
}

type ChunkStorage struct {
	sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	chopFile string
	n        int
	ws       WriteStore
	in       <-chan ChunkJob
	pb       ProgressBar

	wg      sync.WaitGroup
	results map[int]IndexChunk
	pErr    error
}

// Stores the chinks passed in the input channel. If chopFile is passed (non-blank string), then the chunks will be
// read from that file. Otherwise it's assumed that the chunk contents are pre-loaded and passed via the input channel.
func NewChunkStorage(ctx context.Context, cancel context.CancelFunc, chopFile string, n int, ws WriteStore, in <-chan ChunkJob, pb ProgressBar) *ChunkStorage {
	return &ChunkStorage{
		ctx:      ctx,
		cancel:   cancel,
		n:        n,
		ws:       ws,
		chopFile: chopFile,
		in:       in,
		pb:       pb,
		wg:       sync.WaitGroup{},
		results:  make(map[int]IndexChunk),
	}
}

func readChunkFromFile(f *os.File, c IndexChunk) ([]byte, error) {
	var err error
	b := make([]byte, c.Size)

	// Position the filehandle to the place where the chunk is meant to come
	// from within the file
	if _, err = f.Seek(int64(c.Start), io.SeekStart); err != nil {
		return b, err
	}
	// Read the whole (uncompressed) chunk into memory

	if _, err = io.ReadFull(f, b); err != nil {
		return b, err
	}

	return b, nil
}

func (s *ChunkStorage) Start() {

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

	// All the chunks are processed in parallel, but we need to preserve the
	// order for later. So add the chunking results to a map, indexed by
	// the chunk number so we can rebuild it in the right order when done
	recordResult := func(num int, r IndexChunk) {
		s.Lock()
		defer s.Unlock()
		s.results[num] = r
	}

	// Start the workers responsible for checksum calculation, compression and
	// storage (if required). Each job comes with a chunk number for sorting later
	for i := 0; i < s.n; i++ {
		s.wg.Add(1)

		var f *os.File
		var fErr error
		// Check if we need to chop the file
		if s.chopFile != "" {
			f, fErr = os.Open(s.chopFile)
			if fErr != nil {
				recordError(fmt.Errorf("unable to open file %s, %s", s.chopFile, fErr))
			}

		}

		go func() {
			defer f.Close()
			for j := range s.in {

				// Update progress bar if any
				if s.pb != nil {
					s.pb.Add(1)
				}

				// Record the index row
				recordResult(j.num, j.chunk)

				// Skip this chunk if the store already has it
				if s.ws.HasChunk(j.chunk.ID) {
					continue
				}

				// Check if we need to read the chunk from the data file
				if s.chopFile != "" {
					var err error
					j.chunk.b, err = readChunkFromFile(f, j.chunk)
					if err != nil {
						recordError(err)
						continue
					}
				}

				// Calculate this chunks checksum and compare to what it's supposed to be
				// according to the index
				sum := sha512.Sum512_256(j.chunk.b)
				if sum != j.chunk.ID {
					recordError(fmt.Errorf("chunk %s checksum does not match", j.chunk.ID))
					continue
				}

				var retried bool
			retry:
				// Compress the chunk
				cb, err := Compress(j.chunk.b)
				if err != nil {
					recordError(err)
					continue
				}

				// The zstd library appears to fail to compress correctly in some cases, to
				// avoid storing invalid chunks, verify the chunk again by decompressing
				// and comparing. See https://github.com/folbricht/desync/issues/37.
				// Ideally the code below should be removed once zstd library can be trusted
				// again.
				db, err := Decompress(nil, cb)
				if err != nil {
					recordError(err)
					continue
				}
				if !bytes.Equal(j.chunk.b, db) {
					if !retried {
						fmt.Fprintln(os.Stderr, "zstd compression error detected, retrying")
						retried = true
						goto retry
					}
					recordError(errors.New("too many zstd compression errors, aborting"))
					continue
				}

				// Store the compressed chunk
				if err = s.ws.StoreChunk(j.chunk.ID, cb); err != nil {
					recordError(err)
					continue
				}
			}
			s.wg.Done()
		}()
	}
}

func (s *ChunkStorage) GetResults() (map[int]IndexChunk, error) {
	s.wg.Wait()
	// Update progress bar if any
	if s.pb != nil {
		s.pb.Stop()
	}
	return s.results, s.pErr
}
