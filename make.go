package desync

import (
	"context"
	"crypto/sha512"
	"os"
	"sync"
)

// IndexFromFile chunks a file in parallel and returns an index. It does not
// store chunks! Each concurrent chunker starts filesize/n bytes apart and
// splits independently. Each chunk worker tries to sync with it's next
// neighbor and if successful stops processing letting the next one continue.
// The main routine reads and assembles a list of (confirmed) chunks from the
// workers, starting with the first worker.
// This algorithm wastes some CPU and I/O if the data doesn't contain chunk
// boundaries, for example if the whole file contains nil bytes.
func IndexFromFile(ctx context.Context,
	name string,
	n int,
	min, avg, max uint64,
) (Index, error) {

	index := Index{
		Index: FormatIndex{
			FeatureFlags: CaFormatExcludeNoDump | CaFormatSHA512256,
			ChunkSizeMin: min,
			ChunkSizeAvg: avg,
			ChunkSizeMax: max,
		},
	}

	// Adjust n if it's a small file that doesn't have n*max bytes
	info, err := os.Stat(name)
	if err != nil {
		return index, err
	}
	nn := int(info.Size()/int64(max)) + 1
	if nn < n {
		n = nn
	}
	size := uint64(info.Size())
	span := size / uint64(n) // intial spacing between chunkers

	// Create/initialize the workers
	worker := make([]*pChunker, n)
	for i := 0; i < n; i++ {
		f, err := os.Open(name) // open one file per worker
		if err != nil {
			return index, err
		}
		defer f.Close()
		start := span * uint64(i)       // starting position for this chunker
		mChunks := (size-start)/min + 1 // max # of chunks this worker can produce
		c, err := NewChunker(f, min, avg, max, start)
		if err != nil {
			return index, err
		}
		p := &pChunker{
			chunker: c,
			results: make(chan IndexChunk, mChunks),
			done:    make(chan struct{}),
		}
		worker[i] = p
	}

	// Link the workers, each one gets a pointer to the next, the last one gets nil
	for i := 1; i < n; i++ {
		worker[i-1].next = worker[i]
	}

	// Start the workers
	for _, w := range worker {
		go w.start(ctx)
		defer w.stop() // shouldn't be necessary, but better be safe
	}

	// Go through the workers, starting with the first one, taking all chunks
	// from their bucket before moving on to the next. It's possible that a worker
	// reaches the end of the stream before the following worker does (eof=true),
	// don't advance to the next worker in that case.
	for _, w := range worker {
		for chunk := range w.results {
			// Assemble the list of chunks in the index
			index.Chunks = append(index.Chunks, chunk)
		}
		// Done reading all chunks from this worker, check for any errors
		if w.err != nil {
			return index, w.err
		}
		// Stop if this worker reached the end of the stream (it's not necessarily
		// the last one!)
		if w.eof {
			break
		}
	}
	return index, nil
}

// Parallel chunk worker - Splits a stream and stores start, size and ID in
// a buffered channel to be sync'ed with surrounding chunkers.
type pChunker struct {
	// "bucket" to store chunk results in until they are sync'ed with the previous
	// chunker and then recorded
	results chan IndexChunk

	// single-stream chunker used by this worker
	chunker Chunker
	once    sync.Once
	done    chan struct{}
	err     error
	next    *pChunker
	eof     bool
	sync    IndexChunk
}

func (c *pChunker) start(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			c.err = Interrupted{}
			break loop
		case <-c.done:
			break loop
		default:
		}
		start, b, err := c.chunker.Next()
		if err != nil {
			c.err = err
			break loop
		}
		if len(b) == 0 {
			// TODO: If this worker reached the end of the stream and it's not the
			// last one, we should probable stop all following workers. Meh, shouldn't
			// be happening for large file or save significant CPU for small ones.
			c.eof = true
			break loop
		}
		// Calculate the chunk ID
		id := sha512.Sum512_256(b)

		// Store it in our bucket
		chunk := IndexChunk{Start: start, Size: uint64(len(b)), ID: id}
		c.results <- chunk

		// Check if the next worker already has this chunk, at which point we stop
		// here and let the next continue
		if c.next != nil && c.next.syncWith(chunk) {
			break
		}
	}
	close(c.results)
}

func (c *pChunker) stop() {
	c.once.Do(func() { close(c.done) })
}

// Returns true if the given chunk lines up with one in the current bucket
func (c *pChunker) syncWith(chunk IndexChunk) bool {
	// Read from our bucket until we're past (or match) where the previous worker
	// currently is
	for chunk.Start > c.sync.Start {
		select {
		case c.sync = <-c.results:
		default: // Nothing in my bucket? Move on
			return false
		}
	}
	// Did we find a match with the previous worker. If so the previous worker
	// should stop and this one will keep going
	return chunk.Start == c.sync.Start && chunk.Size == c.sync.Size
}
