package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// IndexFromFile chunks a file in parallel and returns an index. It does not
// store chunks! Each concurrent chunker starts filesize/n bytes apart and
// splits independently. Each chunk worker tries to sync with it's next
// neighbor and if successful stops processing letting the next one continue.
// The main routine reads and assembles a list of (confirmed) chunks from the
// workers, starting with the first worker.
// This algorithm wastes some CPU and I/O if the data doesn't contain chunk
// boundaries, for example if the whole file contains nil bytes. If progress
// is not nil, it'll be updated with the confirmed chunk position in the file.
func IndexFromFile(ctx context.Context,
	name string,
	n int,
	min, avg, max uint64,
	progress func(uint64),
) (Index, ChunkingStats, error) {

	index := Index{
		Index: FormatIndex{
			FeatureFlags: CaFormatExcludeNoDump | CaFormatSHA512256,
			ChunkSizeMin: min,
			ChunkSizeAvg: avg,
			ChunkSizeMax: max,
		},
	}

	stats := ChunkingStats{}

	// Adjust n if it's a small file that doesn't have n*max bytes
	info, err := os.Stat(name)
	if err != nil {
		return index, stats, err
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
			return index, stats, err
		}
		defer f.Close()
		start := span * uint64(i)       // starting position for this chunker
		mChunks := (size-start)/min + 1 // max # of chunks this worker can produce
		s, err := f.Seek(int64(start), io.SeekStart)
		if err != nil {
			return index, stats, err
		}
		if uint64(s) != start {
			return index, stats, fmt.Errorf("requested seek to position %d, but got %d", start, s)
		}
		c, err := NewChunker(f, min, avg, max)
		if err != nil {
			return index, stats, err
		}
		p := &pChunker{
			chunker: c,
			results: make(chan IndexChunk, mChunks),
			done:    make(chan struct{}),
			offset:  start,
			stats:   &stats,
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
			if progress != nil {
				progress(chunk.Start + chunk.Size)
			}
			stats.incAccepted()
		}
		// Done reading all chunks from this worker, check for any errors
		if w.err != nil {
			return index, stats, w.err
		}
		// Stop if this worker reached the end of the stream (it's not necessarily
		// the last worker!)
		if w.eof {
			break
		}
	}
	return index, stats, nil
}

// Parallel chunk worker - Splits a stream and stores start, size and ID in
// a buffered channel to be sync'ed with surrounding chunkers.
type pChunker struct {
	// "bucket" to store chunk results in until they are sync'ed with the previous
	// chunker and then recorded
	results chan IndexChunk

	// single-stream chunker used by this worker
	chunker Chunker

	// starting position in the stream for this worker, needed to calculate
	// the absolute position of every boundry that is returned
	offset uint64

	once  sync.Once
	done  chan struct{}
	err   error
	next  *pChunker
	eof   bool
	sync  IndexChunk
	stats *ChunkingStats
}

func (c *pChunker) start(ctx context.Context) {
	defer close(c.results)
	defer c.stop()
	for {
		select {
		case <-ctx.Done():
			c.err = Interrupted{}
			return
		case <-c.done:
			return
		default: // We weren't asked to stop and weren't interrupted, carry on
		}
		start, b, err := c.chunker.Next()
		if err != nil {
			c.err = err
			return
		}
		c.stats.incProduced()
		start += c.offset
		if len(b) == 0 {
			// TODO: If this worker reached the end of the stream and it's not the
			// last one, we should probable stop all following workers. Meh, shouldn't
			// be happening for large file or save significant CPU for small ones.
			c.eof = true
			return
		}
		// Calculate the chunk ID
		id := sha512.Sum512_256(b)

		// Store it in our bucket
		chunk := IndexChunk{Start: start, Size: uint64(len(b)), ID: id}
		c.results <- chunk

		// Check if the next worker already has this chunk, at which point we stop
		// here and let the next continue
		if c.next != nil && c.next.syncWith(chunk) {
			return
		}

		// If the next worker has stopped and has no more chunks in its bucket,
		// we want to skip that and try to sync with the one after
		if c.next != nil && !c.next.active() && len(c.next.results) == 0 {
			c.next = c.next.next
		}
	}
}

func (c *pChunker) stop() {
	c.once.Do(func() { close(c.done) })
}

func (c *pChunker) active() bool {
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

// Returns true if the given chunk lines up with one in the current bucket
func (c *pChunker) syncWith(chunk IndexChunk) bool {
	// Read from our bucket until we're past (or match) where the previous worker
	// currently is
	for chunk.Start > c.sync.Start {
		var ok bool
		select {
		case c.sync, ok = <-c.results:
			if !ok {
				return false
			}
		default: // Nothing in my bucket? Move on
			return false
		}
	}
	// Did we find a match with the previous worker. If so the previous worker
	// should stop and this one will keep going
	return chunk.Start == c.sync.Start && chunk.Size == c.sync.Size
}

type ChunkingStats struct {
	ChunksAccepted uint64
	ChunksProduced uint64
}

func (s *ChunkingStats) incAccepted() {
	atomic.AddUint64(&s.ChunksAccepted, 1)
}

func (s *ChunkingStats) incProduced() {
	atomic.AddUint64(&s.ChunksProduced, 1)
}
