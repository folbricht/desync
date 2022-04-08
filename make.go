package desync

import (
	"context"
	"crypto"
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
	pb ProgressBar,
) (Index, ChunkingStats, error) {

	stats := ChunkingStats{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var digestFlag uint64
	if Digest.Algorithm() == crypto.SHA512_256 {
		digestFlag = CaFormatSHA512256
	}

	index := Index{
		Index: FormatIndex{
			FeatureFlags: CaFormatExcludeNoDump | digestFlag,
			ChunkSizeMin: min,
			ChunkSizeAvg: avg,
			ChunkSizeMax: max,
		},
	}

	// If our input file has a catar header, copy its feature flags into the index
	f, err := os.Open(name)
	if err != nil {
		return index, stats, err
	}
	fDecoder := NewFormatDecoder(f)
	piece, err := fDecoder.Next()
	if err == nil {
		switch t := piece.(type) {
		case FormatEntry:
			index.Index.FeatureFlags |= t.FeatureFlags
		}
	}
	f.Close()

	size, err := GetFileSize(name)
	if err != nil {
		return index, stats, err
	}

	// Adjust n if it's a small file that doesn't have n*max bytes
	nn := size/max + 1
	if nn < uint64(n) {
		n = int(nn)
	}
	span := size / uint64(n) // initial spacing between chunkers

	// Setup and start the progressbar if any
	pb.SetTotal(int(size))
	pb.Start()
	defer pb.Finish()

	// Null chunks is produced when a large section of null bytes is chunked. There are no
	// split points in those sections so it's always of max chunk size. Used for optimizations
	// when chunking files with large empty sections.
	nullChunk := NewNullChunk(max)

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
			chunker:   c,
			results:   make(chan IndexChunk, mChunks),
			done:      make(chan struct{}),
			offset:    start,
			stats:     &stats,
			nullChunk: nullChunk,
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
			pb.Set(int(chunk.Start + chunk.Size))
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

	// Null chunk for optimizing chunking sparse files
	nullChunk *NullChunk
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
			// last one, we should probably stop all following workers. Meh, shouldn't
			// be happening for large file or save significant CPU for small ones.
			c.eof = true
			return
		}
		// Calculate the chunk ID
		id := Digest.Sum(b)

		// Store it in our bucket
		chunk := IndexChunk{Start: start, Size: uint64(len(b)), ID: id}
		c.results <- chunk

		// Check if the next worker already has this chunk, at which point we stop
		// here and let the next continue
		if c.next != nil {
			inSync, zeroes := c.next.syncWith(chunk)
			if inSync {
				return
			}
			numNullChunks := int(int(zeroes) / len(c.nullChunk.Data))
			if numNullChunks > 0 {
				if err := c.chunker.Advance(numNullChunks * len(c.nullChunk.Data)); err != nil {
					c.err = err
					return
				}
				nc := chunk
				for i := 0; i < numNullChunks; i++ {
					nc = IndexChunk{Start: nc.Start + nc.Size, Size: uint64(len(c.nullChunk.Data)), ID: c.nullChunk.ID}
					c.results <- nc
					zeroes -= uint64(len(c.nullChunk.Data))
				}
			}
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

// Returns true if the given chunk lines up with one in the current bucket. Also returns
// the number of zero bytes this chunker has found from 'chunk'. This helps the previous
// chunker to skip chunking over those areas and put a null-chunks (always max size) in
// place instead.
func (c *pChunker) syncWith(chunk IndexChunk) (bool, uint64) {
	// Read from our bucket until we're past (or match) where the previous worker
	// currently is
	var prev IndexChunk
	for chunk.Start > c.sync.Start {
		prev = c.sync
		var ok bool
		select {
		case c.sync, ok = <-c.results:
			if !ok {
				return false, 0
			}
		default: // Nothing in my bucket? Move on
			return false, 0
		}
	}

	// Did we find a match with the previous worker? If so, the previous worker
	// should stop and this one will keep going
	if chunk.Start == c.sync.Start && chunk.Size == c.sync.Size {
		return true, 0
	}

	// The previous chunker didn't sync up with this one, but perhaps we're in a large area
	// of nulls (chunk split points are unlikely to line up). If so we can tell the previous
	// chunker how many nulls are coming so it doesn't need to do all the work again and can
	// skip ahead, producing null-chunks of max size.
	var n uint64
	if c.sync.ID == c.nullChunk.ID && prev.ID == c.nullChunk.ID {
		// We know there're at least some null chunks in front of the previous chunker. Let's
		// see if there are more in our bucket so we can tell the previous chunker how far to
		// skip ahead.
		n = prev.Start + prev.Size - chunk.Start
		for {
			var ok bool
			select {
			case c.sync, ok = <-c.results:
				if !ok {
					return false, n
				}
			default: // Nothing more in my bucket? Move on
				return false, n
			}
			if c.sync.ID != c.nullChunk.ID { // Hit the end of the null chunks, stop here
				break
			}
			n += uint64(len(c.nullChunk.Data))
		}
	}
	return false, n
}

// ChunkingStats is used to report statistics of a parallel chunking operation.
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
