package desync

import (
	"context"
	"crypto/sha512"
	"os"
	"sync"
)

// IndexFromFile chunks a file in parallel and returns an index. It does not
// store chunks! Each concurrent chunker starts filesize/n bytes apart and
// splits independently. The main goroutine then starts with the first chunker
// and attempts to align the produced chunks with the 2nd one, at which point
// the 1st chunker is stopped and the 2nd attempts to sync with the chunks
// produced by the 3rd and so on. If they do not sync (perhaps because no
// boundaries are found by the rolling hash algorithm), the first one will just
// keep going. In that case some CPU and I/O will be wasted, but the resulting
// index will still be correct.
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

	// Create and start the workers
	var worker []*pChunker
	for i := 0; i < n; i++ {
		f, err := os.Open(name)
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
		go p.start(ctx)
		defer p.stop()
		worker = append(worker, p)
	}

	// Go through the workers, starting with the first one being the master/authority.
	// If one of the produced chunks matches one that the next worker has produced,
	// then we make the next one the master and stop the current worker.
	for i := 0; i < n; i++ {
		var (
			sync      = IndexChunk{}
			foundSync bool
		)
		for chunk := range worker[i].results {
			// Any chunk produced by the current master is correct and is recorded
			// in the array of chunks that will be returned.
			index.Chunks = append(index.Chunks, chunk)

			// If there are no more workers, just keep going finishing off the chunking
			// using the current worker
			if i+1 >= n {
				continue
			}

			// Did we advance past the current sync point? If so, grab chunks from
			// the next worker until it either sync's up or it is ahead of us
		nextSync:
			for chunk.Start > sync.Start {
				select {
				case sync = <-worker[i+1].results:
				default: // next worker doesn't have chunks in its bucket?
					break nextSync // Move on and try again later
				}
			}

			// Did we find sync point with the next worker?
			if chunk.Start == sync.Start && chunk.Size == sync.Size {
				foundSync = true
				break
			}
		}
		// The next worker is going to be the master, stop the current master
		worker[i].stop()

		// Check for any errors
		if worker[i].err != nil {
			return index, worker[i].err
		}

		// If we got here without a sync match, that means the current master
		// reached the end. So don't go to the next worker
		if !foundSync {
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
			break loop
		}
		// Calculate the chunk ID
		id := sha512.Sum512_256(b)

		// Record it in our bucket
		c.results <- IndexChunk{Start: start, Size: uint64(len(b)), ID: id}
	}
	close(c.results)
}

func (c *pChunker) stop() {
	c.once.Do(func() { close(c.done) })
}
