package desync

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/pkg/errors"

	"io"
)

// Index represents the content of an index file
type Index struct {
	Index  FormatIndex
	Chunks []IndexChunk
}

// IndexChunk is a table entry in an index file containing the chunk ID (SHA256)
// Similar to an FormatTableItem but with Start and Size instead of just offset to
// make it easier to use throughout the application.
type IndexChunk struct {
	ID    ChunkID
	Start uint64
	Size  uint64
}

// IndexFromReader parses a caibx structure (from a reader) and returns a populated Caibx
// object
func IndexFromReader(r io.Reader) (c Index, err error) {
	d := NewFormatDecoder(bufio.NewReader(r))
	var ok bool
	// Read the index
	e, err := d.Next()
	if err != nil {
		return c, errors.Wrap(err, "reading index")
	}

	c.Index, ok = e.(FormatIndex)
	if !ok {
		return c, errors.New("input is not an index file")
	}

	if c.Index.FeatureFlags&CaFormatSHA512256 == 0 {
		return c, errors.New("only SHA512/256 is supported")
	}

	// Read the table
	e, err = d.Next()
	if err != nil {
		return c, errors.Wrap(err, "reading chunk table")
	}
	table, ok := e.(FormatTable)
	if !ok {
		return c, errors.New("index table not found in input")
	}

	// Convert the chunk table into a different format for easier use
	c.Chunks = make([]IndexChunk, len(table.Items))
	var lastOffset uint64
	for i, r := range table.Items {
		c.Chunks[i].ID = r.Chunk
		c.Chunks[i].Start = lastOffset
		c.Chunks[i].Size = r.Offset - lastOffset
		lastOffset = r.Offset
		// Check the max size of the chunk only. The min apperently doesn't apply
		// to the last chunk.
		if c.Chunks[i].Size > c.Index.ChunkSizeMax {
			return c, fmt.Errorf("chunk size %d is larger than maximum %d", c.Chunks[i].Size, c.Index.ChunkSizeMax)
		}
	}
	return
}

// WriteTo writes the index and chunk table into a stream
func (i *Index) WriteTo(w io.Writer) (int64, error) {
	index := FormatIndex{
		FormatHeader: FormatHeader{Size: 48, Type: CaFormatIndex},
		FeatureFlags: i.Index.FeatureFlags,
		ChunkSizeMin: i.Index.ChunkSizeMin,
		ChunkSizeAvg: i.Index.ChunkSizeAvg,
		ChunkSizeMax: i.Index.ChunkSizeMax,
	}

	bw := bufio.NewWriter(w)
	d := NewFormatEncoder(bw)
	n, err := d.Encode(index)
	if err != nil {
		return n, err
	}

	// Convert the chunk list back into the format used in index files (with offset
	// instead of start+size)
	var offset uint64
	fChunks := make([]FormatTableItem, len(i.Chunks))
	for p, c := range i.Chunks {
		offset += c.Size
		fChunks[p] = FormatTableItem{Chunk: c.ID, Offset: offset}
	}
	table := FormatTable{
		FormatHeader: FormatHeader{Size: math.MaxUint64, Type: CaFormatTable},
		Items:        fChunks,
	}
	n1, err := d.Encode(table)

	if err := bw.Flush(); err != nil {
		return n + n1, err
	}
	return n + n1, err
}

// Length returns the total (uncompressed) size of the indexed stream
func (i *Index) Length() int64 {
	if len(i.Chunks) < 1 {
		return 0
	}
	lastChunk := i.Chunks[len(i.Chunks)-1]
	return int64(lastChunk.Start + lastChunk.Size)
}

// ChunkStream splits up a blob into chunks using the provided chunker (single stream),
// populates a store with the chunks and returns an index. Hashing and compression
// is performed in n goroutines while the hashing algorithm is performed serially.
func ChunkStream(ctx context.Context, c Chunker, ws WriteStore, n int) (Index, error) {
	type chunkJob struct {
		num   int
		start uint64
		b     []byte
	}
	var (
		stop    bool
		wg      sync.WaitGroup
		mu      sync.Mutex
		pErr    error
		in      = make(chan chunkJob)
		results = make(map[int]IndexChunk)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := NewChunkStorage(ws)

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if pErr == nil {
			pErr = err
		}
		cancel()
	}

	// All the chunks are processed in parallel, but we need to preserve the
	// order for later. So add the chunking results to a map, indexed by
	// the chunk number so we can rebuild it in the right order when done
	recordResult := func(num int, r IndexChunk) {
		mu.Lock()
		defer mu.Unlock()
		results[num] = r
	}

	// Start the workers responsible for checksum calculation, compression and
	// storage (if required). Each job comes with a chunk number for sorting later
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for c := range in {
				// Create a chunk object, needed to calculate the checksum
				chunk := NewChunkFromUncompressed(c.b)

				// Record the index row
				idxChunk := IndexChunk{Start: c.start, Size: uint64(len(c.b)), ID: chunk.ID()}
				recordResult(c.num, idxChunk)

				if err := s.StoreChunk(chunk); err != nil {
					recordError(err)
					continue
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop if there are any errors. To keep the index list in
	// order, we calculate the checksum here before handing	them over to the
	// workers for compression and storage. That could probablybe optimized further
	var num int // chunk #, so we can re-assemble the index in the right order later
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			stop = true
			break
		default:
		}
		start, b, err := c.Next()
		if err != nil {
			return Index{}, err
		}
		if len(b) == 0 {
			break
		}

		// Send it off for compression and storage
		in <- chunkJob{num: num, start: start, b: b}

		num++
	}
	close(in)
	wg.Wait()

	// Everything has settled, now see if something happend that would invalidate
	// the results. Either an error or an interrupt by the user. We don't just
	// want to bail out when it happens and abandon any running goroutines that
	// might still be writing/processing chunks. Only stop here it's safe like here.
	if stop {
		return Index{}, pErr
	}

	// All the chunks have been processed and are stored in a map. Now build a
	// list in the correct order to be used in the index below
	chunks := make([]IndexChunk, len(results))
	for i := 0; i < len(results); i++ {
		chunks[i] = results[i]
	}

	// Build and return the index
	index := Index{
		Index: FormatIndex{
			FeatureFlags: CaFormatExcludeNoDump | CaFormatSHA512256,
			ChunkSizeMin: c.Min(),
			ChunkSizeAvg: c.Avg(),
			ChunkSizeMax: c.Max(),
		},
		Chunks: chunks,
	}
	return index, pErr
}
