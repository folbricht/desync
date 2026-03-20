package desync

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Build an index with a pre-determined set of (potentially repeated) chunks
func indexSequence(ids ...uint8) Index {
	var (
		chunks        = make([]IndexChunk, len(ids))
		start  uint64 = 0
		size   uint64 = 100
	)
	for i, id := range ids {
		chunks[i] = IndexChunk{Start: start, Size: size, ID: ChunkID{id}}
		start += size
	}
	return Index{Chunks: chunks}
}

func TestExtract(t *testing.T) {
	// Make a test file that's guaranteed to have duplicate chunks.
	b, err := os.ReadFile("testdata/chunker.input")
	require.NoError(t, err)
	for range 4 { // Replicate it a few times to make sure we get dupes
		b = append(b, b...)
	}
	b = append(b, make([]byte, 2*ChunkSizeMaxDefault)...) // want to have at least one null-chunk in the input
	in, err := os.CreateTemp("", "in")
	require.NoError(t, err)
	defer os.RemoveAll(in.Name())
	_, err = io.Copy(in, bytes.NewReader(b))
	require.NoError(t, err)
	in.Close()

	// Record the checksum of the input file, used to compare to the output later
	inSum := md5.Sum(b)

	// Chunk the file to get an index
	index, _, err := IndexFromFile(
		context.Background(),
		in.Name(),
		10,
		ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
		NewProgressBar(""),
	)
	require.NoError(t, err)

	// Chop up the input file into a (temporary) local store
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	err = ChopFile(context.Background(), in.Name(), index.Chunks, s, 10, NewProgressBar(""))
	require.NoError(t, err)

	// Make a blank store - used to test a case where no chunk *should* be requested
	blankstore := t.TempDir()
	bs, err := NewLocalStore(blankstore, StoreOptions{})
	require.NoError(t, err)

	// Prepare output files for each test - first a non-existing one
	outdir := t.TempDir()
	out1 := filepath.Join(outdir, "out1")

	// This one is a complete file matching what we expect at the end
	out2, err := os.CreateTemp("", "out2")
	require.NoError(t, err)
	_, err = io.Copy(out2, bytes.NewReader(b))
	require.NoError(t, err)
	out2.Close()
	defer os.Remove(out2.Name())

	// Incomplete or damaged file that has most but not all data
	out3, err := os.CreateTemp("", "out3")
	require.NoError(t, err)
	b[0] ^= 0xff // flip some bits
	b[len(b)-1] ^= 0xff
	b = append(b, 0) // make it longer
	_, err = io.Copy(out3, bytes.NewReader(b))
	require.NoError(t, err)
	out3.Close()
	defer os.Remove(out3.Name())

	tests := map[string]struct {
		outfile string
		store   Store
	}{
		"extract to new file":        {outfile: out1, store: s},
		"extract to complete file":   {outfile: out2.Name(), store: bs},
		"extract to incomplete file": {outfile: out3.Name(), store: s},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			defer os.Remove(test.outfile)
			_, err := AssembleFile(context.Background(), test.outfile, index, test.store, nil,
				AssembleOptions{10, InvalidSeedActionBailOut},
			)
			require.NoError(t, err)

			outBytes, err := os.ReadFile(test.outfile)
			require.NoError(t, err)

			outSum := md5.Sum(outBytes)
			assert.Equal(t, inSum, outSum, "checksum of extracted file doesn't match expected")
		})
	}
}

func TestSeed(t *testing.T) {
	// Prepare different types of data slices that'll be used to assemble target
	// and seed files with varying amount of duplication
	data1, err := os.ReadFile("testdata/chunker.input")
	require.NoError(t, err)
	null := make([]byte, 4*ChunkSizeMaxDefault)
	rand1 := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rand1)
	rand2 := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rand2)

	// Setup a temporary store
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	// Define tests with files with different content, by building files out
	// of sets of byte slices to create duplication or not between the target and
	// its seeds
	tests := map[string]struct {
		target [][]byte
		seeds  [][][]byte
	}{
		"extract without seed": {
			target: [][]byte{rand1, rand2},
			seeds:  nil},
		"extract all null file": {
			target: [][]byte{null, null, null, null, null},
			seeds:  nil},
		"extract repetitive file": {
			target: [][]byte{data1, data1, data1, data1, data1},
			seeds:  nil},
		"extract with single file seed": {
			target: [][]byte{data1, null, null, rand1, null},
			seeds: [][][]byte{
				{data1, null, rand2, rand2, data1},
			},
		},
		"extract with multiple file seeds": {
			target: [][]byte{null, null, rand1, null, data1},
			seeds: [][][]byte{
				{rand2, null, rand2, rand2, data1},
				{data1, null, rand2, rand2, data1},
				{rand2},
			},
		},
		"extract with identical file seed": {
			target: [][]byte{data1, null, rand1, null, data1},
			seeds: [][][]byte{
				{data1, null, rand1, null, data1},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Build the destination file so we can chunk it
			dst, err := os.CreateTemp("", "dst")
			require.NoError(t, err)
			dstBytes := join(test.target...)
			_, err = io.Copy(dst, bytes.NewReader(dstBytes))
			require.NoError(t, err)
			dst.Close()
			defer os.Remove(dst.Name())

			// Record the checksum of the target file, used to compare to the output later
			dstSum := md5.Sum(dstBytes)

			// Chunk the file to get an index
			dstIndex, _, err := IndexFromFile(
				context.Background(),
				dst.Name(),
				10,
				ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
				NewProgressBar(""),
			)
			require.NoError(t, err)

			// Chop up the input file into the store
			err = ChopFile(context.Background(), dst.Name(), dstIndex.Chunks, s, 10, NewProgressBar(""))
			require.NoError(t, err)

			// Build the seed files and indexes then populate the array of seeds
			var seeds []Seed
			for _, f := range test.seeds {
				seedFile, err := os.CreateTemp("", "seed")
				require.NoError(t, err)
				_, err = io.Copy(seedFile, bytes.NewReader(join(f...)))
				require.NoError(t, err)
				seedFile.Close()
				defer os.Remove(seedFile.Name())
				seedIndex, _, err := IndexFromFile(
					context.Background(),
					seedFile.Name(),
					10,
					ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
					NewProgressBar(""),
				)
				require.NoError(t, err)
				seed, err := NewFileSeed(dst.Name(), seedFile.Name(), seedIndex)
				require.NoError(t, err)
				seeds = append(seeds, seed)
			}

			_, err = AssembleFile(context.Background(), dst.Name(), dstIndex, s, seeds,
				AssembleOptions{10, InvalidSeedActionBailOut},
			)
			require.NoError(t, err)
			b, err := os.ReadFile(dst.Name())
			require.NoError(t, err)
			outSum := md5.Sum(b)
			assert.Equal(t, dstSum, outSum, "checksum of extracted file doesn't match expected")
		})
	}

}

// TestSelfSeedInPlace is the same as TestSelfSeed but the target file is
// pre-populated with the correct content before extraction. Every chunk must
// be kept in-place and the self-seed must not cause any re-writes.
func TestSelfSeedInPlace(t *testing.T) {
	// Setup a temporary store
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	// Build a number of fake chunks that can then be used in the test in any order
	type rawChunk struct {
		id   ChunkID
		data []byte
	}
	size := 1024
	numChunks := 10
	chunks := make([]rawChunk, numChunks)

	for i := range numChunks {
		b := make([]byte, size)
		rand.Read(b)
		chunk := NewChunk(b)
		require.NoError(t, s.StoreChunk(chunk))
		chunks[i] = rawChunk{chunk.ID(), b}
	}

	// The target is pre-written with the correct content,
	// so every chunk should be detected as in-place.
	tests := map[string]struct {
		index []int
	}{
		"single chunk": {
			index: []int{0},
		},
		"repeating single chunk": {
			index: []int{0, 0, 0, 0, 0},
		},
		"repeating chunk sequence": {
			index: []int{0, 1, 2, 0, 1, 2, 2},
		},
		"repeating chunk sequence mid file": {
			index: []int{1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3},
		},
		"repeating chunk sequence reversed": {
			index: []int{0, 1, 2, 2, 1, 0},
		},
		"non-repeating chunks": {
			index: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Build an index from the target chunks
			var idx Index
			var b []byte
			for i, p := range test.index {
				chunk := IndexChunk{
					ID:    chunks[p].id,
					Start: uint64(i * size),
					Size:  uint64(size),
				}
				b = append(b, chunks[p].data...)
				idx.Chunks = append(idx.Chunks, chunk)
			}

			// Calculate the expected checksum
			sum := md5.Sum(b)

			// Build a temp target file pre-populated with the correct content
			dst, err := os.CreateTemp("", "dst")
			require.NoError(t, err)
			defer os.Remove(dst.Name())
			_, err = dst.Write(b)
			require.NoError(t, err)
			dst.Close()

			// Extract the file
			stats, err := AssembleFile(context.Background(), dst.Name(), idx, s, nil,
				AssembleOptions{1, InvalidSeedActionBailOut},
			)
			require.NoError(t, err)

			// Compare the checksums to that of the input data
			b, err = os.ReadFile(dst.Name())
			require.NoError(t, err)
			outSum := md5.Sum(b)
			assert.Equal(t, sum, outSum, "checksum of extracted file doesn't match expected")

			// All chunks must be in-place. The in-place check in writeChunk
			// runs before the self-seed lookup, so repeated chunks are not
			// re-written from the self-seed.
			assert.Equal(t, uint64(len(test.index)), stats.ChunksInPlace, "expected all chunks in-place")
		})
	}

}

func join(slices ...[]byte) []byte {
	var out []byte
	for _, b := range slices {
		out = append(out, b...)
	}
	return out
}

func readCaibxFile(t *testing.T, indexLocation string) (idx Index) {
	is, err := NewLocalIndexStore(filepath.Dir(indexLocation))
	require.NoError(t, err)
	defer is.Close()
	indexName := filepath.Base(indexLocation)
	idx, err = is.GetIndex(indexName)
	require.NoError(t, err)
	return idx
}
