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

// TestAssembleIntegration exercises the full assembly pipeline end-to-end,
// combining all source types in a single reconstruction: in-place skips,
// in-place copies (including cycle detection with buffer-break), self-seed,
// file seeds, and store fetches. It uses variable-size chunks so that
// byte-offset calculations, overlap detection, and buffer sizing are tested
// with non-uniform boundaries.
//
// Each scenario writes an "old" file (the in-place seed), then calls
// AssembleFile to reconstruct a different target layout. The test verifies
// both the output content (md5 checksum + file size) and the per-source
// chunk statistics reported by ExtractStats.
func TestAssembleIntegration(t *testing.T) {
	// Create 10 chunks of different sizes filled with random data.
	// Variable sizes ensure offset math in overlaps(), inPlaceCopy.Execute()
	// (buffer sizing), and Tarjan cycle detection are exercised with
	// non-trivial byte boundaries.
	type rawChunk struct {
		id   ChunkID
		data []byte
	}
	chunkSizes := []int{1024, 768, 512, 896, 640, 1152, 384, 1280, 576, 704}
	chunks := make([]rawChunk, len(chunkSizes))
	for i, size := range chunkSizes {
		b := make([]byte, size)
		rand.Read(b)
		id := Digest.Sum(b)
		chunks[i] = rawChunk{id: id, data: b}
	}

	// Named constants for chunk indices to make scenario definitions readable.
	const (
		A = 0 // 1024 bytes
		B = 1 // 768 bytes
		C = 2 // 512 bytes
		D = 3 // 896 bytes
		E = 4 // 640 bytes
		F = 5 // 1152 bytes
		G = 6 // 384 bytes
		H = 7 // 1280 bytes
		X = 8 // 576 bytes
		Y = 9 // 704 bytes
	)

	// buildIndex constructs an Index from chunk references, laying them out
	// contiguously. It also sets ChunkSizeMax to the largest chunk in the
	// index, which is required by newNullChunkSeed inside AssembleFile.
	buildIndex := func(indices ...int) Index {
		ic := make([]IndexChunk, len(indices))
		var offset uint64
		var maxSize uint64
		for i, idx := range indices {
			size := uint64(len(chunks[idx].data))
			ic[i] = IndexChunk{ID: chunks[idx].id, Start: offset, Size: size}
			offset += size
			if size > maxSize {
				maxSize = size
			}
		}
		return Index{
			Index:  FormatIndex{ChunkSizeMax: maxSize},
			Chunks: ic,
		}
	}

	// buildContent returns the raw bytes for a sequence of chunks,
	// used both as file content and as the expected output for verification.
	buildContent := func(indices ...int) []byte {
		var out []byte
		for _, idx := range indices {
			out = append(out, chunks[idx].data...)
		}
		return out
	}

	// buildStore creates a TestStore containing only the specified chunks.
	// Limiting the store to the minimum required set means that if the
	// planner incorrectly routes a chunk to the store (instead of a seed
	// or in-place source), the test fails with ChunkMissing rather than
	// silently succeeding.
	buildStore := func(indices ...int) *TestStore {
		s := &TestStore{Chunks: make(map[ChunkID][]byte)}
		for _, idx := range indices {
			s.Chunks[chunks[idx].id] = chunks[idx].data
		}
		return s
	}

	type scenario struct {
		name            string
		inPlaceIndices  []int // Chunks written to target file before assembly (the "old" content)
		targetIndices   []int // Desired output layout
		fileSeedIndices []int // External file seed content (nil = no file seed)
		storeIndices    []int // Chunks available in the store
		wantInPlace     uint64
		wantFromSeeds   uint64
		wantFromStore   uint64
	}

	scenarios := []scenario{
		// Scenario 1: exercises every source type in one assembly.
		//
		// In-place seed (old file): [A][B][C][D][E] = 3840 bytes
		// Target:                   [B][A][C][F][G][G][D][H] = 6400 bytes
		// File seed:                [F][X][X]
		// Store:                    G, H
		//
		// After truncation to 6400 bytes the file is:
		//   [A:1024][B:768][C:512][D:896][E:640][zeros:2560]
		//
		// Source analysis per target position:
		//   Pos 0 (B): in-place copy — B exists at seed offset 1024, target offset 0.
		//              Part of A↔B cycle (asymmetric sizes: 1024 vs 768).
		//   Pos 1 (A): in-place copy — A exists at seed offset 0, target offset 768.
		//              Part of A↔B cycle. Buffer-break picks B (smaller src).
		//   Pos 2 (C): skip in-place — C is at offset 1792 in both seed and target.
		//   Pos 3 (F): file seed — F is not in the in-place seed, found in file seed.
		//              D's in-place read [2304:3200] overlaps F's write [2304:3456],
		//              so D's read must complete first (enforced by inPlaceReads).
		//   Pos 4 (G): self-seed — G appears at both pos 4 and 5. Self-seed copies
		//              from pos 5 (requires source position > target position).
		//   Pos 5 (G): store — self-seed can't source from itself (p <= startPos).
		//   Pos 6 (D): in-place copy — D at seed offset 2304, target offset 4224.
		//              Independent move, no cycle.
		//   Pos 7 (H): store — H is not in any seed.
		{
			name:            "all source types combined",
			inPlaceIndices:  []int{A, B, C, D, E},
			targetIndices:   []int{B, A, C, F, G, G, D, H},
			fileSeedIndices: []int{F, X, X},
			storeIndices:    []int{G, H},
			wantInPlace:     4, // B (cycle), A (cycle), C (skip), D (independent move)
			wantFromSeeds:   2, // F (file seed), G at pos 4 (self-seed)
			wantFromStore:   2, // G at pos 5, H
		},

		// Scenario 2: in-place seed is larger than the target.
		//
		// In-place seed: [A][B][C][D] = 3200 bytes
		// Target:        [B][A] = 1792 bytes
		//
		// Since the seed (3200) is larger than the target (1792), truncation
		// is deferred until after assembly so that in-place reads can access
		// the full seed data. A↔B form a swap cycle. After assembly, the
		// file is truncated to 1792 bytes.
		{
			name:           "in-place seed larger than target",
			inPlaceIndices: []int{A, B, C, D},
			targetIndices:  []int{B, A},
			storeIndices:   nil,
			wantInPlace:    2, // A↔B swap cycle
			wantFromSeeds:  0,
			wantFromStore:  0,
		},

		// Scenario 3: in-place seed is smaller than the target.
		//
		// In-place seed: [A][B] = 1792 bytes
		// Target:        [A][B][C][D] = 3200 bytes
		//
		// The file is extended (truncated up) to 3200 bytes. A and B are
		// already at the correct offsets and detected by the initial scan.
		// C and D are beyond the seed data and must come from the store.
		{
			name:           "in-place seed smaller than target",
			inPlaceIndices: []int{A, B},
			targetIndices:  []int{A, B, C, D},
			storeIndices:   []int{C, D},
			wantInPlace:    2, // A, B detected in-place by initial scan
			wantFromSeeds:  0,
			wantFromStore:  2, // C, D fetched from store
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			targetPath := filepath.Join(dir, "target")

			// Write the "old" file content — this is what the in-place seed
			// describes. AssembleFile will detect it as non-empty, run the
			// initial scan, and use the in-place seed to rearrange chunks.
			inPlaceContent := buildContent(sc.inPlaceIndices...)
			require.NoError(t, os.WriteFile(targetPath, inPlaceContent, 0644))

			// Create the in-place seed. This wraps a FileSeed where source
			// and destination are the same file.
			inPlaceIdx := buildIndex(sc.inPlaceIndices...)
			inPlaceSeed, err := NewInPlaceSeed(targetPath, inPlaceIdx)
			require.NoError(t, err)
			seeds := []Seed{inPlaceSeed}

			// If the scenario includes a file seed, write it to a separate
			// file and create a FileSeed that maps its chunks by ID.
			if sc.fileSeedIndices != nil {
				seedPath := filepath.Join(dir, "fileseed")
				seedContent := buildContent(sc.fileSeedIndices...)
				require.NoError(t, os.WriteFile(seedPath, seedContent, 0644))
				seedIdx := buildIndex(sc.fileSeedIndices...)
				fs, err := NewFileSeed(targetPath, seedPath, seedIdx)
				require.NoError(t, err)
				seeds = append(seeds, fs)
			}

			// Build the target index (desired output layout) and compute
			// the expected content for verification.
			targetIdx := buildIndex(sc.targetIndices...)
			expected := buildContent(sc.targetIndices...)
			expectedSum := md5.Sum(expected)

			// Build the store with only the chunks that should be fetched
			// from it. Any chunk incorrectly routed here will succeed;
			// any chunk missing from here will fail with ChunkMissing.
			store := buildStore(sc.storeIndices...)

			// Run the full assembly pipeline with 4 concurrent workers.
			stats, err := AssembleFile(
				context.Background(), targetPath, targetIdx, store, seeds,
				AssembleOptions{N: 4, InvalidSeedAction: InvalidSeedActionBailOut},
			)
			require.NoError(t, err)

			// Verify the output file matches the expected content.
			output, err := os.ReadFile(targetPath)
			require.NoError(t, err)
			assert.Equal(t, int64(len(expected)), int64(len(output)), "output file size mismatch")
			outSum := md5.Sum(output)
			assert.Equal(t, expectedSum, outSum, "output checksum mismatch")

			// Verify that chunks were sourced from the expected places.
			// This catches planner bugs where the output is correct but
			// chunks were fetched from the wrong source (e.g. store
			// instead of in-place copy).
			assert.Equal(t, len(sc.targetIndices), stats.ChunksTotal, "ChunksTotal")
			assert.Equal(t, sc.wantInPlace, stats.ChunksInPlace, "ChunksInPlace")
			assert.Equal(t, sc.wantFromSeeds, stats.ChunksFromSeeds, "ChunksFromSeeds")
			assert.Equal(t, sc.wantFromStore, stats.ChunksFromStore, "ChunksFromStore")
		})
	}
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
