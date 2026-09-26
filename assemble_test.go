package desync

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"math"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
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
	tmp := t.TempDir()
	in := filepath.Join(tmp, "in")
	require.NoError(t, os.WriteFile(in, b, 0644))

	// Record the checksum of the input file, used to compare to the output later
	inSum := md5.Sum(b)

	// Chunk the file to get an index
	index, _, err := IndexFromFile(
		context.Background(),
		in,
		10,
		ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
		NewProgressBar(""),
	)
	require.NoError(t, err)

	// Chop up the input file into a (temporary) local store
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	err = ChopFile(context.Background(), in, index.Chunks, s, 10, NewProgressBar(""))
	require.NoError(t, err)

	// Make a blank store - used to test a case where no chunk *should* be requested
	blankstore := t.TempDir()
	bs, err := NewLocalStore(blankstore, StoreOptions{})
	require.NoError(t, err)

	// Prepare output files for each test - first a non-existing one
	out1 := filepath.Join(tmp, "out1")

	// This one is a complete file matching what we expect at the end
	out2 := filepath.Join(tmp, "out2")
	require.NoError(t, os.WriteFile(out2, b, 0644))

	// Incomplete or damaged file that has most but not all data
	b[0] ^= 0xff // flip some bits
	b[len(b)-1] ^= 0xff
	b = append(b, 0) // make it longer
	out3 := filepath.Join(tmp, "out3")
	require.NoError(t, os.WriteFile(out3, b, 0644))

	tests := map[string]struct {
		outfile string
		store   Store
	}{
		"extract to new file":        {outfile: out1, store: s},
		"extract to complete file":   {outfile: out2, store: bs},
		"extract to incomplete file": {outfile: out3, store: s},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
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
			tmp := t.TempDir()
			dst := filepath.Join(tmp, "dst")
			dstBytes := join(test.target...)
			require.NoError(t, os.WriteFile(dst, dstBytes, 0644))

			// Record the checksum of the target file, used to compare to the output later
			dstSum := md5.Sum(dstBytes)

			// Chunk the file to get an index
			dstIndex, _, err := IndexFromFile(
				context.Background(),
				dst,
				10,
				ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
				NewProgressBar(""),
			)
			require.NoError(t, err)

			// Chop up the input file into the store
			err = ChopFile(context.Background(), dst, dstIndex.Chunks, s, 10, NewProgressBar(""))
			require.NoError(t, err)

			// Build the seed files and indexes then populate the array of seeds
			var seeds []Seed
			for i, f := range test.seeds {
				seedFile := filepath.Join(tmp, fmt.Sprintf("seed%d", i))
				require.NoError(t, os.WriteFile(seedFile, join(f...), 0644))
				seedIndex, _, err := IndexFromFile(
					context.Background(),
					seedFile,
					10,
					ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
					NewProgressBar(""),
				)
				require.NoError(t, err)
				seed, err := NewFileSeed(dst, seedFile, seedIndex)
				require.NoError(t, err)
				seeds = append(seeds, seed)
			}

			_, err = AssembleFile(context.Background(), dst, dstIndex, s, seeds,
				AssembleOptions{10, InvalidSeedActionBailOut},
			)
			require.NoError(t, err)
			b, err := os.ReadFile(dst)
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
			dst := filepath.Join(t.TempDir(), "dst")
			require.NoError(t, os.WriteFile(dst, b, 0644))

			// Extract the file
			stats, err := AssembleFile(context.Background(), dst, idx, s, nil,
				AssembleOptions{1, InvalidSeedActionBailOut},
			)
			require.NoError(t, err)

			// Compare the checksums to that of the input data
			b, err = os.ReadFile(dst)
			require.NoError(t, err)
			outSum := md5.Sum(b)
			assert.Equal(t, sum, outSum, "checksum of extracted file doesn't match expected")

			// All chunks must be in-place. The plan generator creates skip
			// placements before matching the self-seed, so repeated chunks
			// are not re-written from the self-seed.
			assert.Equal(t, uint64(len(test.index)), stats.ChunksInPlace, "expected all chunks in-place")
		})
	}

}

// Null segments skip the destination read-back after writing since they have
// no external source that could change during extraction. Assemble a null-heavy
// file over an existing file full of non-zero data to confirm the null sections
// are still written out correctly.
func TestExtractNullsOverExistingFile(t *testing.T) {
	data := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(data)
	null := make([]byte, 4*ChunkSizeMaxDefault)
	b := join(null, data, null)

	tmp := t.TempDir()
	in := filepath.Join(tmp, "in")
	require.NoError(t, os.WriteFile(in, b, 0644))
	inSum := md5.Sum(b)

	index, _, err := IndexFromFile(
		context.Background(),
		in,
		10,
		ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
		NewProgressBar(""),
	)
	require.NoError(t, err)

	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)
	require.NoError(t, ChopFile(context.Background(), in, index.Chunks, s, 10, NewProgressBar("")))

	// Pre-populate the output file with non-zero data so the null sections
	// actually have to be written, not just skipped over in a blank file
	garbage := make([]byte, len(b))
	rand.Read(garbage)
	out := filepath.Join(tmp, "out")
	require.NoError(t, os.WriteFile(out, garbage, 0644))

	_, err = AssembleFile(context.Background(), out, index, s, nil,
		AssembleOptions{10, InvalidSeedActionBailOut})
	require.NoError(t, err)

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, inSum, md5.Sum(got))
}

func join(slices ...[]byte) []byte {
	var out []byte
	for _, b := range slices {
		out = append(out, b...)
	}
	return out
}

// testChunk is a chunk of known content, used to build indexes and file
// content from the same set of chunks.
type testChunk struct {
	id   ChunkID
	data []byte
}

// randomChunks returns chunks of the given sizes filled with random data.
func randomChunks(sizes ...int) []testChunk {
	chunks := make([]testChunk, len(sizes))
	for i, size := range sizes {
		b := make([]byte, size)
		rand.Read(b)
		chunks[i] = testChunk{id: Digest.Sum(b), data: b}
	}
	return chunks
}

// filledChunk returns a chunk of size bytes, all of them set to fill.
func filledChunk(size int, fill byte) testChunk {
	b := bytes.Repeat([]byte{fill}, size)
	return testChunk{id: Digest.Sum(b), data: b}
}

// pickChunks returns the chunks at the given positions, the layout of a file
// in a test scenario.
func pickChunks(chunks []testChunk, positions ...int) []testChunk {
	out := make([]testChunk, len(positions))
	for i, pos := range positions {
		out[i] = chunks[pos]
	}
	return out
}

// chunkIndex lays the chunks out contiguously in the order given, with the
// maximum chunk size AssembleFile needs for its null chunk seed.
func chunkIndex(chunks ...testChunk) Index {
	idx := Index{Chunks: make([]IndexChunk, len(chunks))}
	var start uint64
	for i, c := range chunks {
		size := uint64(len(c.data))
		idx.Chunks[i] = IndexChunk{ID: c.id, Start: start, Size: size}
		idx.Index.ChunkSizeMax = max(idx.Index.ChunkSizeMax, size)
		start += size
	}
	return idx
}

// chunkContent returns the data of the chunks, concatenated. It's both the
// content of a file and the expected output of an assembly.
func chunkContent(chunks ...testChunk) []byte {
	var out []byte
	for _, c := range chunks {
		out = append(out, c.data...)
	}
	return out
}

// chunkStore returns a store holding only the given chunks, so that a chunk
// the plan routes to the store unexpectedly fails with ChunkMissing rather
// than succeeding silently.
func chunkStore(chunks ...testChunk) *TestStore {
	s := &TestStore{Chunks: make(map[ChunkID][]byte, len(chunks))}
	for _, c := range chunks {
		s.Chunks[c.id] = c.data
	}
	return s
}

// writeChunkFile writes the chunks' content to a file in a new temporary
// directory and returns its path.
func writeChunkFile(t *testing.T, chunks ...testChunk) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(path, chunkContent(chunks...), 0644))
	return path
}

// testAssembleOptions are the options the assembly tests run with: several
// workers, and no tolerance for seeds that don't match their data.
var testAssembleOptions = AssembleOptions{N: 4, InvalidSeedAction: InvalidSeedActionBailOut}

// TestAssembleIntegration exercises the full assembly pipeline end-to-end,
// combining all source types in a single reconstruction: in-place skips,
// in-place copies (including cycles broken by buffering), self-seed,
// file seeds, and store fetches. It uses variable-size chunks so that
// byte-offset calculations, overlap detection, and buffer sizing are tested
// with non-uniform boundaries.
//
// Each scenario writes an "old" file (the in-place seed), then calls
// AssembleFile to reconstruct a different target layout. The test verifies
// both the output content (md5 checksum + file size) and the per-source
// chunk statistics reported by ExtractStats.
func TestAssembleIntegration(t *testing.T) {
	// Chunks of different sizes make sure the offset math in overlap
	// detection, the buffer sizing of in-place moves and cycle breaking
	// are exercised with non-trivial byte boundaries.
	chunks := randomChunks(1024, 768, 512, 896, 640, 1152, 384, 1280, 576, 704)

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

	// The scenarios below refer to the chunks by position.
	buildIndex := func(positions ...int) Index {
		return chunkIndex(pickChunks(chunks, positions...)...)
	}
	buildContent := func(positions ...int) []byte {
		return chunkContent(pickChunks(chunks, positions...)...)
	}
	buildStore := func(positions ...int) *TestStore {
		return chunkStore(pickChunks(chunks, positions...)...)
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
		//              Part of A↔B cycle, broken by buffering A.
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

		// Scenario 4: data inserted at the front.
		//
		// In-place seed: [A][B][C] = 2304 bytes
		// Target:        [H][A][B][C] = 3584 bytes
		//
		// There is no cycle, but A is written over the sources of B and C,
		// so those have to be read first.
		{
			name:           "insert at the front",
			inPlaceIndices: []int{A, B, C},
			targetIndices:  []int{H, A, B, C},
			storeIndices:   []int{H},
			wantInPlace:    3, // A, B, C moved
			wantFromSeeds:  0,
			wantFromStore:  1, // H
		},

		// Scenario 5: one chunk overwrites the sources of two moves.
		//
		// In-place seed: [C][G][A] = 1920 bytes
		// Target:        [H][C][G] = 2176 bytes
		//
		// H from the store is written to [0:1280] which holds the sources
		// of both C and G. It has to wait for both to be read.
		{
			name:           "store chunk over two move sources",
			inPlaceIndices: []int{C, G, A},
			targetIndices:  []int{H, C, G},
			storeIndices:   []int{H},
			wantInPlace:    2, // C, G moved
			wantFromSeeds:  0,
			wantFromStore:  1, // H
		},

		// Scenario 6: nested cycles.
		//
		// In-place seed: [H][G][X] = 2240 bytes
		// Target:        [G][X][C][H] = 2752 bytes
		//
		// H is written over the sources of G and X, and G and X are written
		// over the source of H. That's two cycles sharing H, which can't
		// be broken by buffering a single chunk per cycle blindly.
		{
			name:           "nested cycles",
			inPlaceIndices: []int{H, G, X},
			targetIndices:  []int{G, X, C, H},
			storeIndices:   []int{C},
			wantInPlace:    3, // G, X, H moved
			wantFromSeeds:  0,
			wantFromStore:  1, // C
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
			inPlaceSeed, err := NewFileSeed(targetPath, targetPath, inPlaceIdx)
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
				testAssembleOptions,
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

// Whether a seed is used in place depends on its data file being the target
// of the assembly, not on how the seed was created.
func TestAssembleSeedOnTarget(t *testing.T) {
	chunks := randomChunks(1024, 768)
	a, b := chunks[0], chunks[1]
	oldIdx, oldContent := chunkIndex(a, b), chunkContent(a, b)
	newIdx, expected := chunkIndex(b, a), chunkContent(b, a)

	// The store is empty, all data has to come from the seed
	store := chunkStore()

	t.Run("file seed on the target", func(t *testing.T) {
		// Reading from the target while writing to it would overwrite
		// chunks before they're copied. The seed has to be used in place.
		target := filepath.Join(t.TempDir(), "target")
		require.NoError(t, os.WriteFile(target, oldContent, 0644))
		seed, err := NewFileSeed(target, target, oldIdx)
		require.NoError(t, err)

		stats, err := AssembleFile(context.Background(), target, newIdx, store, []Seed{seed}, testAssembleOptions)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), stats.ChunksInPlace)

		got, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	})

	t.Run("file seed of another file", func(t *testing.T) {
		// Like extracting into a temporary file that replaces the seed's
		// file later. The seed is read from, not rearranged.
		dir := t.TempDir()
		old := filepath.Join(dir, "old")
		require.NoError(t, os.WriteFile(old, oldContent, 0644))
		target := filepath.Join(dir, "target")
		seed, err := NewFileSeed(target, old, oldIdx)
		require.NoError(t, err)

		stats, err := AssembleFile(context.Background(), target, newIdx, store, []Seed{seed}, testAssembleOptions)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), stats.ChunksFromSeeds)

		got, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, expected, got)

		// The seed's own file is left alone
		got, err = os.ReadFile(old)
		require.NoError(t, err)
		require.Equal(t, oldContent, got)
	})
}

// TestAssembleInPlaceRandomized rearranges random in-place layouts into random
// targets. Every chunk available in the old file has to be taken from there,
// and the output has to match the target regardless of how the moves overlap.
func TestAssembleInPlaceRandomized(t *testing.T) {
	rng := mrand.New(mrand.NewPCG(1, 2))

	// A pool of chunks with different sizes and random content
	sizes := make([]int, 8)
	for i := range sizes {
		sizes[i] = 64 * (rng.IntN(8) + 1)
	}
	pool := randomChunks(sizes...)
	store := chunkStore(pool...)

	randomLayout := func() []int {
		layout := make([]int, rng.IntN(10)+1)
		for i := range layout {
			layout[i] = rng.IntN(len(pool))
		}
		return layout
	}

	// Without a memory budget for buffers, cycles are broken with the store
	// instead. The chunks taken from there aren't in place.
	for _, budget := range []int64{math.MaxInt64, 0, 512} {
		t.Run(fmt.Sprintf("budget %d", budget), func(t *testing.T) {
			setBufferBudget(t, budget)
			dir := t.TempDir()
			for i := range 300 {
				oldLayout, newLayout := randomLayout(), randomLayout()
				oldChunks := pickChunks(pool, oldLayout...)
				newChunks := pickChunks(pool, newLayout...)
				oldIdx, oldContent := chunkIndex(oldChunks...), chunkContent(oldChunks...)
				newIdx, expected := chunkIndex(newChunks...), chunkContent(newChunks...)

				target := filepath.Join(dir, fmt.Sprintf("target%d", i))
				require.NoError(t, os.WriteFile(target, oldContent, 0644))
				seed, err := NewFileSeed(target, target, oldIdx)
				require.NoError(t, err)

				stats, err := AssembleFile(context.Background(), target, newIdx, store, []Seed{seed},
					testAssembleOptions)
				require.NoError(t, err, "old layout %v, new layout %v", oldLayout, newLayout)

				output, err := os.ReadFile(target)
				require.NoError(t, err)
				require.Equal(t, expected, output, "old layout %v, new layout %v", oldLayout, newLayout)

				var wantInPlace uint64
				for _, c := range newLayout {
					if slices.Contains(oldLayout, c) {
						wantInPlace++
					}
				}
				if budget == math.MaxInt64 {
					require.Equal(t, wantInPlace, stats.ChunksInPlace, "old layout %v, new layout %v", oldLayout, newLayout)
				} else {
					require.LessOrEqual(t, stats.ChunksInPlace, wantInPlace, "old layout %v, new layout %v", oldLayout, newLayout)
				}
			}
		})
	}
}

// setBufferBudget sets the memory in-place buffers may hold for the test.
func setBufferBudget(t *testing.T, budget int64) {
	t.Helper()
	orig := inPlaceBufferBudget
	inPlaceBufferBudget = func() int64 { return budget }
	t.Cleanup(func() { inPlaceBufferBudget = orig })
}

// A swap needs one of the chunks held in memory. Without any memory for it,
// that chunk comes from the store.
func TestAssembleSwapWithoutBufferBudget(t *testing.T) {
	setBufferBudget(t, 0)

	chunks := randomChunks(1024, 768)
	a, b := chunks[0], chunks[1]

	target := writeChunkFile(t, a, b)
	seed, err := NewFileSeed(target, target, chunkIndex(a, b))
	require.NoError(t, err)

	stats, err := AssembleFile(context.Background(), target, chunkIndex(b, a), chunkStore(a, b),
		[]Seed{seed}, testAssembleOptions)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.ChunksInPlace)
	assert.Equal(t, uint64(1), stats.ChunksFromStore)

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, chunkContent(b, a), got)
}

// A target that's larger than the output is shrunk to the output's size,
// keeping the chunks that are already in place.
func TestExtractOverLargerFile(t *testing.T) {
	chunks := randomChunks(1024, 768)
	a, b := chunks[0], chunks[1]
	target := writeChunkFile(t, a, b)

	// The store is empty, the chunk has to be found in place
	stats, err := AssembleFile(context.Background(), target, chunkIndex(a), chunkStore(), nil,
		testAssembleOptions)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.ChunksInPlace)

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, a.data, got)
}

// An index of a zero-length file has no chunks. Extracting it should produce
// an empty output file rather than panic.
func TestExtractEmptyIndex(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")

	// Write some data into the output file to confirm it gets truncated
	require.NoError(t, os.WriteFile(out, []byte("old content"), 0644))

	index := Index{
		Index: FormatIndex{
			FeatureFlags: CaFormatSHA512256,
			ChunkSizeMin: ChunkSizeMinDefault,
			ChunkSizeAvg: ChunkSizeAvgDefault,
			ChunkSizeMax: ChunkSizeMaxDefault,
		},
	}
	store, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	_, err = AssembleFile(context.Background(), out, index, store, nil,
		AssembleOptions{10, InvalidSeedActionBailOut})
	require.NoError(t, err)

	b, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Empty(t, b)
}
