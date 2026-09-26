package desync

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelfSeedPlanSteps(t *testing.T) {
	tests := map[string]struct {
		index    Index
		expected []string
	}{
		"all unique chunks": {
			index: indexSequence(0x01, 0x02, 0x03),
			expected: []string{
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [0:100]",
				"Store: Copy 0200000000000000000000000000000000000000000000000000000000000000 to [100:200]",
				"Store: Copy 0300000000000000000000000000000000000000000000000000000000000000 to [200:300]",
			},
		},
		"single chunk": {
			index: indexSequence(0x01),
			expected: []string{
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [0:100]",
			},
		},
		"repeated pair at end": {
			// Sequence: 01 02 03 01 02 01 02
			// Positions 0,1 copy from 5,6; positions 3,4 copy from 5,6;
			// positions 2,5,6 come from store.
			index: indexSequence(0x01, 0x02, 0x03, 0x01, 0x02, 0x01, 0x02),
			expected: []string{
				"SelfSeed: Copy [500:700] to [0:200]",
				"Store: Copy 0300000000000000000000000000000000000000000000000000000000000000 to [200:300]",
				"SelfSeed: Copy [500:700] to [300:500]",
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [500:600]",
				"Store: Copy 0200000000000000000000000000000000000000000000000000000000000000 to [600:700]",
			},
		},
		"full duplicate sequence": {
			// Sequence: 01 02 03 01 02 03
			// Positions 0-2 copy from 3-5; positions 3-5 come from store.
			index: indexSequence(0x01, 0x02, 0x03, 0x01, 0x02, 0x03),
			expected: []string{
				"SelfSeed: Copy [300:600] to [0:300]",
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [300:400]",
				"Store: Copy 0200000000000000000000000000000000000000000000000000000000000000 to [400:500]",
				"Store: Copy 0300000000000000000000000000000000000000000000000000000000000000 to [500:600]",
			},
		},
		"same chunk repeated": {
			// Sequence: 01 01 01
			// Position 2 comes from store, then positions 0 and 1 each
			// self-seed from position 2.
			index: indexSequence(0x01, 0x01, 0x01),
			expected: []string{
				"SelfSeed: Copy [200:300] to [0:100]",
				"SelfSeed: Copy [200:300] to [100:200]",
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [200:300]",
			},
		},
		"single repeated chunk": {
			// Sequence: 01 02 01
			// Position 0 copies from 2; positions 1,2 come from store.
			index: indexSequence(0x01, 0x02, 0x01),
			expected: []string{
				"SelfSeed: Copy [200:300] to [0:100]",
				"Store: Copy 0200000000000000000000000000000000000000000000000000000000000000 to [100:200]",
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [200:300]",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, planStrings("test", test.index))
		})
	}
}

func TestInPlaceChunkDetection(t *testing.T) {
	// Create chunk data and compute their IDs
	data1 := make([]byte, 100)
	data1[0] = 0x01
	id1 := Digest.Sum(data1)

	data2 := make([]byte, 100)
	data2[0] = 0x02
	id2 := Digest.Sum(data2)

	data3 := make([]byte, 100)
	data3[0] = 0x03
	id3 := Digest.Sum(data3)

	idx := Index{
		Chunks: []IndexChunk{
			{ID: id1, Start: 0, Size: 100},
			{ID: id2, Start: 100, Size: 100},
			{ID: id3, Start: 200, Size: 100},
		},
	}

	// Create a target file where chunks 0 and 2 match but chunk 1 does not
	target := filepath.Join(t.TempDir(), "target")
	f, err := os.Create(target)
	require.NoError(t, err)

	_, err = f.Write(data1) // chunk 0: correct
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 100)) // chunk 1: wrong data
	require.NoError(t, err)
	_, err = f.Write(data3) // chunk 2: correct
	require.NoError(t, err)
	require.NoError(t, f.Close())

	got := planStrings(target, idx, planWithTargetIsBlank(false))

	cid2 := ChunkID(id2)
	expected := []string{
		"InPlace: Skip [0:100]",
		fmt.Sprintf("Store: Copy %s to [100:200]", &cid2),
		"InPlace: Skip [200:300]",
	}
	require.Equal(t, expected, got)

	// Subtest: all chunks match in-place — consecutive ones should merge
	t.Run("consecutive merge", func(t *testing.T) {
		target2 := filepath.Join(t.TempDir(), "target2")
		f2, err := os.Create(target2)
		require.NoError(t, err)
		_, err = f2.Write(data1)
		require.NoError(t, err)
		_, err = f2.Write(data2)
		require.NoError(t, err)
		_, err = f2.Write(data3)
		require.NoError(t, err)
		require.NoError(t, f2.Close())

		require.Equal(t, []string{"InPlace: Skip [0:300]"},
			planStrings(target2, idx, planWithTargetIsBlank(false)))
	})
}

func TestFileSeedPlanSteps(t *testing.T) {
	tests := map[string]struct {
		target   Index
		seed     Index
		expected []string
	}{
		"basic matching": {
			// Target: 01, 02, 03, 04
			// Seed:   02, 03
			// Chunks 1-2 from seed, 0 and 3 from store.
			target: indexSequence(0x01, 0x02, 0x03, 0x04),
			seed:   indexSequence(0x02, 0x03),
			expected: []string{
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [0:100]",
				"FileSeed(seed): Copy to [100:300]",
				"Store: Copy 0400000000000000000000000000000000000000000000000000000000000000 to [300:400]",
			},
		},
		"all from seed": {
			// Target: 01, 02, 03
			// Seed:   01, 02, 03
			// One seed step covering all.
			target: indexSequence(0x01, 0x02, 0x03),
			seed:   indexSequence(0x01, 0x02, 0x03),
			expected: []string{
				"FileSeed(seed): Copy to [0:300]",
			},
		},
		"no match": {
			// Target: 01, 02
			// Seed:   05, 06
			// Both from store.
			target: indexSequence(0x01, 0x02),
			seed:   indexSequence(0x05, 0x06),
			expected: []string{
				"Store: Copy 0100000000000000000000000000000000000000000000000000000000000000 to [0:100]",
				"Store: Copy 0200000000000000000000000000000000000000000000000000000000000000 to [100:200]",
			},
		},
		"seed priority over self-seed": {
			// Target: 01, 02, 01
			// Seed:   01, 02, 01
			// Neither can clone, so the seed fills all positions without
			// the dependency a self-seed copy of position 0 would have.
			target: indexSequence(0x01, 0x02, 0x01),
			seed:   indexSequence(0x01, 0x02, 0x01),
			expected: []string{
				"FileSeed(seed): Copy to [0:300]",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			seed, err := NewFileSeed("test", "seed", test.seed)
			require.NoError(t, err)

			got := planStrings("test", test.target, planWithSeeds([]Seed{seed}))
			require.Equal(t, test.expected, got)
		})
	}
}

func TestInPlaceSeedPlanSteps(t *testing.T) {
	// Variable-size chunks, each filled with a distinct byte so its hash is
	// unique.
	A := filledChunk(200, 0xAA)
	B := filledChunk(150, 0xBB)
	C := filledChunk(100, 0xCC)
	D := filledChunk(50, 0xDD)
	E := filledChunk(180, 0xEE) // only appears in store
	F := filledChunk(120, 0xFF) // only appears in file seed

	// storeStep formats a store-sourced step string.
	storeStep := func(c testChunk, start uint64) string {
		id := c.id
		return fmt.Sprintf("Store: Copy %s to [%d:%d]", &id, start, start+uint64(len(c.data)))
	}

	t.Run("swap two chunks", func(t *testing.T) {
		// In-place: [A:200][B:150]
		// Target:   [B:150][A:200]
		// One cycle: A↔B, broken by buffering A. The copy of B fills the
		// buffer before it overwrites A.
		f := writeChunkFile(t, A, B)
		inPlace, err := NewFileSeed(f, f, chunkIndex(A, B))
		require.NoError(t, err)

		got := planStrings(f, chunkIndex(B, A),
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))
		expected := []string{
			"InPlace: Copy [200:350] to [0:150]",
			"InPlace: Copy [0:200] to [150:350] from buffer",
		}
		require.Equal(t, expected, got)
	})

	t.Run("buffer filled by the step overwriting it", func(t *testing.T) {
		// In a swap, A is buffered. The copy of B overwrites A's source, so
		// it fills the buffer first rather than waiting for A. The copy of A
		// still waits for B to read its source.
		f := writeChunkFile(t, A, B)
		inPlace, err := NewFileSeed(f, f, chunkIndex(A, B))
		require.NoError(t, err)

		steps := newPlan(f, chunkIndex(B, A), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false)).Steps()
		require.Len(t, steps, 2)
		copyB, copyA := steps[0], steps[1]
		require.Equal(t, "InPlace: Copy [200:350] to [0:150]", copyB.source.String())
		require.Equal(t, []*inPlaceBuffer{copyA.source.(*inPlaceCopy).buffer}, copyB.fills)
		require.Empty(t, copyA.fills)
		require.Empty(t, copyB.dependencies)
		require.Contains(t, copyA.dependencies, copyB)
	})

	t.Run("two independent cycles", func(t *testing.T) {
		// In-place: [A:200][B:150][C:100][D:50]
		// Target:   [B:150][A:200][D:50][C:100]
		// Cycle 1: A↔B in byte range [0,350)
		// Cycle 2: C↔D in byte range [350,500)
		f := writeChunkFile(t, A, B, C, D)
		inPlace, err := NewFileSeed(f, f, chunkIndex(A, B, C, D))
		require.NoError(t, err)

		got := planStrings(f, chunkIndex(B, A, D, C),
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))
		expected := []string{
			"InPlace: Copy [200:350] to [0:150]",
			"InPlace: Copy [0:200] to [150:350] from buffer",
			"InPlace: Copy [450:500] to [350:400]",
			"InPlace: Copy [350:450] to [400:500] from buffer",
		}
		require.Equal(t, expected, got)
	})

	t.Run("rearrange with store chunks", func(t *testing.T) {
		// In-place: [A:200][B:150]
		// Target:   [B:150][A:200][E:180]
		// A↔B cycle, E from store (not in seed).
		f := writeChunkFile(t, A, B)
		inPlace, err := NewFileSeed(f, f, chunkIndex(A, B))
		require.NoError(t, err)

		got := planStrings(f, chunkIndex(B, A, E),
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))
		expected := []string{
			"InPlace: Copy [200:350] to [0:150]",
			"InPlace: Copy [0:200] to [150:350] from buffer",
			storeStep(E, 350),
		}
		require.Equal(t, expected, got)
	})

	t.Run("partial rearrangement with skip", func(t *testing.T) {
		// In-place: [A:200][B:150][C:100]
		// Target:   [A:200][C:100][B:150]
		// A already at [0:200] in both indexes → skip.
		// B↔C cycle: B [200:350]→[300:450], C [350:450]→[200:300].
		f := writeChunkFile(t, A, B, C)
		inPlace, err := NewFileSeed(f, f, chunkIndex(A, B, C))
		require.NoError(t, err)

		got := planStrings(f, chunkIndex(A, C, B),
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))
		expected := []string{
			"InPlace: Copy [350:450] to [200:300]",
			"InPlace: Copy [200:350] to [300:450] from buffer",
			"InPlace: Skip [0:200]",
		}
		require.Equal(t, expected, got)
	})

	t.Run("store chunk overwrites the sources of two moves", func(t *testing.T) {
		// In-place: [C:100][D:50][A:200]
		// Target:   [E:180][D:50][C:100]
		// E is written to [0:180] which holds the sources of both moves,
		// so it has to wait for both of them. The two travel different
		// distances, so they stay separate moves.
		f := writeChunkFile(t, C, D, A)
		inPlace, err := NewFileSeed(f, f, chunkIndex(C, D, A))
		require.NoError(t, err)

		steps := newPlan(f, chunkIndex(E, D, C), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false)).Steps()
		require.Equal(t, map[string][]string{
			"InPlace: Copy [100:150] to [180:230]": nil,
			"InPlace: Copy [0:100] to [230:330]":   nil,
			storeStep(E, 0): {
				"InPlace: Copy [0:100] to [230:330]",
				"InPlace: Copy [100:150] to [180:230]",
			},
		}, stepDependencies(steps))
	})

	t.Run("chunks of one shift move together", func(t *testing.T) {
		// In-place: [C:100][D:50][A:200]
		// Target:   [E:180][C:100][D:50]
		// C and D stay next to each other and travel the same distance, so
		// a single move covers both of them.
		f := writeChunkFile(t, C, D, A)
		inPlace, err := NewFileSeed(f, f, chunkIndex(C, D, A))
		require.NoError(t, err)

		steps := newPlan(f, chunkIndex(E, C, D), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false)).Steps()
		require.Equal(t, []string{
			"InPlace: Copy [0:150] to [180:330]",
			storeStep(E, 0),
		}, stepStrings(steps))
		require.Equal(t, 2, steps[0].numChunks, "chunks covered by the move")
	})

	t.Run("a run that would form a cycle is split again", func(t *testing.T) {
		// In-place: [X:100][A:100][C:100][Z:100][B:100]
		// Target:   [P:100][B:100][Q:100][A:100][C:100]
		// A and C travel the same distance and are adjacent, but merging
		// them creates a cycle: B is written where A reads, and reads where
		// C writes. The run is split again rather than held in memory.
		x := filledChunk(100, 0x01)
		a := filledChunk(100, 0x02)
		c := filledChunk(100, 0x03)
		z := filledChunk(100, 0x04)
		b := filledChunk(100, 0x05)
		q := filledChunk(100, 0x06) // only in the store
		r := filledChunk(100, 0x07) // only in the store

		f := writeChunkFile(t, x, a, c, z, b)
		inPlace, err := NewFileSeed(f, f, chunkIndex(x, a, c, z, b))
		require.NoError(t, err)

		steps := newPlan(f, chunkIndex(r, b, q, a, c), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false)).Steps()
		moveA := "InPlace: Copy [100:200] to [300:400]"
		moveB := "InPlace: Copy [400:500] to [100:200]"
		moveC := "InPlace: Copy [200:300] to [400:500]"
		require.Equal(t, map[string][]string{
			moveA:             nil,
			moveB:             {moveA},
			moveC:             {moveB},
			storeStep(r, 0):   nil,
			storeStep(q, 200): {moveC},
		}, stepDependencies(steps))
	})

	t.Run("moves outside of cycles are ordered", func(t *testing.T) {
		// In-place: [A:200][B:150][C:100]
		// Target:   [D:50][A:200][B:150][C:100]
		// Every chunk shifts by 50 bytes. There is no cycle but each move
		// overwrites the source of the next one, so that has to be read
		// first. The chunks all travel the same distance, but it's shorter
		// than any of them, so they can't be moved as one run.
		f := writeChunkFile(t, A, B, C)
		inPlace, err := NewFileSeed(f, f, chunkIndex(A, B, C))
		require.NoError(t, err)

		steps := newPlan(f, chunkIndex(D, A, B, C), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false)).Steps()
		require.Equal(t, map[string][]string{
			"InPlace: Copy [0:200] to [50:250]":    {"InPlace: Copy [200:350] to [250:400]"},
			"InPlace: Copy [200:350] to [250:400]": {"InPlace: Copy [350:450] to [400:500]"},
			"InPlace: Copy [350:450] to [400:500]": nil,
			storeStep(D, 0):                        {"InPlace: Copy [0:200] to [50:250]"},
		}, stepDependencies(steps))
	})

	t.Run("runs outside of a cycle stay merged", func(t *testing.T) {
		// In-place: [F0][F1][B][F3][A][C][D][E]
		// Target:   [T0][A][C][T3][B][T5][T6][T7][D][E]
		// A and C move together, but merged they form a cycle with B: B is
		// written where A reads, and reads where C writes. Only that run is
		// split. D and E are a run elsewhere, not part of any cycle, and stay
		// one move.
		c := func(fill byte) testChunk { return filledChunk(100, fill) }
		f0, f1, b, f3, a, cc, d, e := c(0x10), c(0x11), c(0x12), c(0x13), c(0x14), c(0x15), c(0x16), c(0x17)
		t0, t3, t5, t6, t7 := c(0x20), c(0x23), c(0x25), c(0x26), c(0x27) // only in the store

		f := writeChunkFile(t, f0, f1, b, f3, a, cc, d, e)
		inPlace, err := NewFileSeed(f, f, chunkIndex(f0, f1, b, f3, a, cc, d, e))
		require.NoError(t, err)

		var moves []string
		for _, step := range newPlan(f, chunkIndex(t0, a, cc, t3, b, t5, t6, t7, d, e), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false)).Steps() {
			if _, ok := step.source.(*inPlaceCopy); ok {
				moves = append(moves, step.source.String())
			}
		}
		require.Equal(t, []string{
			"InPlace: Copy [400:500] to [100:200]",
			"InPlace: Copy [500:600] to [200:300]",
			"InPlace: Copy [200:300] to [400:500]",
			"InPlace: Copy [600:800] to [800:1000]",
		}, moves)
	})

	t.Run("zero chunks are left to the null seed", func(t *testing.T) {
		// In-place: [zeros:256][Z:256]
		// Target:   [Z:256][zeros:256]
		// Moving the zeros would read them from the target. The null seed
		// writes them instead, once the move of Z read its source.
		zeros := testChunk{id: Digest.Sum(make([]byte, 256)), data: make([]byte, 256)}
		z := filledChunk(256, 0x5A)
		f := writeChunkFile(t, zeros, z)
		inPlace, err := NewFileSeed(f, f, chunkIndex(zeros, z))
		require.NoError(t, err)

		steps := newPlan(f, chunkIndex(z, zeros), nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false),
			planWithSeeds([]Seed{&nullChunkSeed{id: zeros.id}})).Steps()
		require.Equal(t, map[string][]string{
			"InPlace: Copy [256:512] to [0:256]": nil,
			"FileSeed(): Copy to [256:512]":      {"InPlace: Copy [256:512] to [0:256]"},
		}, stepDependencies(steps))
	})

	t.Run("mixed in-place and file seed", func(t *testing.T) {
		// In-place: [A:200][B:150]
		// File seed "seedfile": [F:120]
		// Target:   [A:200][F:120][B:150]
		// A at same offset → skip.
		// B moves [200:350]→[320:470] (B must read before F writes to [200:320]).
		// F from file seed at [200:320].
		f := writeChunkFile(t, A, B)
		inPlaceSeed, err := NewFileSeed(f, f, chunkIndex(A, B))
		require.NoError(t, err)
		fileSeed, err := NewFileSeed(f, "seedfile", chunkIndex(F))
		require.NoError(t, err)

		got := planStrings(f, chunkIndex(A, F, B),
			planWithInPlaceSeed(inPlaceSeed), planWithSeeds([]Seed{fileSeed}), planWithTargetIsBlank(false))
		expected := []string{
			"InPlace: Copy [200:350] to [320:470]",
			"InPlace: Skip [0:200]",
			"FileSeed(seedfile): Copy to [200:320]",
		}
		require.Equal(t, expected, got)
	})
}

// planStrings builds a plan and returns the string representation of the
// source of every step, in the order the steps are handed out.
func planStrings(name string, idx Index, opts ...planOption) []string {
	return stepStrings(newPlan(name, idx, nil, opts...).Steps())
}

// stepStrings returns the string representation of the steps' sources.
func stepStrings(steps []*planStep) []string {
	got := make([]string, len(steps))
	for i, s := range steps {
		got[i] = s.source.String()
	}
	return got
}

// stepDependencies returns the sorted dependencies of every step, keyed and
// identified by their string representation.
func stepDependencies(steps []*planStep) map[string][]string {
	deps := make(map[string][]string, len(steps))
	for _, step := range steps {
		var d []string
		for dep := range step.dependencies {
			d = append(d, dep.source.String())
		}
		slices.Sort(d)
		deps[step.source.String()] = d
	}
	return deps
}

func TestBreakCycles(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 1000 {
		// Build a random graph with up to 20 nodes, including cycles
		// and nodes with several outgoing edges.
		n := rng.IntN(20) + 1
		succ := make([][]int, n)
		for i := range succ {
			for range rng.IntN(4) {
				if j := rng.IntN(n); j != i {
					succ[i] = append(succ[i], j)
				}
			}
		}

		// Removing the outgoing edges of marked nodes must leave the graph
		// without cycles, i.e. all nodes can be sorted topologically.
		marked := breakCycles(succ)
		inDegree := make([]int, n)
		for i := range succ {
			if marked[i] {
				continue
			}
			for _, j := range succ[i] {
				inDegree[j]++
			}
		}
		var queue []int
		for i, d := range inDegree {
			if d == 0 {
				queue = append(queue, i)
			}
		}
		var sorted int
		for len(queue) > 0 {
			i := queue[0]
			queue = queue[1:]
			sorted++
			if marked[i] {
				continue
			}
			for _, j := range succ[i] {
				inDegree[j]--
				if inDegree[j] == 0 {
					queue = append(queue, j)
				}
			}
		}
		require.Equal(t, n, sorted, "cycle left in graph %v with marked nodes %v", succ, marked)
	}
}

// movesInCycles finds exactly the nodes on a cycle: those that reach another
// node which reaches them back.
func TestMovesInCycles(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for range 1000 {
		n := rng.IntN(20) + 1
		succ := make([][]int, n)
		for i := range succ {
			for range rng.IntN(3) {
				if j := rng.IntN(n); j != i {
					succ[i] = append(succ[i], j)
				}
			}
		}

		// reach[i][j] is set when there's a path from i to j
		reach := make([][]bool, n)
		for i := range reach {
			reach[i] = make([]bool, n)
			queue := []int{i}
			for len(queue) > 0 {
				v := queue[0]
				queue = queue[1:]
				for _, w := range succ[v] {
					if !reach[i][w] {
						reach[i][w] = true
						queue = append(queue, w)
					}
				}
			}
		}
		want := make([]bool, n)
		for i := range n {
			for j := range n {
				if i != j && reach[i][j] && reach[j][i] {
					want[i] = true
				}
			}
		}
		require.Equal(t, want, movesInCycles(succ), "graph %v", succ)
	}
}

func TestFileSeedValidation(t *testing.T) {
	// Create two chunks with known data and compute their IDs
	data1 := make([]byte, 100)
	data1[0] = 0xAA
	id1 := Digest.Sum(data1)

	data2 := make([]byte, 100)
	data2[0] = 0xBB
	id2 := Digest.Sum(data2)

	seedIndex := Index{
		Chunks: []IndexChunk{
			{ID: id1, Start: 0, Size: 100},
			{ID: id2, Start: 100, Size: 100},
		},
	}

	// Target index matches the seed exactly
	targetIndex := Index{
		Chunks: []IndexChunk{
			{ID: id1, Start: 0, Size: 100},
			{ID: id2, Start: 100, Size: 100},
		},
	}

	t.Run("valid seed", func(t *testing.T) {
		seedFile := filepath.Join(t.TempDir(), "seed")
		f, err := os.Create(seedFile)
		require.NoError(t, err)
		_, err = f.Write(data1)
		require.NoError(t, err)
		_, err = f.Write(data2)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		seed, err := NewFileSeed("target", seedFile, seedIndex)
		require.NoError(t, err)

		plan := newPlan("target", targetIndex, nil, planWithSeeds([]Seed{seed}))

		require.NoError(t, plan.Validate())
	})

	t.Run("invalid seed", func(t *testing.T) {
		seedFile := filepath.Join(t.TempDir(), "seed")
		f, err := os.Create(seedFile)
		require.NoError(t, err)
		_, err = f.Write(data1)
		require.NoError(t, err)
		_, err = f.Write(data2)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		seed, err := NewFileSeed("target", seedFile, seedIndex)
		require.NoError(t, err)

		plan := newPlan("target", targetIndex, nil, planWithSeeds([]Seed{seed}))

		// Corrupt the seed file after the plan was created
		err = os.WriteFile(seedFile, make([]byte, 200), 0644)
		require.NoError(t, err)

		err = plan.Validate()
		require.Error(t, err)

		var seedErr SeedInvalid
		require.ErrorAs(t, err, &seedErr)
		require.Equal(t, []Seed{seed}, seedErr.Seeds)
	})

	t.Run("stale in-place seed chunk at its position", func(t *testing.T) {
		// The in-place seed claims [1][2] but the file only holds chunk 1.
		// Chunk 1 is skipped after hashing it. The seed is out of date for
		// chunk 2, which has to come from the store without invalidating
		// the seed.
		f := filepath.Join(t.TempDir(), "target")
		err := os.WriteFile(f, append(data1, make([]byte, 100)...), 0644)
		require.NoError(t, err)

		inPlace, err := NewFileSeed(f, f, seedIndex)
		require.NoError(t, err)

		plan := newPlan(f, targetIndex, nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))
		require.NoError(t, plan.Validate())

		require.Equal(t, []string{
			"InPlace: Skip [0:100]",
			fmt.Sprintf("Store: Copy %s to [100:200]", ChunkID(id2)),
		}, stepStrings(plan.Steps()))
	})

	t.Run("invalid in-place seed move", func(t *testing.T) {
		// The in-place seed claims [1][2] but the file only holds chunk 1
		// at the correct position. Chunk 2 is moved to the front of the
		// target, so Validate must check the move's source as well.
		f := filepath.Join(t.TempDir(), "target")
		err := os.WriteFile(f, append(data1, make([]byte, 100)...), 0644)
		require.NoError(t, err)

		inPlace, err := NewFileSeed(f, f, seedIndex)
		require.NoError(t, err)

		swapped := Index{
			Chunks: []IndexChunk{
				{ID: id2, Start: 0, Size: 100},
				{ID: id1, Start: 100, Size: 100},
			},
		}
		plan := newPlan(f, swapped, nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))

		err = plan.Validate()
		var seedErr SeedInvalid
		require.ErrorAs(t, err, &seedErr)
		require.Equal(t, []Seed{inPlace}, seedErr.Seeds)
	})

	t.Run("stale chunk inside a moved run", func(t *testing.T) {
		// The in-place seed claims [1][2] at the front of the file, and both
		// move to the back by the same distance, so one move covers them.
		// Only the first of the two is where the seed says it is, which the
		// move has to notice for all of its chunks.
		f := filepath.Join(t.TempDir(), "target")
		err := os.WriteFile(f, append(data1, make([]byte, 100)...), 0644)
		require.NoError(t, err)

		inPlace, err := NewFileSeed(f, f, seedIndex)
		require.NoError(t, err)

		shifted := Index{
			Chunks: []IndexChunk{
				{ID: ChunkID{0xCC}, Start: 0, Size: 200}, // from the store
				{ID: id1, Start: 200, Size: 100},
				{ID: id2, Start: 300, Size: 100},
			},
		}
		plan := newPlan(f, shifted, nil,
			planWithInPlaceSeed(inPlace), planWithTargetIsBlank(false))

		// One move covers the whole run, and it's invalid
		require.Equal(t, []string{
			"InPlace: Copy [0:200] to [200:400]",
			fmt.Sprintf("Store: Copy %s to [0:200]", ChunkID{0xCC}),
		}, stepStrings(plan.Steps()))

		err = plan.Validate()
		var seedErr SeedInvalid
		require.ErrorAs(t, err, &seedErr)
		require.Equal(t, []Seed{inPlace}, seedErr.Seeds)
	})

	t.Run("null seed skipped", func(t *testing.T) {
		// Create a null chunk index — data is all zeros
		nullData := make([]byte, 100)
		nullID := Digest.Sum(nullData)

		nullTargetIndex := Index{
			Chunks: []IndexChunk{
				{ID: nullID, Start: 0, Size: 100},
			},
		}

		// Use a null seed (FileName() returns "", so Validate skips it)
		ns := &nullChunkSeed{id: nullID}
		defer ns.close()

		plan := newPlan("target", nullTargetIndex, nil, planWithSeeds([]Seed{ns}))

		require.NoError(t, plan.Validate())
	})
}

// Planning again for other seeds reuses what's known about the target
// rather than reading it again.
func TestReplanReusesTarget(t *testing.T) {
	data := make([]byte, 100)
	data[0] = 0xAA
	idx := Index{Chunks: []IndexChunk{{ID: Digest.Sum(data), Start: 0, Size: 100}}}

	f := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(f, data, 0644))
	plan := newPlan(f, idx, nil, planWithTargetIsBlank(false))

	// Changing the target isn't noticed by the new plan, it's not hashed
	// again
	require.NoError(t, os.WriteFile(f, make([]byte, 100), 0644))
	replanned := plan.replan(planWithSeeds(nil))

	require.Same(t, plan.selfSeed, replanned.selfSeed)
	require.Equal(t, []string{"InPlace: Skip [0:100]"}, stepStrings(replanned.Steps()))
}

func TestOverlapping(t *testing.T) {
	// Chunks at [0:100] [100:200] [200:300]
	chunks := indexSequence(1, 2, 3).Chunks
	for _, test := range []struct {
		start, end uint64
		lo, hi     int
	}{
		{0, 100, 0, 1},   // exactly the first chunk
		{50, 150, 0, 2},  // across a boundary
		{100, 200, 1, 2}, // touching the neighbours only
		{150, 160, 1, 2}, // within a chunk
		{0, 300, 0, 3},   // all chunks
		{250, 400, 2, 3}, // past the end
		{300, 400, 3, 3}, // beyond all chunks
	} {
		lo, hi := overlapping(chunks, test.start, test.end)
		require.Equal(t, []int{test.lo, test.hi}, []int{lo, hi}, "range [%d:%d]", test.start, test.end)
	}
}

// Chunks that can be cloned are taken from wherever they can be, seeds first,
// before any chunks are copied.
func TestCloningPriority(t *testing.T) {
	// Target: A B A with 256 byte chunks at [0:256] [256:512] [512:768].
	// With a blocksize of 64, the self-seed can clone A from 512 to 0.
	a, b := ChunkID{0x0A}, ChunkID{0x0B}
	target := Index{Chunks: []IndexChunk{
		{ID: a, Start: 0, Size: 256},
		{ID: b, Start: 256, Size: 256},
		{ID: a, Start: 512, Size: 256},
	}}
	storeStep := func(id ChunkID, start uint64) string {
		return fmt.Sprintf("Store: Copy %s to [%d:%d]", id, start, start+256)
	}

	planSteps := func(t *testing.T, seedIndex Index) []string {
		t.Helper()
		name := filepath.Join(t.TempDir(), "target")
		seed, err := NewFileSeed(name, "seed", seedIndex)
		require.NoError(t, err)
		seed.canReflink = true

		plan := newPlan(name, target, nil, planWithSeeds([]Seed{seed}), planWithBlocksize(64))
		plan.selfSeed.canReflink = true
		return stepStrings(plan.replan().Steps())
	}

	t.Run("self-seed clone over seed copy", func(t *testing.T) {
		// A is at an offset in the seed that can't be cloned to either
		// position, so position 0 is cloned from position 2 instead, and
		// only position 2 is copied from the seed.
		got := planSteps(t, Index{Chunks: []IndexChunk{
			{ID: ChunkID{0xFF}, Start: 0, Size: 32},
			{ID: a, Start: 32, Size: 256},
		}})
		require.Equal(t, []string{
			"SelfSeed: Copy [512:768] to [0:256]",
			storeStep(b, 256),
			"FileSeed(seed): Copy to [512:768]",
		}, got)
	})

	t.Run("seed clone over self-seed clone", func(t *testing.T) {
		// A is aligned in the seed, both positions are cloned from there.
		got := planSteps(t, Index{Chunks: []IndexChunk{
			{ID: a, Start: 0, Size: 256},
		}})
		require.Equal(t, []string{
			"FileSeed(seed): Copy to [0:256]",
			storeStep(b, 256),
			"FileSeed(seed): Copy to [512:768]",
		}, got)
	})
}

// countingSeed counts how often the seed is searched for a match.
type countingSeed struct {
	*FileSeed
	lookups int
}

func (s *countingSeed) LongestMatchFrom(chunks []IndexChunk, startPos int) (int, int) {
	s.lookups++
	return s.FileSeed.LongestMatchFrom(chunks, startPos)
}

// A seed that can't clone isn't searched for matches to clone. Otherwise
// every position is looked up once more, only to find the match has to be
// copied.
func TestCloningPassSkipsSeedsThatCantClone(t *testing.T) {
	idx := indexSequence(1, 2, 3)
	seed, err := NewFileSeed("target", "seed", idx)
	require.NoError(t, err)
	seed.canReflink = false
	counting := &countingSeed{FileSeed: seed}

	plan := newPlan("target", idx, nil, planWithSeeds([]Seed{counting}), planWithBlocksize(64))
	require.Equal(t, []string{"FileSeed(seed): Copy to [0:300]"}, stepStrings(plan.Steps()))
	require.Equal(t, 1, counting.lookups, "one lookup finds the whole run to copy")
}

func TestReflinkable(t *testing.T) {
	for _, test := range []struct {
		src, length, dst uint64
		want             bool
	}{
		{0, 256, 0, true},     // aligned, spans full blocks
		{64, 256, 128, true},  // aligned relative to each other
		{32, 256, 0, false},   // different alignment
		{0, 64, 0, false},     // no full block after the first boundary
		{16, 100, 16, false},  // too small for a full block
		{16, 200, 1040, true}, // same alignment, full block inside
	} {
		require.Equal(t, test.want, reflinkable(test.src, test.length, test.dst, 64),
			"src %d, length %d, dst %d", test.src, test.length, test.dst)
	}
}
