package desync

import (
	"fmt"
	"os"
	"path/filepath"
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
			plan, err := NewPlan("test", test.index, nil)
			require.NoError(t, err)
			defer plan.Close()

			steps := plan.Steps()
			got := make([]string, len(steps))
			for i, s := range steps {
				got[i] = s.source.String()
			}
			require.Equal(t, test.expected, got)
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
	f.Close()

	plan, err := NewPlan(target, idx, nil, PlanWithTargetIsBlank(false))
	require.NoError(t, err)
	defer plan.Close()

	steps := plan.Steps()
	got := make([]string, len(steps))
	for i, s := range steps {
		got[i] = s.source.String()
	}

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
		f2.Close()

		plan2, err := NewPlan(target2, idx, nil, PlanWithTargetIsBlank(false))
		require.NoError(t, err)
		defer plan2.Close()

		steps2 := plan2.Steps()
		got2 := make([]string, len(steps2))
		for i, s := range steps2 {
			got2[i] = s.source.String()
		}

		expected2 := []string{
			"InPlace: Skip [0:300]",
		}
		require.Equal(t, expected2, got2)
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
		"self-seed priority": {
			// Target: 01, 02, 01
			// Seed:   01, 02, 01
			// Self-seed fills position 0 (copy from position 2),
			// seed fills positions 1-2 (matching seed chunks 02, 01).
			target: indexSequence(0x01, 0x02, 0x01),
			seed:   indexSequence(0x01, 0x02, 0x01),
			expected: []string{
				"SelfSeed: Copy [200:300] to [0:100]",
				"FileSeed(seed): Copy to [100:300]",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			seed, err := NewFileSeed("test", "seed", test.seed)
			require.NoError(t, err)

			plan, err := NewPlan("test", test.target, nil, PlanWithSeeds([]Seed{seed}))
			require.NoError(t, err)
			defer plan.Close()

			steps := plan.Steps()
			got := make([]string, len(steps))
			for i, s := range steps {
				got[i] = s.source.String()
			}
			require.Equal(t, test.expected, got)
		})
	}
}

func TestInPlaceSeedPlanSteps(t *testing.T) {
	// Create variable-size chunks with known data and compute real ChunkIDs.
	// Each chunk is filled with a distinct byte so the SHA512/256 hash is unique.
	type chunk struct {
		id   ChunkID
		size uint64
	}
	newChunk := func(size int, fill byte) chunk {
		data := make([]byte, size)
		for i := range data {
			data[i] = fill
		}
		return chunk{id: ChunkID(Digest.Sum(data)), size: uint64(size)}
	}

	A := newChunk(200, 0xAA)
	B := newChunk(150, 0xBB)
	C := newChunk(100, 0xCC)
	D := newChunk(50, 0xDD)
	E := newChunk(180, 0xEE) // only appears in store
	F := newChunk(120, 0xFF) // only appears in file seed

	// buildIndex creates an Index with contiguous chunk offsets.
	buildIndex := func(chunks ...chunk) Index {
		ic := make([]IndexChunk, len(chunks))
		var start uint64
		for i, c := range chunks {
			ic[i] = IndexChunk{ID: c.id, Start: start, Size: c.size}
			start += c.size
		}
		return Index{Chunks: ic}
	}

	// planSteps is a helper that creates a plan and returns its step strings.
	planSteps := func(t *testing.T, target Index, opts ...PlanOption) []string {
		t.Helper()
		plan, err := NewPlan("test", target, nil, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { plan.Close() })
		steps := plan.Steps()
		got := make([]string, len(steps))
		for i, s := range steps {
			got[i] = s.source.String()
		}
		return got
	}

	// storeStep formats a store-sourced step string.
	storeStep := func(c chunk, start uint64) string {
		id := c.id
		return fmt.Sprintf("Store: Copy %s to [%d:%d]", &id, start, start+c.size)
	}

	t.Run("swap two chunks", func(t *testing.T) {
		// In-place: [A:200][B:150]
		// Target:   [B:150][A:200]
		// One cycle: A↔B.
		inPlace, err := NewInPlaceSeed("test", buildIndex(A, B))
		require.NoError(t, err)

		got := planSteps(t, buildIndex(B, A), PlanWithSeeds([]Seed{inPlace}))
		expected := []string{
			"InPlace: Copy [0:200] to [150:350]",
			"InPlace: Copy [200:350] to [0:150]",
		}
		require.Equal(t, expected, got)
	})

	t.Run("two independent cycles", func(t *testing.T) {
		// In-place: [A:200][B:150][C:100][D:50]
		// Target:   [B:150][A:200][D:50][C:100]
		// Cycle 1: A↔B in byte range [0,350)
		// Cycle 2: C↔D in byte range [350,500)
		inPlace, err := NewInPlaceSeed("test", buildIndex(A, B, C, D))
		require.NoError(t, err)

		got := planSteps(t, buildIndex(B, A, D, C), PlanWithSeeds([]Seed{inPlace}))
		expected := []string{
			"InPlace: Copy [0:200] to [150:350]",
			"InPlace: Copy [200:350] to [0:150]",
			"InPlace: Copy [350:450] to [400:500]",
			"InPlace: Copy [450:500] to [350:400]",
		}
		require.Equal(t, expected, got)
	})

	t.Run("rearrange with store chunks", func(t *testing.T) {
		// In-place: [A:200][B:150]
		// Target:   [B:150][A:200][E:180]
		// A↔B cycle, E from store (not in seed).
		inPlace, err := NewInPlaceSeed("test", buildIndex(A, B))
		require.NoError(t, err)

		got := planSteps(t, buildIndex(B, A, E), PlanWithSeeds([]Seed{inPlace}))
		expected := []string{
			"InPlace: Copy [0:200] to [150:350]",
			"InPlace: Copy [200:350] to [0:150]",
			storeStep(E, 350),
		}
		require.Equal(t, expected, got)
	})

	t.Run("partial rearrangement with skip", func(t *testing.T) {
		// In-place: [A:200][B:150][C:100]
		// Target:   [A:200][C:100][B:150]
		// A already at [0:200] in both indexes → skip.
		// B↔C cycle: B [200:350]→[300:450], C [350:450]→[200:300].
		inPlace, err := NewInPlaceSeed("test", buildIndex(A, B, C))
		require.NoError(t, err)

		got := planSteps(t, buildIndex(A, C, B), PlanWithSeeds([]Seed{inPlace}))
		expected := []string{
			"InPlace: Skip [0:200]",
			"InPlace: Copy [200:350] to [300:450]",
			"InPlace: Copy [350:450] to [200:300]",
		}
		require.Equal(t, expected, got)
	})

	t.Run("mixed in-place and file seed", func(t *testing.T) {
		// In-place: [A:200][B:150]
		// File seed "seedfile": [F:120]
		// Target:   [A:200][F:120][B:150]
		// A at same offset → skip.
		// B moves [200:350]→[320:470] (B must read before F writes to [200:320]).
		// F from file seed at [200:320].
		inPlaceSeed, err := NewInPlaceSeed("test", buildIndex(A, B))
		require.NoError(t, err)
		fileSeed, err := NewFileSeed("test", "seedfile", buildIndex(F))
		require.NoError(t, err)

		got := planSteps(t, buildIndex(A, F, B),
			PlanWithSeeds([]Seed{inPlaceSeed, fileSeed}))
		expected := []string{
			"InPlace: Skip [0:200]",
			"InPlace: Copy [200:350] to [320:470]",
			"FileSeed(seedfile): Copy to [200:320]",
		}
		require.Equal(t, expected, got)
	})
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
		f.Close()

		seed, err := NewFileSeed("target", seedFile, seedIndex)
		require.NoError(t, err)

		plan, err := NewPlan("target", targetIndex, nil, PlanWithSeeds([]Seed{seed}))
		require.NoError(t, err)
		defer plan.Close()

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
		f.Close()

		seed, err := NewFileSeed("target", seedFile, seedIndex)
		require.NoError(t, err)

		plan, err := NewPlan("target", targetIndex, nil, PlanWithSeeds([]Seed{seed}))
		require.NoError(t, err)
		defer plan.Close()

		// Corrupt the seed file after the plan was created
		err = os.WriteFile(seedFile, make([]byte, 200), 0644)
		require.NoError(t, err)

		err = plan.Validate()
		require.Error(t, err)

		var seedErr SeedInvalid
		require.ErrorAs(t, err, &seedErr)
		require.Equal(t, []Seed{seed}, seedErr.Seeds)
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

		plan, err := NewPlan("target", nullTargetIndex, nil, PlanWithSeeds([]Seed{ns}))
		require.NoError(t, err)
		defer plan.Close()

		require.NoError(t, plan.Validate())
	})
}
