package desync

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Moves are cloned when the file supports it, they cover a run of several
// chunks, and their source and destination have the same alignment and don't
// overlap.
func TestInPlaceMoveClones(t *testing.T) {
	a := filledChunk(256, 0x0A)
	b := filledChunk(256, 0x0B)
	pad := func(size int) testChunk { return filledChunk(size, 0xEE) }

	// moveClones plans the target over a file holding the in-place chunks and
	// reports whether each move clones, by its string.
	moveClones := func(t *testing.T, canReflink bool, inPlace []testChunk, target ...testChunk) map[string]bool {
		t.Helper()
		f := writeChunkFile(t, inPlace...)
		seed, err := NewFileSeed(f, f, chunkIndex(inPlace...))
		require.NoError(t, err)
		seed.canReflink = canReflink
		plan := newPlan(f, chunkIndex(target...), nil,
			planWithInPlaceSeed(seed), planWithTargetIsBlank(false), planWithBlocksize(64))
		clones := make(map[string]bool)
		for _, step := range plan.Steps() {
			if c, ok := step.source.(*inPlaceCopy); ok {
				clones[c.String()] = c.clone
			}
		}
		return clones
	}

	t.Run("aligned run", func(t *testing.T) {
		got := moveClones(t, true, []testChunk{a, b}, pad(512), a, b)
		require.Equal(t, map[string]bool{"InPlace: Copy [0:512] to [512:1024]": true}, got)
	})
	t.Run("without reflink support", func(t *testing.T) {
		got := moveClones(t, false, []testChunk{a, b}, pad(512), a, b)
		require.Equal(t, map[string]bool{"InPlace: Copy [0:512] to [512:1024]": false}, got)
	})
	t.Run("overlapping", func(t *testing.T) {
		// Shifted by 64 bytes, less than a chunk, so the chunks move one by
		// one and each overlaps its own source
		got := moveClones(t, true, []testChunk{a, b}, pad(64), a, b)
		require.Equal(t, map[string]bool{
			"InPlace: Copy [0:256] to [64:320]":    false,
			"InPlace: Copy [256:512] to [320:576]": false,
		}, got)
	})
	t.Run("different alignment", func(t *testing.T) {
		got := moveClones(t, true, []testChunk{a, b}, pad(600), a, b)
		require.Equal(t, map[string]bool{"InPlace: Copy [0:512] to [600:1112]": false}, got)
	})
	t.Run("single chunk", func(t *testing.T) {
		// Aligned and apart, but a swap moves the chunks one by one
		got := moveClones(t, true, []testChunk{a, b}, b, a)
		require.Equal(t, map[string]bool{
			"InPlace: Copy [256:512] to [0:256]":             false,
			"InPlace: Copy [0:256] to [256:512] from buffer": false,
		}, got)
	})
}

// A cloned move clones its whole blocks and copies the rest, and copies all of
// it if the filesystem refuses the clone.
func TestInPlaceMoveCloneBlocks(t *testing.T) {
	// 1000 bytes at 100 move to 2148, both 36 bytes past a 64 byte block
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 251)
	}
	want := append([]byte(nil), data...)
	copy(want[2148:], data[100:1100])

	move := func(t *testing.T) (*inPlaceCopy, *os.File) {
		t.Helper()
		name := filepath.Join(t.TempDir(), "target")
		require.NoError(t, os.WriteFile(name, data, 0644))
		f, err := os.OpenFile(name, os.O_RDWR, 0)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		return &inPlaceCopy{
			chunks:    []IndexChunk{{Start: 100, Size: 1000}},
			dstOffset: 2148,
			clone:     true,
			blocksize: 64,
		}, f
	}
	contents := func(t *testing.T, f *os.File) []byte {
		t.Helper()
		got := make([]byte, len(data))
		_, err := f.ReadAt(got, 0)
		require.NoError(t, err)
		return got
	}
	stubClone := func(t *testing.T, clone func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error) {
		orig := cloneRange
		cloneRange = clone
		t.Cleanup(func() { cloneRange = orig })
	}

	t.Run("clone", func(t *testing.T) {
		var calls [][3]uint64
		stubClone(t, func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
			calls = append(calls, [3]uint64{srcOffset, srcLength, dstOffset})
			// Stand in for the clone by copying the range
			b := make([]byte, srcLength)
			if _, err := src.ReadAt(b, int64(srcOffset)); err != nil {
				return err
			}
			_, err := dst.WriteAt(b, int64(dstOffset))
			return err
		})
		s, f := move(t)
		copied, cloned, err := s.Execute(f)
		require.NoError(t, err)
		assert.Equal(t, [][3]uint64{{128, 960, 2176}}, calls, "cloned range")
		assert.Equal(t, uint64(960), cloned)
		assert.Equal(t, uint64(40), copied, "unaligned head and tail")
		require.Equal(t, want, contents(t, f))
	})

	t.Run("clone refused", func(t *testing.T) {
		stubClone(t, func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
			return errors.New("simulated clone failure")
		})
		s, f := move(t)
		copied, cloned, err := s.Execute(f)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), cloned)
		assert.Equal(t, uint64(1000), copied)
		require.Equal(t, want, contents(t, f))
	})
}
