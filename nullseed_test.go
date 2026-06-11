package desync

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simulates filesystems like ZFS where CanClone succeeds but the actual
// cloning of blocks fails, e.g. because the zero blockfile hasn't been
// committed to disk yet. The null seed is expected to fall back to writing
// zeros, or to leave a blank target untouched.
func TestNullChunkSectionCloneFallback(t *testing.T) {
	defer func() { cloneRange = CloneRange }()

	const blocksize = 4096
	length := uint64(3*blocksize + 100)
	dir := t.TempDir()

	blockfile, err := os.CreateTemp(dir, ".tmp-block")
	require.NoError(t, err)
	defer blockfile.Close()
	_, err = blockfile.Write(make([]byte, blocksize))
	require.NoError(t, err)

	newSection := func() *nullChunkSection {
		return &nullChunkSection{from: 0, to: length, blockfile: blockfile, canReflink: true}
	}

	t.Run("copies zeros when cloning fails", func(t *testing.T) {
		cloneRange = func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
			return errors.New("simulated clone failure")
		}
		dstName := filepath.Join(dir, "out1")
		require.NoError(t, os.WriteFile(dstName, bytes.Repeat([]byte{0xff}, int(length)), 0644))
		dst, err := os.OpenFile(dstName, os.O_RDWR, 0)
		require.NoError(t, err)
		defer dst.Close()

		copied, cloned, err := newSection().WriteInto(dst, 0, length, blocksize, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), cloned)
		assert.Equal(t, length, copied)

		got, err := os.ReadFile(dstName)
		require.NoError(t, err)
		assert.Equal(t, make([]byte, length), got)
	})

	t.Run("leaves blank target untouched when cloning fails", func(t *testing.T) {
		cloneRange = func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
			return errors.New("simulated clone failure")
		}
		dstName := filepath.Join(dir, "out2")
		dst, err := os.Create(dstName)
		require.NoError(t, err)
		defer dst.Close()
		require.NoError(t, dst.Truncate(int64(length)))

		_, cloned, err := newSection().WriteInto(dst, 0, length, blocksize, true)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), cloned)

		got, err := os.ReadFile(dstName)
		require.NoError(t, err)
		assert.Equal(t, make([]byte, length), got)
	})

	t.Run("section smaller than a block writes only within its range", func(t *testing.T) {
		var cloneCalls int
		cloneRange = func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
			cloneCalls++
			return nil
		}
		const bigBlock = 131072
		from, sectionLen := uint64(1000), uint64(5000)
		fileLen := uint64(3 * bigBlock)
		dstName := filepath.Join(dir, "out-small")
		require.NoError(t, os.WriteFile(dstName, bytes.Repeat([]byte{0xff}, int(fileLen)), 0644))
		dst, err := os.OpenFile(dstName, os.O_RDWR, 0)
		require.NoError(t, err)
		defer dst.Close()

		section := &nullChunkSection{from: from, to: from + sectionLen, blockfile: blockfile, canReflink: true}
		copied, cloned, err := section.WriteInto(dst, from, sectionLen, bigBlock, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), cloned)
		assert.Equal(t, sectionLen, copied)
		assert.Equal(t, 0, cloneCalls)

		got, err := os.ReadFile(dstName)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{0xff}, int(from)), got[:from])
		assert.Equal(t, make([]byte, sectionLen), got[from:from+sectionLen])
		assert.Equal(t, bytes.Repeat([]byte{0xff}, int(fileLen-from-sectionLen)), got[from+sectionLen:])
	})

	t.Run("copies remainder when cloning fails mid-section", func(t *testing.T) {
		var calls int
		cloneRange = func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
			calls++
			if calls > 1 {
				return errors.New("simulated clone failure")
			}
			// Write the zeros a real clone would have produced
			_, err := dst.WriteAt(make([]byte, srcLength), int64(dstOffset))
			return err
		}
		dstName := filepath.Join(dir, "out3")
		require.NoError(t, os.WriteFile(dstName, bytes.Repeat([]byte{0xff}, int(length)), 0644))
		dst, err := os.OpenFile(dstName, os.O_RDWR, 0)
		require.NoError(t, err)
		defer dst.Close()

		copied, cloned, err := newSection().WriteInto(dst, 0, length, blocksize, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(blocksize), cloned)
		assert.Equal(t, length-blocksize, copied)

		got, err := os.ReadFile(dstName)
		require.NoError(t, err)
		assert.Equal(t, make([]byte, length), got)
	})
}
