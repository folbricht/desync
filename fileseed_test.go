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
// cloning of blocks fails, e.g. due to alignment constraints or because the
// source hasn't been committed to disk yet. The seed segment is expected to
// fall back to copying the data.
func TestFileSeedSegmentCloneFallback(t *testing.T) {
	cloneRange = func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
		return errors.New("simulated clone failure")
	}
	defer func() { cloneRange = CloneRange }()

	const blocksize = 4096
	size := uint64(3*blocksize + 100)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	dir := t.TempDir()
	srcName := filepath.Join(dir, "seed")
	require.NoError(t, os.WriteFile(srcName, data, 0644))

	dstName := filepath.Join(dir, "out")
	dst, err := os.Create(dstName)
	require.NoError(t, err)
	defer dst.Close()
	require.NoError(t, dst.Truncate(int64(size)))

	segment := newFileSeedSegment(srcName, []IndexChunk{{Start: 0, Size: size}}, true)
	copied, cloned, err := segment.WriteInto(dst, 0, size, blocksize, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), cloned)
	assert.Equal(t, size, copied)

	got, err := os.ReadFile(dstName)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// A segment smaller than a filesystem block contains no cloneable blocks and
// must be copied without touching data outside its range. Large blocks are
// common on ZFS where st_blksize reports the recordsize, 128k by default.
func TestFileSeedSegmentSmallerThanBlock(t *testing.T) {
	var cloneCalls int
	cloneRange = func(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
		cloneCalls++
		return nil
	}
	defer func() { cloneRange = CloneRange }()

	const blocksize = 131072
	size := uint64(4096)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i + 1)
	}
	dir := t.TempDir()
	srcName := filepath.Join(dir, "seed")
	require.NoError(t, os.WriteFile(srcName, data, 0644))

	// Make the target larger than the segment and fill it with a marker to
	// detect writes outside the segment range
	dstName := filepath.Join(dir, "out")
	require.NoError(t, os.WriteFile(dstName, bytes.Repeat([]byte{0xff}, 3*blocksize), 0644))
	dst, err := os.OpenFile(dstName, os.O_RDWR, 0)
	require.NoError(t, err)
	defer dst.Close()

	segment := newFileSeedSegment(srcName, []IndexChunk{{Start: 0, Size: size}}, true)
	copied, cloned, err := segment.WriteInto(dst, 0, size, blocksize, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), cloned)
	assert.Equal(t, size, copied)
	assert.Equal(t, 0, cloneCalls)

	got, err := os.ReadFile(dstName)
	require.NoError(t, err)
	assert.Equal(t, data, got[:size])
	assert.Equal(t, bytes.Repeat([]byte{0xff}, 3*blocksize-int(size)), got[size:])
}
