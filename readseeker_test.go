package desync

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chunkID returns the ID desync would assign to the given plain data.
func chunkID(b []byte) ChunkID { return Digest.Sum(b) }

// TestIndexReadSeekerSizeMismatchPanic ensures that an index declaring a chunk
// size larger than the actual stored chunk results in an error rather than a
// "slice bounds out of range" panic in the read path.
func TestIndexReadSeekerSizeMismatchPanic(t *testing.T) {
	data := []byte("data") // real chunk is 4 bytes
	store := &TestStore{Chunks: map[ChunkID][]byte{chunkID(data): data}}

	// The index lies: it claims the chunk is 1000 bytes long.
	idx := Index{
		Index: FormatIndex{ChunkSizeMax: ChunkSizeMaxDefault},
		Chunks: []IndexChunk{
			{ID: chunkID(data), Start: 0, Size: 1000},
		},
	}

	r := NewIndexReadSeeker(idx, store)

	// Seek past the real chunk length but within the declared size.
	_, err := r.Seek(200, io.SeekStart)
	require.NoError(t, err)

	buf := make([]byte, 16)
	require.NotPanics(t, func() {
		_, err = r.Read(buf)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected size for chunk")
}

// TestIndexReadSeekerSizeMismatchNoLoop ensures that a short (under-sized) chunk
// that is not the last one causes Read to return an error promptly instead of
// spinning in a zero-progress loop.
func TestIndexReadSeekerSizeMismatchNoLoop(t *testing.T) {
	short := []byte("data")    // real chunk is 4 bytes
	tail := []byte("trailing") // a normal following chunk
	store := &TestStore{Chunks: map[ChunkID][]byte{
		chunkID(short): short,
		chunkID(tail):  tail,
	}}

	// First chunk claims 1000 bytes but only 4 are stored, and it's followed by
	// another chunk so the "last chunk" short-read break does not apply.
	idx := Index{
		Index: FormatIndex{ChunkSizeMax: ChunkSizeMaxDefault},
		Chunks: []IndexChunk{
			{ID: chunkID(short), Start: 0, Size: 1000},
			{ID: chunkID(tail), Start: 1000, Size: uint64(len(tail))},
		},
	}

	r := NewIndexReadSeeker(idx, store)

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, err := r.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected size for chunk")
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not return; likely spinning in a zero-progress loop")
	}
}

// TestIndexReadSeekerValid verifies the read path still returns the correct
// content for a well-formed multi-chunk index, including a null chunk served
// from memory, and that seeking works.
func TestIndexReadSeekerValid(t *testing.T) {
	head := []byte("hello, world")
	null := make([]byte, ChunkSizeMaxDefault)
	tail := []byte("goodbye, world")

	store := &TestStore{Chunks: map[ChunkID][]byte{
		chunkID(head): head,
		chunkID(tail): tail,
		// the null chunk is intentionally not stored; it must be served from memory
	}}

	idx := Index{
		Index: FormatIndex{ChunkSizeMax: ChunkSizeMaxDefault},
		Chunks: []IndexChunk{
			{ID: chunkID(head), Start: 0, Size: uint64(len(head))},
			{ID: NewNullChunk(ChunkSizeMaxDefault).ID, Start: uint64(len(head)), Size: ChunkSizeMaxDefault},
			{ID: chunkID(tail), Start: uint64(len(head)) + ChunkSizeMaxDefault, Size: uint64(len(tail))},
		},
	}

	want := bytes.Join([][]byte{head, null, tail}, nil)

	// Full sequential read.
	r := NewIndexReadSeeker(idx, store)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	// Seek to the start of the last chunk and read its content.
	off := int64(len(head)) + int64(ChunkSizeMaxDefault)
	_, err = r.Seek(off, io.SeekStart)
	require.NoError(t, err)
	got, err = io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, tail, got)
}
