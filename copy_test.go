package desync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	src_store_dir := t.TempDir()
	dst_store_dir := t.TempDir()

	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)

	dst_store, err := NewLocalStore(dst_store_dir, StoreOptions{})
	require.NoError(t, err)

	first_chunk_data := []byte("some data")
	first_chunk := NewChunk(first_chunk_data)
	first_chunk_id := first_chunk.ID()

	src_store.StoreChunk(first_chunk)
	has_the_stored_chunk, _ := src_store.HasChunk(first_chunk_id)
	require.True(t, has_the_stored_chunk)

	chunks := make([]ChunkID, 1)
	chunks[0] = first_chunk_id

	Copy(context.Background(), chunks, src_store, dst_store, 1, NewProgressBar(""))
	require.NoError(t, err)
	has_the_chunk, _ := dst_store.HasChunk(first_chunk_id)

	require.True(t, has_the_chunk)
}
