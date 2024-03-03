package desync

import (
	"context"
	"testing"
	"time"

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

	Copy(context.Background(), chunks, src_store, dst_store, 1, NewProgressBar(""),false,100)
	require.NoError(t, err)
	has_the_chunk, _ := dst_store.HasChunk(first_chunk_id)

	require.True(t, has_the_chunk)
}

func TestTimeThrottle(t *testing.T) {

	// If the wait time is zero, we never wait

	wait := time.Duration(time.Millisecond * 0)
	throttle := TimeThrottle{time.Now(), wait}
	w, _ := throttle.calculateThrottle()
	require.False(t, w)

	past := time.Now().Add(-time.Hour * 1)
	throttle = TimeThrottle{past, wait}
	w, _ = throttle.calculateThrottle()
	require.False(t, w)

	wait = time.Duration(time.Hour * 1)
	throttle = TimeThrottle{time.Now(), wait}
	w, d := throttle.calculateThrottle()
	require.True(t, w)
	require.True(t, d > time.Duration(time.Minute*59))

	// Assuming out last exection was in the past, we don't wait
	past = time.Now().Add(-time.Hour * 1)
	wait = time.Duration(time.Second * 60)
	throttle = TimeThrottle{past, wait}
	w, _ = throttle.calculateThrottle()
	require.False(t, w)

	wait = time.Duration(time.Second * 60)
	throttle = TimeThrottle{time.Now(), wait}
	present := throttle.lastExecutionTime
	// Without the sleep this can fail. At least on windows
	// https://github.com/folbricht/desync/actions/runs/8131384060/job/22220648517?pr=258
	time.Sleep(time.Duration(time.Millisecond*100))
	throttle.reset()
	future := throttle.lastExecutionTime
	require.True(t, present.Before(future))
}
