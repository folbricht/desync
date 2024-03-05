package desync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestLimiter(t *testing.T){

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()

	limiter := rate.NewLimiter(rate.Limit(50), 50)
	err := limiter.WaitN(ctx,1)
	require.Nil(t, err)

	limiter = rate.NewLimiter(rate.Limit(2), 1)
	_ = limiter.WaitN(ctx,1)
	require.Nil(t, err)
	err = limiter.WaitN(ctx,1)
	require.Nil(t, err)

	limiter = rate.NewLimiter(rate.Limit(1), 1)
	_ = limiter.WaitN(ctx,1)
	require.Nil(t, err)
	err = limiter.WaitN(ctx,1)
	require.NotNil(t, err)




}

func TestCopyWithNoLimit(t *testing.T) {

	src_store_dir := t.TempDir()

	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)
	throttleOptions := ThrottleOptions{100,100,time.Millisecond*10000}
	throttledStore  := NewRateLimitedLocalStore(src_store, throttleOptions)

	chunk_data := []byte("some data")
	chunk := NewChunk(chunk_data)
	chunk_id := chunk.ID()

	err  = throttledStore.StoreChunk(chunk)
	require.NotNil(t,err)
	hasChunk, err := throttledStore.HasChunk(chunk_id)
	require.NotNil(t,err)
	require.True(t,hasChunk)


}