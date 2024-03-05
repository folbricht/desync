package desync

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func random_data() ([]byte,error){

    b := make([]byte, 16)
    _, err := rand.Read(b)
    if err != nil {
        fmt.Println("Error: ", err)
        return b,err
    }

    return b,nil
}

func TestLimiter(t *testing.T){

	// This is testing the framework. So not great, but I needed to get my head around what the relation
	// between events and burst do
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

	// assert our store is working
	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)
	
	chunk_data := []byte("some data")
	chunk := NewChunk(chunk_data)

	err = src_store.StoreChunk(chunk)
	require.Nil(t,err)

	throttleOptions := ThrottleOptions{1,1,time.Second*60}
	throttledStore  := NewRateLimitedLocalStore(src_store, throttleOptions)

	
	chunk_data = []byte("different data")
	chunk = NewChunk(chunk_data)
	chunk_id := chunk.ID()
	
	err  = throttledStore.StoreChunk(chunk)
	require.Nil(t,err)
	hasChunk, err := throttledStore.HasChunk(chunk_id)
	require.Nil(t,err)
	require.True(t,hasChunk)

	start := time.Now()
	// We start with 1 token in the bucket and replenish at 1 token per second
	// This should take ~10 seconds.
	// We test it takes 8s to guard against flakiness
	for i := 0; i < 10; i++ {
		err  = throttledStore.StoreChunk(chunk)
		// This test will eventually fail when I get deadlines enabled
		require.Nil(t,err)
	}
	finish := time.Now()

	require.True(t, finish.Sub(start).Seconds() > 8)
}

func TestForAFullBucketNoWait(t *testing.T) {

	src_store_dir := t.TempDir()

	// assert our store is working
	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)
	throttleOptions := ThrottleOptions{1,100,time.Second*60}
	throttledStore  := NewRateLimitedLocalStore(src_store, throttleOptions)

	start := time.Now()

	
	chunk_ids := make([]ChunkID, 10)
	// The bucket is full, we shouldn't wait
	for i := 0; i < 10; i++ {
		
		data,err := random_data()
		require.NoError(t,err)
		chunk := NewChunk(data)
		chunk_ids[i] = chunk.ID()
		err  = throttledStore.StoreChunk(chunk)
		require.Nil(t,err)
	}
	finish := time.Now()
	require.True(t, finish.Sub(start).Seconds() < 2)
	for i := 0; i < 10; i++ {

		has,err :=	throttledStore.HasChunk(chunk_ids[i])
		require.Nil(t,err)
		require.True(t,has)

	}
}