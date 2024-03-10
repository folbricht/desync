package desync

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)


func NewTestRateLimitedLocalStore(t *testing.T, eventRate float64, burstRate int, timeout time.Duration, immediateOrFail bool) *RateLimitedStore{

	src_store_dir := t.TempDir()
	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)
	
	throttleOptions := ThrottleOptions{eventRate,burstRate,timeout,immediateOrFail}
	store :=NewRateLimitedStore(src_store, throttleOptions)
	require.Equal(t,store.options.burstRate,burstRate )
	return store

}

func random_data() ([]byte,error){

    b := make([]byte, 16)
    _, err := rand.Read(b)
    if err != nil {
        fmt.Println("Error: ", err)
        return b,err
    }

    return b,nil
}

func makeChunk(t *testing.T,) *Chunk{
	data,err := random_data()
	require.NoError(t,err)
	chunk := NewChunk(data)
	return chunk
}

func storeLoop(t *testing.T, max int, chunk_ids []ChunkID, store RateLimitedStore){
	
	for i := 0; i < max; i++ {
		
		
		chunk := makeChunk(t)
		chunk_ids[i] = chunk.ID()
		err  := store.StoreChunk(chunk)
		require.Nil(t,err)
	}
	

}

func chunkCheck(t *testing.T, max int, chunk_ids []ChunkID, store RateLimitedStore) {
	for i := 0; i < max; i++ {

		has,err :=	store.HasChunk(chunk_ids[i])
		require.Nil(t,err)
		require.True(t,has)

	}
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

	
	throttledStore := NewTestRateLimitedLocalStore(t,1,1,time.Second*60, false)
	chunk_data := []byte("different datas")
	chunk := NewChunk(chunk_data)

	
	start := time.Now()
	// We start with 1 token in the bucket and replenish at 1 token per second
	// This should take ~10 seconds.
	// We test it takes 8s to guard against flakiness
	for i := 0; i < 10; i++ {
		err  := throttledStore.StoreChunk(chunk)
		// This test will eventually fail when I get deadlines enabled
		require.Nil(t,err)
	}
	finish := time.Now()

	require.True(t, finish.Sub(start).Seconds() > 8)
}


func TestForAFullBucketNoWait(t *testing.T) {

	chunk_count := 10
	// Bucket has initial size chunk_count
	throttledStore := NewTestRateLimitedLocalStore(t,1,chunk_count + 1,time.Second*60, false)

	chunk_ids := make([]ChunkID, chunk_count)
	start := time.Now()
	// The bucket is full, we shouldn't have to wait
	storeLoop(t,chunk_count,chunk_ids,*throttledStore)
	finish := time.Now()
	require.True(t, finish.Sub(start).Seconds() < 2)
	chunkCheck(t,chunk_count,chunk_ids,*throttledStore)
}

func TestForAFastReplenishmentRateLittleWait(t *testing.T) {

	chunk_count := 10
	// Bucket only has one token, but we replenish chunk_count tokens every second
	throttledStore := NewTestRateLimitedLocalStore(t,float64( chunk_count + 1),1,time.Second*60,false)
	
	start := time.Now()

	
	chunk_ids := make([]ChunkID, chunk_count)
	storeLoop(t,chunk_count,chunk_ids,*throttledStore)
	
	finish := time.Now()
	require.True(t, finish.Sub(start).Seconds() < 2)
	chunkCheck(t,chunk_count,chunk_ids,*throttledStore)

	
}

func TestTimeout(t *testing.T) {

	// Bucket only has one token, and we replenish very slowly. We timeout, so second invocation will fail
	throttledStore := NewTestRateLimitedLocalStore(t,float64(1) /100,1,time.Millisecond*1000, false)
	


	data,err := random_data()
	require.NoError(t,err)
	chunk := NewChunk(data)
	err  = throttledStore.StoreChunk(chunk)
	require.Nil(t,err)
	err  = throttledStore.StoreChunk(chunk)
	require.NotNil(t,err)
	require.True(t, errors.Is(err,RateLimitedExceeded))
}

func TestNoTimeout(t *testing.T) {
	chunk_count := 10
	// Bucket only has one token, replenish 1 per second. Timeout is 11 seconds.
	throttledStore := NewTestRateLimitedLocalStore(t,1,1,time.Second*11, false)
	

	chunk_ids := make([]ChunkID, chunk_count)
	storeLoop(t,chunk_count,chunk_ids,*throttledStore)
}

func TestImmediateOrFail(t *testing.T) {

	// Bucket only has one token, and we replenish very slowly. Second invocation will fail
	throttledStore := NewTestRateLimitedLocalStore(t,float64(1) /100,1,time.Second*60, true)
	


	data,err := random_data()
	require.NoError(t,err)
	chunk := NewChunk(data)

	err  = throttledStore.StoreChunk(chunk)
	require.Nil(t,err)

	err  = throttledStore.StoreChunk(chunk)
	require.NotNil(t,err)
	
}

func TestHasNoChunk(t *testing.T) {
	
	throttledStore := NewTestRateLimitedLocalStore(t,2,2,time.Second*11, false)
	chunk := makeChunk(t)
	has, err := throttledStore.HasChunk(chunk.ID())
	require.Nil(t,err)
	require.False(t, has)
	
}

func TestStoresAndHasChunk(t *testing.T) {
	
	throttledStore := NewTestRateLimitedLocalStore(t,2,2,time.Second*1, false)
	chunk := makeChunk(t)
	err := throttledStore.StoreChunk(chunk)
	require.Nil(t,err)
	has, err := throttledStore.HasChunk(chunk.ID())
	require.Nil(t,err)
	require.True(t, has)
	
}


func TestStoresAndHasChunkWithWaits(t *testing.T) {
	
	// Start with 1 token, replenish at 1 token per second. Consume 1 token per HasToken, should take ~5s
	throttledStore := NewTestRateLimitedLocalStore(t,float64(1),1,time.Second*10, false)
	chunk := makeChunk(t)
	start := time.Now()
	err := throttledStore.StoreChunk(chunk)
	require.Nil(t,err)
	count:= 0
	for count < 5{
		count++
		has, err := throttledStore.HasChunk(chunk.ID())
		require.Nil(t,err)
		require.True(t, has)
	}
	finish := time.Now()
	require.True(t, finish.Sub(start).Seconds() < 7)
	require.True(t, finish.Sub(start).Seconds() > 3)
	
}
