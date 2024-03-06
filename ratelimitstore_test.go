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


func NewTestRateLimitedLocalStore(t *testing.T, eventRate float64, burstRate int, timeout time.Duration) *RateLimitedLocalStore{

	src_store_dir := t.TempDir()
	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)
	
	throttleOptions := ThrottleOptions{eventRate,burstRate,timeout}
	return NewRateLimitedLocalStore(src_store, throttleOptions)


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


func storeLoop(t *testing.T, max int, chunk_ids []ChunkID, store RateLimitedLocalStore){
	
	for i := 0; i < max; i++ {
		
		data,err := random_data()
		require.NoError(t,err)
		chunk := NewChunk(data)
		chunk_ids[i] = chunk.ID()
		err  = store.StoreChunk(chunk)
		require.Nil(t,err)
	}
	

}

func chunkCheck(t *testing.T, max int, chunk_ids []ChunkID, store RateLimitedLocalStore) {
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

	
	throttledStore := NewTestRateLimitedLocalStore(t,1,1,time.Second*60)
	chunk_data := []byte("different data")
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
	throttledStore := NewTestRateLimitedLocalStore(t,1,chunk_count + 1,time.Second*60)

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
	// Bucket only has one, but we replenish chunk_count tokens every second
	throttledStore := NewTestRateLimitedLocalStore(t,float64( chunk_count + 1),1,time.Second*60)
	
	start := time.Now()

	
	chunk_ids := make([]ChunkID, chunk_count)
	storeLoop(t,chunk_count,chunk_ids,*throttledStore)
	
	finish := time.Now()
	require.True(t, finish.Sub(start).Seconds() < 2)
	chunkCheck(t,chunk_count,chunk_ids,*throttledStore)

	
}