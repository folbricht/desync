package desync

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)


func NewTestRateLimitedLocalStore(t *testing.T, eventRate float64, burstRate int) *RateLimitedStore{

	src_store_dir := t.TempDir()
	src_store, err := NewLocalStore(src_store_dir, StoreOptions{})
	require.NoError(t, err)
	
	throttleOptions := ThrottleOptions{eventRate,burstRate}
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

	
	throttledStore := NewTestRateLimitedLocalStore(t,1,1)
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
	throttledStore := NewTestRateLimitedLocalStore(t,1,chunk_count + 1)

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
	throttledStore := NewTestRateLimitedLocalStore(t,float64( chunk_count + 1),1)
	
	start := time.Now()

	
	chunk_ids := make([]ChunkID, chunk_count)
	storeLoop(t,chunk_count,chunk_ids,*throttledStore)
	
	finish := time.Now()
	require.True(t, finish.Sub(start).Seconds() < 2)
	chunkCheck(t,chunk_count,chunk_ids,*throttledStore)

	
}


func TestNoTimeout(t *testing.T) {
	chunk_count := 10
	// Bucket only has one token, replenish 1 per second. Timeout is 11 seconds.
	throttledStore := NewTestRateLimitedLocalStore(t,1,1)
	

	chunk_ids := make([]ChunkID, chunk_count)
	storeLoop(t,chunk_count,chunk_ids,*throttledStore)
}


func TestHasNoChunk(t *testing.T) {
	
	throttledStore := NewTestRateLimitedLocalStore(t,2,2)
	chunk := makeChunk(t)
	has, err := throttledStore.HasChunk(chunk.ID())
	require.Nil(t,err)
	require.False(t, has)
	
}

func TestStoresAndHasChunk(t *testing.T) {
	
	throttledStore := NewTestRateLimitedLocalStore(t,2,2)
	chunk := makeChunk(t)
	err := throttledStore.StoreChunk(chunk)
	require.Nil(t,err)
	has, err := throttledStore.HasChunk(chunk.ID())
	require.Nil(t,err)
	require.True(t, has)
	
}


func TestStoresAndHasChunkWithWaits(t *testing.T) {
	
	// Start with 1 token, replenish at 1 token per second. Consume 1 token per HasChunk, should take ~5s
	throttledStore := NewTestRateLimitedLocalStore(t,float64(1),1)
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

func TestHTTPHandlerReadWriteWithThrottle(t *testing.T) {


	tests:= map[string]struct {
		eventRate float64
		burstRate int 
		minTime float64
		maxTime float64
		ops int 
		readers int 
	} {
		
		"full bucket" : {10,220,0,5,100,100},
		"bucket with 50 tokens and a 10 t/s replenishment rate" : {10,50,13,17,100,100},
		"bucket with 1 tokens and a 0.2 t/s replenishment rate" : {0.2,1,45,55,5,5},
		
	}

	for name, test := range tests {
		t.Run(name,func(t *testing.T) {

	
	
	upstream:= NewTestRateLimitedLocalStore(t,test.eventRate,test.burstRate)
	rw := httptest.NewServer(NewHTTPHandler(upstream, true, false, []converter{Compressor{}}, ""))
	defer rw.Close()

	chunkIdChan := make(chan ChunkID,test.ops)
	defer close(chunkIdChan)

	var wg sync.WaitGroup
	rwStoreURL, _ := url.Parse(rw.URL)
	start := time.Now()
	for i:=0; i < test.readers; i++{

		
		go func ()  {
			defer wg.Done()
			
			rwStore, err := NewRemoteHTTPStore(rwStoreURL, StoreOptions{})
			require.NoError(t, err)
			dataIn := []byte("some data")
			chunkIn := NewChunk(dataIn)
			err = rwStore.StoreChunk(chunkIn)
			require.NoError(t, err)
			chunkIdChan <- chunkIn.ID()
		
		}()
		wg.Add(1) 
		go func ()  {
			defer wg.Done()
			rwStore, err := NewRemoteHTTPStore(rwStoreURL, StoreOptions{})
			require.NoError(t, err)
			id  := <- chunkIdChan
			hasChunk, err := rwStore.HasChunk(id)
			require.NoError(t, err)
			require.True(t, hasChunk)
		
		}()
		wg.Add(1)
	}
	
	wg.Wait()

	finish := time.Now()
	diff := finish.Sub(start).Seconds()
	require.True(t, diff < test.maxTime)
	require.True(t, diff > test.minTime)
	})
}
	
}
