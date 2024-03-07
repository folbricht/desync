package desync

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type ThrottleOptions struct {
	eventRate                float64
	burstRate int
	timeout time.Duration
	immediateOrFail bool
}

type RateLimitedLocalStore struct {

	wrappedStore WriteStore

	limiter  *rate.Limiter

	options ThrottleOptions

}

var RateLimitedExceeded = errors.New("Rate Limit Exceeded")

func NewRateLimitedLocalStore(s WriteStore, options ThrottleOptions) *RateLimitedLocalStore {
	
	limiter := rate.NewLimiter(rate.Limit(options.eventRate), options.burstRate)
	return &RateLimitedLocalStore{wrappedStore: s,limiter: limiter, options: options }
}

func (s RateLimitedLocalStore) GetChunk(id ChunkID) (*Chunk, error) {

	return s.wrappedStore.GetChunk(id)
}

func (s RateLimitedLocalStore) HasChunk(id ChunkID) (bool, error) {


	return s.wrappedStore.HasChunk(id)
}


func (s RateLimitedLocalStore) StoreChunk(chunk *Chunk) error {
	
	// This isn't ideal because what I'm really interested is in size over the wire.
	_, err := chunk.Data()
	if err != nil {
		return err
	}
	
	//size := len(b)
	ctx, cancel:= context.WithTimeout(context.Background(), s.options.timeout)
	defer cancel()
	
	if s.options.immediateOrFail{
	   if !s.limiter.AllowN(time.Now(),1){
			err = errors.New("Unable to immediately store")
	   }
	} else{
		err = s.limiter.WaitN(ctx,1)
	}
	
	if err != nil {

		fmt.Println("Rate limit context error:", err)
		return RateLimitedExceeded
	}

	return s.wrappedStore.StoreChunk(chunk)
	
}
