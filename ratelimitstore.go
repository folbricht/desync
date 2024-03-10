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


type RateLimitedStore struct {

	wrappedStore WriteStore

	limiter  *rate.Limiter

	options ThrottleOptions

}

var RateLimitedExceeded = errors.New("Rate Limit Exceeded")


func NewRateLimitedStore(s WriteStore, options ThrottleOptions) *RateLimitedStore {
	
	limiter := rate.NewLimiter(rate.Limit(options.eventRate), options.burstRate)
	return &RateLimitedStore{wrappedStore: s,limiter: limiter, options: options }
}

func (s RateLimitedStore) GetChunk(id ChunkID) (*Chunk, error) {

	chunk,err := s.wrappedStore.GetChunk(id)
	if err != nil{
		return chunk, err
	}
	ctx, cancel:= context.WithTimeout(context.Background(), s.options.timeout)
	defer cancel()
	err  = s.limiter.WaitN(ctx,1)
	return chunk, err
}

func (s RateLimitedStore) HasChunk(id ChunkID) (bool, error) {

	

	has,err := s.wrappedStore.HasChunk(id)
	if err != nil{
		return has, err
	}
	ctx, cancel:= context.WithTimeout(context.Background(), s.options.timeout)
	defer cancel()
	err  = s.limiter.WaitN(ctx,1)
	return has, err
	
}


func (s RateLimitedStore) StoreChunk(chunk *Chunk) error {
	
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
