package desync

import (
	"fmt"
	"sync"
)

var _ Store = &DedupQueue{}

// DedupQueue wraps a store and provides deduplication of incoming chunk requests. This is useful when
// a burst of requests for the same chunk is received and the chunk store serving those is slow. With
// the DedupQueue wrapper, concurrent requests for the same chunk will result in just one request to the
// upstread store. Implements the Store interface.
type DedupQueue struct {
	store         Store
	mu            sync.Mutex
	getChunkQueue *queue
	hasChunkQueue *queue
}

// NewDedupQueue initializes a new instance of the wrapper.
func NewDedupQueue(store Store) *DedupQueue {
	return &DedupQueue{
		store:         store,
		getChunkQueue: newQueue(),
		hasChunkQueue: newQueue(),
	}
}

func (q *DedupQueue) GetChunk(id ChunkID) (*Chunk, error) {
	req, isInFlight := q.getChunkQueue.loadOrStore(id)

	if isInFlight { // The request is already in-flight, wait for it to come back
		data, err := req.wait()
		switch b := data.(type) {
		case nil:
			return nil, err
		case *Chunk:
			return b, err
		default:
			return nil, fmt.Errorf("internal error: unexpected type %T", data)
		}
	}

	// This request is the first one for this chunk, execute as normal
	b, err := q.store.GetChunk(id)

	// Signal to any others that wait for us that we're done, they'll use our data
	// and don't need to hit the store themselves
	req.markDone(b, err)

	// We're done, drop the request from the queue to avoid keeping all the chunk data
	// in memory after the request is done
	q.getChunkQueue.delete(id)

	return b, err
}

func (q *DedupQueue) HasChunk(id ChunkID) (bool, error) {
	req, isInFlight := q.hasChunkQueue.loadOrStore(id)

	if isInFlight { // The request is already in-flight, wait for it to come back
		data, err := req.wait()
		return data.(bool), err
	}

	// This request is the first one for this chunk, execute as normal
	hasChunk, err := q.store.HasChunk(id)

	// Signal to any others that wait for us that we're done, they'll use our data
	// and don't need to hit the store themselves
	req.markDone(hasChunk, err)

	// We're done, drop the request from the queue to avoid keeping all in memory
	q.hasChunkQueue.delete(id)
	return hasChunk, err
}

func (q *DedupQueue) String() string { return q.store.String() }

func (q *DedupQueue) Close() error { return q.store.Close() }

// queue manages the in-flight requests
type queue struct {
	requests map[ChunkID]*request
	mu       sync.Mutex
}

func newQueue() *queue {
	return &queue{requests: make(map[ChunkID]*request)}
}

// Returns either a new request, or an existing one from the queue.
func (q *queue) loadOrStore(id ChunkID) (*request, bool) {
	q.mu.Lock()
	req, isInFlight := q.requests[id]
	if !isInFlight {
		req = newRequest()
		q.requests[id] = req
	}
	q.mu.Unlock()
	return req, isInFlight
}

func (q *queue) delete(id ChunkID) {
	q.mu.Lock()
	delete(q.requests, id)
	q.mu.Unlock()
}

// queueRequests is used to dedup requests for GetChunk() or HasChunk() with the data
// being either the chunk itself or a bool in case of HasChunk().
type request struct {
	data interface{}
	err  error
	done chan struct{}
}

func newRequest() *request {
	return &request{done: make(chan struct{})}
}

// Wait for the request to complete. Returns the data as well as the error from the request.
func (r *request) wait() (interface{}, error) {
	<-r.done
	return r.data, r.err
}

// Set the result data and marks this request as comlete.
func (r *request) markDone(data interface{}, err error) {
	r.data = data
	r.err = err
	close(r.done)
}
