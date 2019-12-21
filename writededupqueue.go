package desync

var _ WriteStore = &WriteDedupQueue{}

// WriteDedupQueue wraps a writable store and provides deduplication of incoming chunk requests and store
// operation. This is useful when a burst of requests for the same chunk is received and the chunk store
// serving those is slow or when the underlying filesystem does not support atomic rename operations
// (Windows). With the DedupQueue wrapper, concurrent requests for the same chunk will result in just
// one request to the upstream store. Implements the WriteStore interface.
type WriteDedupQueue struct {
	S WriteStore
	*DedupQueue
}

// NewWriteDedupQueue initializes a new instance of the wrapper.
func NewWriteDedupQueue(store WriteStore) *WriteDedupQueue {
	return &WriteDedupQueue{
		S:          store,
		DedupQueue: NewDedupQueue(store),
	}
}

func (q *WriteDedupQueue) GetChunk(id ChunkID) (*Chunk, error) {
	return q.DedupQueue.GetChunk(id)
}

func (q *WriteDedupQueue) HasChunk(id ChunkID) (bool, error) {
	return q.DedupQueue.HasChunk(id)
}

func (q *WriteDedupQueue) StoreChunk(chunk *Chunk) error {
	id := chunk.ID()
	req, isInFlight := q.getChunkQueue.loadOrStore(id)

	if isInFlight { // The request is already in-flight, wait for it to come back
		_, err := req.wait()
		return err
	}

	// This request is the first one for this chunk, execute as normal
	err := q.S.StoreChunk(chunk)

	// Signal to any others that wait for us that we're done, they'll use our data
	// and don't need to hit the store themselves
	req.markDone(chunk, err)

	// We're done, drop the request from the queue to avoid keeping all the chunk data
	// in memory after the request is done
	q.getChunkQueue.delete(id)

	return err
}
