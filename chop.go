package desync

import (
	"context"
)

// ChopFile split a file according to a list of chunks obtained from an Index
// and stores them in the provided store
func ChopFile(ctx context.Context, name string, chunks []IndexChunk, ws WriteStore, n int, pb ProgressBar) error {
	var in = make(chan ChunkJob)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := NewChunkStorage(ctx, cancel, name, n, ws, in, pb)
	s.Start()

	// Feed the workers, stop if there are any errors
	var num int
loop:
	for _, c := range chunks {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		in <- ChunkJob{num: num, chunk: c}
		num++
	}
	close(in)

	// s.GetResults() will block until all chunk jobs are processed
	_, pErr := s.GetResults()

	return pErr
}
