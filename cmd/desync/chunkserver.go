package main

import "context"

func chunkServer(ctx context.Context, args []string) error {
	return server(ctx, ChunkServer, args)
}
