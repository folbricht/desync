package main

import "context"

func indexServer(ctx context.Context, args []string) error {
	return server(ctx, IndexServer, args)
}
