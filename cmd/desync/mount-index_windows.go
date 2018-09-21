package main

import (
	"context"
	"fmt"
)

func mountIdx(context.Context, []string) error {
	return fmt.Errorf("Subcommand not available on this platform")
}
