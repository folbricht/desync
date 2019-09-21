package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// Define writers for STDOUT and STDERR that are used in the commands.
// This allows tests to override them and write to buffers instead.
var (
	stdout io.Writer = os.Stdout
	stderr io.Writer = os.Stderr
)

func main() {
	// Install a signal handler for SIGINT or SIGTERM to cancel a context in
	// order to clean up and shut down gracefully if Ctrl+C is hit.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	// Read config early
	cobra.OnInitialize(initConfig, setDigestAlgorithm)

	// Register the sub-commands under root
	rootCmd := newRootCommand()
	rootCmd.AddCommand(
		newConfigCommand(ctx),
		newCatCommand(ctx),
		newCacheCommand(ctx),
		newMakeCommand(ctx),
		newExtractCommand(ctx),
		newChopCommand(ctx),
		newChunkCommand(ctx),
		newInfoCommand(ctx),
		newListCommand(ctx),
		newMountIndexCommand(ctx),
		newPruneCommand(ctx),
		newPullCommand(ctx),
		newIndexServerCommand(ctx),
		newChunkServerCommand(ctx),
		newTarCommand(ctx),
		newUntarCommand(ctx),
		newVerifyCommand(ctx),
		newVerifyIndexCommand(ctx),
		newMtreeCommand(ctx),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func printJSON(w io.Writer, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(w, string(b))
	return nil
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
