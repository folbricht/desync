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

var sighup = make(chan os.Signal)

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

	// Install a signal handler for SIGHUP. This does not interrupt execution
	// and is meant to trigger events like a config reload in some commands
	signal.Notify(sighup, syscall.SIGHUP)

	// Read config early
	cobra.OnInitialize(initConfig, setDigestAlgorithm, setVerbose)

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
		newinspectChunksCommand(ctx),
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
		newManpageCommand(ctx, rootCmd),
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
