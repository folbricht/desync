package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/folbricht/desync"
)

const usage = `desync <command> [options]
desync <command> -h

Commands:
make         - split a blob into chunks and create an index file
extract      - build a blob from a caibx file
verify       - verify the integrity of a local store
list-chunks  - list all chunk IDs contained in a caibx
cache        - populate a cache without writing to a blob
chop         - split a blob based on existing caibx and store the chunks
pull         - serve chunks using the casync protocol over stdin/stdout
untar        - extract directory tree from a catar file
prune        - remove all unreferenced chunks from a local store
chunk-server - start a HTTP chunk server
`

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, usage)
		flag.PrintDefaults()
	}

	if len(os.Args) < 2 {
		die(errors.New("No command given. See -h for help."))
	}

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

	cmd := os.Args[1]
	args := os.Args[2:]

	handlers := map[string]func(context.Context, []string) error{
		"-h":           help,
		"extract":      extract,
		"verify":       verify,
		"cache":        cache,
		"list-chunks":  list,
		"chop":         chop,
		"pull":         pull,
		"untar":        untar,
		"prune":        prune,
		"chunk-server": server,
		"make":         makeCmd,
	}
	h, ok := handlers[cmd]
	if !ok {
		die(fmt.Errorf("Unknown command %s", cmd))
	}

	if err := h(ctx, args); err != nil {
		die(err)
	}
}

func help(ctx context.Context, args []string) error {
	flag.Usage()
	os.Exit(1)
	return nil
}

func readCaibxFile(name string) (c desync.Index, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer f.Close()
	return desync.IndexFromReader(f)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

type multiArg struct {
	list []string
}

func (a *multiArg) Set(v string) error {
	a.list = append(a.list, v)
	return nil
}

func (a *multiArg) String() string { return "" }
