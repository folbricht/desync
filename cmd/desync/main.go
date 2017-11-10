package main

import (
	"context"
	"crypto/sha512"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"

	casync "github.com/folbricht/go-casync"
)

const usage = `desync [options] <caibx> <output>`

func main() {
	var (
		storeLocation string
		cacheLocation string
		n             int
		err           error
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.StringVar(&storeLocation, "s", "", "casync store location")
	flag.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flag.IntVar(&n, "n", 10, "number of goroutines")
	flag.Parse()

	if flag.NArg() < 2 {
		die(errors.New("Not enough arguments. See -h for help."))
	}
	if flag.NArg() > 2 {
		die(errors.New("Too many arguments. See -h for help."))
	}

	inFile := flag.Arg(0)
	outFile := flag.Arg(1)
	if inFile == outFile {
		die(errors.New("Input and output filenames match."))
	}

	// Checkout the store
	if storeLocation == "" {
		die(errors.New("No casync store provided. See -h for help."))
	}
	loc, err := url.Parse(storeLocation)
	if err != nil {
		die(fmt.Errorf("Unable to parse store location: %s", err))
	}
	var s casync.Store
	switch loc.Scheme {
	case "ssh":
		s, err = casync.NewRemoteSSHStore(loc, n)
		if err != nil {
			die(err)
		}
	case "":
		s, err = casync.NewLocalStore(loc.Path)
		if err != nil {
			die(err)
		}
	default:
	}

	// See if we want to use a local store as cache.
	if cacheLocation != "" {
		cache, err := casync.NewLocalStore(cacheLocation)
		if err != nil {
			die(err)
		}
		s = casync.NewCache(s, cache)
	}

	// Read the input
	c, err := readCaibxFile(inFile)
	if err != nil {
		die(err)
	}

	// Prepare the output, TODO: Write to tempfile then rename
	// TODO: Set the filesize before writing to avoid fragmentation
	o, err := os.Create(outFile)
	if err != nil {
		die(err)
	}
	defer o.Close()

	// Build the blob from the chunks, TODO: Confirm the filesize matches the
	// expected size by looking at the offset of the last chunk
	errs := assembleBlob(o, c.Chunks, s, n)
	if len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
}

type ChunkProcessJob struct {
	id     casync.ChunkID
	result chan ChunkProcessResult
}

type ChunkProcessResult struct {
	err error
	b   []byte
}

// Takes a list of chunks and tries to reassemble the block asking the provided
// store for the chunks. It uses a bit of fancy concurrency to load N-chunks ahead
// while putting them back together in the right order. TODO: Should probably
// break this up a bit and move some useful bits out of main into the casync package.
func assembleBlob(w io.Writer, chunks []casync.BlobIndexChunk, s casync.Store, n int) []error {
	downloadQ := make(chan ChunkProcessJob)
	assembleQ := make(chan ChunkProcessJob, n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	// Start the downloaders. Each of them will use the done channel to tell
	// the assembler when complete. The results also contain any errors which
	// are taken care of by the assembler.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for j := range downloadQ {
				b, err := s.GetChunk(j.id)
				r := ChunkProcessResult{b: b, err: err}
				j.result <- r // Tell the assembler it's done
			}
			wg.Done()
		}()
	}

	// Start the feeder that feeds the download and assembler queues
	wg.Add(1)
	go func() {
		for _, c := range chunks {
			job := ChunkProcessJob{
				id:     c.ID,
				result: make(chan ChunkProcessResult),
			}
			assembleQ <- job
			downloadQ <- job

			// See if we're meant to stop
			select {
			case <-ctx.Done():
				break
			default:
			}
		}
		close(downloadQ)
		close(assembleQ)
		wg.Done()
	}()

	// Assemble the results. It goes through the jobs in the order they were
	// created and waits on the data channel in each job to ensure the order
	// is preserved
	var errs []error
	for j := range assembleQ {
		r := <-j.result
		if r.err != nil {
			errs = append(errs, r.err)
			cancel()
			continue
		}
		// The result contains the compressed chunk. Decompress it into the output
		// stream while at the same time calculate the SHA512/256 so we can compare it.
		h := sha512.New512_256()
		mw := io.MultiWriter(h, w)
		if _, err := casync.DecompressInto(mw, r.b); err != nil {
			errs = append(errs, err)
			cancel()
			continue
		}
		sum, err := casync.ChunkIDFromSlice(h.Sum(nil))
		if err != nil {
			errs = append(errs, err)
			cancel()
			continue
		}
		if sum != j.id {
			errs = append(errs, fmt.Errorf("unexpected sha256 %s for chunk id %s", sum, j.id))
			cancel()
			continue
		}
	}

	wg.Wait()

	return errs
}

func readCaibxFile(name string) (c casync.Caibx, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer f.Close()
	return casync.CaibxFromReader(f)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
