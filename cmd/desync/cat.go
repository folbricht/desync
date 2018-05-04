package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"io"

	"github.com/folbricht/desync"
)

const catUsage = `desync cat [options] <caibx> [<outputfile>]

Stream a caibx to stdout or a file-like object, optionally seeking and limiting
the read length.

Unlike extract, this supports output to FIFOs, named pipes, and other
non-seekable destinations.

This is inherently slower than extract as while multiple chunks can be
retrieved concurrently, writing to stdout cannot be parallelized.`

func cat(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		err            error
		storeLocations = new(multiArg)
		offset         int
		length         int
		readIndex      bool
		clientCert     string
		clientKey      string
	)
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, catUsage)
		flags.PrintDefaults()
	}

	flags.BoolVar(&readIndex, "i", false, "Read index file (caidx), not catar, in 2-argument mode")
	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.IntVar(&offset, "o", 0, "offset in bytes to seek to before reading")
	flags.IntVar(&length, "l", 0, "number of bytes to read")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	var outFile io.Writer
	if flags.NArg() == 2 {
		outFileName := flags.Arg(1)
		outFile, err = os.Create(outFileName)
		if err != nil {
			return err
		}
	} else {
		outFile = os.Stdout
	}

	inFile := flags.Arg(0)

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	// Parse the store locations, open the stores and add a cache is requested
	opts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}
	s, err := MultiStoreWithCache(opts, cacheLocation, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	c, err := readCaibxFile(inFile)
	if err != nil {
		return err
	}

	// Write the output
	readSeeker := desync.NewIndexReadSeeker(c, s)
	if _, err = readSeeker.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	if length > 0 {
		_, err = io.CopyN(outFile, readSeeker, int64(length))
	} else {
		_, err = io.Copy(outFile, readSeeker)
	}
	return err
}
