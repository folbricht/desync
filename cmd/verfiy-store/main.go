package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	casync "github.com/folbricht/go-casync"
)

const usage = `verify-store [options] <store>`

func main() {
	var (
		repair bool
		n      int
		err    error
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.IntVar(&n, "n", 10, "number of goroutines")
	flag.BoolVar(&repair, "r", false, "Remove any invalid chunks")
	flag.Parse()

	if flag.NArg() < 1 {
		die(errors.New("Not enough arguments. See -h for help."))
	}
	if flag.NArg() > 1 {
		die(errors.New("Too many arguments. See -h for help."))
	}
	s, err := casync.NewLocalStore(flag.Arg(0))
	if err != nil {
		die(err)
	}
	if err := s.Verify(n, repair); err != nil {
		die(err)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
