package main

import (
	"os"

	"github.com/folbricht/desync"
	"golang.org/x/crypto/ssh/terminal"
	pb "gopkg.in/cheggaaa/pb.v1"
)

// NewProgressBar initializes a wrapper for a https://github.com/cheggaaa/pb
// progressbar that implements desync.ProgressBar
func NewProgressBar(prefix string) desync.ProgressBar {
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		return nil
	}
	bar := pb.New(0).Prefix(prefix)
	bar.ShowCounters = false
	bar.Output = os.Stderr
	return ProgressBar{bar}
}

type ProgressBar struct {
	*pb.ProgressBar
}

func (p ProgressBar) SetTotal(total int) {
	p.ProgressBar.SetTotal(total)
}

func (p ProgressBar) Start() {
	p.ProgressBar.Start()
}

func (p ProgressBar) Set(current int) {
	p.ProgressBar.Set(current)
}
