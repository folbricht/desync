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
	if !terminal.IsTerminal(int(os.Stderr.Fd())) && os.Getenv("DESYNC_PROGRESSBAR_ENABLED") == "" {
		return nil
	}
	bar := pb.New(0).Prefix(prefix)
	bar.ShowCounters = false
	bar.Output = stderr
	return ProgressBar{bar}
}

// ProgressBar wraps https://github.com/cheggaaa/pb and implements desync.ProgressBar
type ProgressBar struct {
	*pb.ProgressBar
}

// SetTotal sets the upper bounds for the progress bar
func (p ProgressBar) SetTotal(total int) {
	p.ProgressBar.SetTotal(total)
}

// Start displaying the progress bar
func (p ProgressBar) Start() {
	p.ProgressBar.Start()
}

// Set the current value
func (p ProgressBar) Set(current int) {
	p.ProgressBar.Set(current)
}

// Write the current state of the progressbar
func (p ProgressBar) Write(b []byte) (n int, err error) {
	return p.ProgressBar.Write(b)
}
