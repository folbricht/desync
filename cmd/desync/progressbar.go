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
	bar.Output = stderr
	return ProgressBar{bar}
}

// ProgressBar wraps https://github.com/cheggaaa/pb and implrments desync.ProgressBar
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

// ProgressReader implements ProgressBar and io.Reader
type ProgressReader struct {
	ProgressBar
	f *os.File
}

func NewProgressReader(prefix string, f *os.File) desync.ProgressBar {
	pb := NewProgressBar(prefix)
	if pb == nil {
		return nil
	}

	pb.f = f
	cur, err := f.Seek(0, SEEK_CUR)
	if err != nil {
		return nil
	}

	size, err := f.Seek(0, SEEK_END)
	f.Seek(cur, SEEK_SET)
	if err != nil {
		return nil
	}

	pb.SetTotal(size)
	pb.Set(cur)

	return pb
}

func (p ProgressReader) Read(b []byte) (n int, err error) {
	n, err = p.f.Read()
	if err != nil {
		return
	}

	cur, err := p.f.Seek(0, SEEK_CUR)
	if err != nil {
		return
	}

	p.Set(cur)
	return
}
