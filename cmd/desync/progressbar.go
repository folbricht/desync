package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh/terminal"
)

type ProgressBar struct {
	mu      sync.Mutex
	done    chan (struct{})
	total   int
	counter int
	fd      int
}

func NewProgressBar(fd int, total int) *ProgressBar {
	return &ProgressBar{total: total, done: make(chan (struct{}))}
}

func (p *ProgressBar) Add(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter += n
	if p.counter > p.total {
		p.counter = p.total
	}
}

func (p *ProgressBar) Start() {
	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
	loop:
		for {
			select {
			case <-p.done:
				break loop
			case <-ticker.C:
				p.draw()
			}
		}
	}()
}

func (p *ProgressBar) Stop() {
	p.draw()
	close(p.done)
}

func (p *ProgressBar) draw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	width, _, err := terminal.GetSize(int(os.Stderr.Fd()))
	if err != nil || width <= 2 { // Is that a terminal and big enough?
		return
	}
	progress := (width - 2) * p.counter / p.total
	blank := width - 2 - progress
	if progress < 0 || blank < 0 { // No need to panic if anything's off
		return
	}
	fmt.Printf("\r|%s%s|", strings.Repeat("=", progress), strings.Repeat(" ", blank))
}
