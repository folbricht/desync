package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/folbricht/desync"
	"golang.org/x/crypto/ssh/terminal"
)

type ConsoleProgressBar struct {
	prefix  string
	mu      sync.Mutex
	done    chan (struct{})
	total   int
	counter int
	fd      int
}

func NewProgressBar(total int, prefix string) desync.ProgressBar {
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		return desync.ProgressBar(nil)
	}
	return &ConsoleProgressBar{prefix: prefix, total: total, done: make(chan (struct{}))}
}

func (p *ConsoleProgressBar) Add(n int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter += n
	if p.counter > p.total {
		p.counter = p.total
	}
}

func (p *ConsoleProgressBar) Set(n int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter = n
	if p.counter > p.total {
		p.counter = p.total
	}
}

func (p *ConsoleProgressBar) Start() {
	if p == nil {
		return
	}
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

func (p *ConsoleProgressBar) Stop() {
	if p == nil {
		return
	}
	p.draw()
	close(p.done)
}

func (p *ConsoleProgressBar) draw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	width, _, err := terminal.GetSize(int(os.Stderr.Fd()))
	if err != nil || width <= len(p.prefix)+2 { // Is that a terminal and big enough?
		return
	}
	progress := (width - len(p.prefix) - 2) * p.counter / p.total
	blank := width - len(p.prefix) - 2 - progress
	if progress < 0 || blank < 0 { // No need to panic if anything's off
		return
	}
	fmt.Printf("\r%s|%s%s|", p.prefix, strings.Repeat("=", progress), strings.Repeat(" ", blank))
}
