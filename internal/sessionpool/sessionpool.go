// Package sessionpool provides a pool of sessions to remote stores.
package sessionpool

import (
	"errors"
	"sync"
)

// Pool hands out sessions to a remote store, such as SSH processes. It
// opens sessions only when all existing ones are in use, up to a maximum, and
// blocks callers while the maximum is in use.
type Pool[T any] struct {
	open  func() (T, error)
	slots chan struct{} // holds one token per caller using a session
	idle  chan T        // open sessions not in use
	mu    sync.Mutex    // serializes opening sessions
}

func New[T any](n int, open func() (T, error)) *Pool[T] {
	n = max(n, 1)
	return &Pool[T]{
		open:  open,
		slots: make(chan struct{}, n),
		idle:  make(chan T, n),
	}
}

// Get returns an idle session, or opens a new one if none is idle. Sessions
// need to be returned with Put.
func (p *Pool[T]) Get() (T, error) {
	p.slots <- struct{}{}
	select {
	case s := <-p.idle:
		return s, nil
	default:
	}

	// Open one session at a time. SSH may fail if multiple instances access
	// the SSH agent concurrently.
	p.mu.Lock()
	defer p.mu.Unlock()

	// A session may have been returned while waiting for the lock
	select {
	case s := <-p.idle:
		return s, nil
	default:
	}
	s, err := p.open()
	if err != nil {
		<-p.slots
		return s, err
	}
	return s, nil
}

// Put returns a session to the pool.
func (p *Pool[T]) Put(s T) {
	p.idle <- s
	<-p.slots
}

// Idle returns the number of open sessions not in use.
func (p *Pool[T]) Idle() int {
	return len(p.idle)
}

// Close waits for all sessions to be returned and closes them with fn. The
// pool can't be used afterwards.
func (p *Pool[T]) Close(fn func(T) error) error {
	for range cap(p.slots) {
		p.slots <- struct{}{}
	}
	var errs []error
	for {
		select {
		case s := <-p.idle:
			errs = append(errs, fn(s))
		default:
			return errors.Join(errs...)
		}
	}
}
