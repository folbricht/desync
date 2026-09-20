package desync

import (
	"errors"
	"sync"
)

// sessionPool hands out sessions to a remote store, such as SSH processes. It
// opens sessions only when all existing ones are in use, up to a maximum, and
// blocks callers while the maximum is in use.
type sessionPool[T any] struct {
	open  func() (T, error)
	slots chan struct{} // holds one token per caller using a session
	idle  chan T        // open sessions not in use
	mu    sync.Mutex    // serializes opening sessions
}

func newSessionPool[T any](n int, open func() (T, error)) *sessionPool[T] {
	n = max(n, 1)
	return &sessionPool[T]{
		open:  open,
		slots: make(chan struct{}, n),
		idle:  make(chan T, n),
	}
}

// get returns an idle session, or opens a new one if none is idle. Sessions
// need to be returned with put.
func (p *sessionPool[T]) get() (T, error) {
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

// put returns a session to the pool.
func (p *sessionPool[T]) put(s T) {
	p.idle <- s
	<-p.slots
}

// close waits for all sessions to be returned and closes them with fn. The
// pool can't be used afterwards.
func (p *sessionPool[T]) close(fn func(T) error) error {
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
