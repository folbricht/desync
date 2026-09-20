package desync

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingOpener returns an open function for a session pool that hands out
// sequentially numbered sessions and counts how many were opened.
func countingOpener() (func() (int, error), *atomic.Int64) {
	var opened atomic.Int64
	return func() (int, error) {
		return int(opened.Add(1)), nil
	}, &opened
}

func TestSessionPoolOpensOnDemand(t *testing.T) {
	open, opened := countingOpener()
	p := newSessionPool(4, open)

	// Sequential use only ever needs one session
	for range 10 {
		s, err := p.get()
		require.NoError(t, err)
		p.put(s)
	}
	assert.Equal(t, int64(1), opened.Load())
}

func TestSessionPoolBlocksAtLimit(t *testing.T) {
	open, opened := countingOpener()
	p := newSessionPool(3, open)

	var sessions []int
	for range 3 {
		s, err := p.get()
		require.NoError(t, err)
		sessions = append(sessions, s)
	}
	assert.Equal(t, int64(3), opened.Load())

	// With all sessions in use, the next caller waits for one to be returned
	// rather than opening another
	got := make(chan int)
	go func() {
		s, err := p.get()
		assert.NoError(t, err)
		got <- s
	}()
	select {
	case <-got:
		require.FailNow(t, "got a session while all were in use")
	case <-time.After(50 * time.Millisecond):
	}

	p.put(sessions[0])
	select {
	case s := <-got:
		assert.Equal(t, sessions[0], s)
	case <-time.After(time.Minute):
		require.FailNow(t, "returned session wasn't handed out")
	}
	assert.Equal(t, int64(3), opened.Load())
}

func TestSessionPoolOpenFailureFreesSlot(t *testing.T) {
	fail := true
	p := newSessionPool(1, func() (int, error) {
		if fail {
			fail = false
			return 0, errors.New("failed to open")
		}
		return 1, nil
	})

	_, err := p.get()
	require.Error(t, err)

	// The failed attempt must not hold on to the only slot, this would block
	// forever otherwise
	s, err := p.get()
	require.NoError(t, err)
	assert.Equal(t, 1, s)
}

func TestSessionPoolConcurrentUse(t *testing.T) {
	const n = 4
	open, opened := countingOpener()
	p := newSessionPool(n, open)

	var (
		inUse, peak atomic.Int64
		wg          sync.WaitGroup
	)
	for range 50 {
		wg.Go(func() {
			for range 20 {
				s, err := p.get()
				if !assert.NoError(t, err) {
					return
				}
				cur := inUse.Add(1)
				for {
					old := peak.Load()
					if cur <= old || peak.CompareAndSwap(old, cur) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				inUse.Add(-1)
				p.put(s)
			}
		})
	}
	wg.Wait()

	assert.LessOrEqual(t, opened.Load(), int64(n))
	assert.LessOrEqual(t, peak.Load(), int64(n))
}

func TestSessionPoolCloseWaitsForSessions(t *testing.T) {
	open, _ := countingOpener()
	p := newSessionPool(2, open)

	s1, err := p.get()
	require.NoError(t, err)
	s2, err := p.get()
	require.NoError(t, err)
	p.put(s1)

	var closed []int
	done := make(chan error)
	go func() {
		done <- p.close(func(s int) error {
			closed = append(closed, s)
			return errors.New("close failed")
		})
	}()

	// One session is still in use, close has to wait for it
	select {
	case <-done:
		require.FailNow(t, "close returned while a session was in use")
	case <-time.After(50 * time.Millisecond):
	}

	p.put(s2)
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "close didn't return after all sessions were returned")
	}
	assert.ElementsMatch(t, []int{s1, s2}, closed)
}
