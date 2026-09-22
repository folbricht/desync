package desync

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (l *adaptiveLimiter) currentLimit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// newTestLimiter returns a limiter that takes the time from the returned
// clock rather than the system.
func newTestLimiter(max int) (*adaptiveLimiter, *time.Time) {
	clock := time.Unix(0, 0)
	l := newAdaptiveLimiter("test", max)
	l.now = func() time.Time { return clock }
	l.start = clock
	return l, &clock
}

// link models the requests to a store over a link with the given round trip
// time, where each response occupies the link for transfer. Responses in
// flight at the same time share the link, and those that don't fit into a
// round trip wait for the ones before them.
type link struct {
	rtt, transfer time.Duration
}

// latency returns the average latency of n concurrent requests.
func (k link) latency(n int) time.Duration {
	return max(k.rtt+time.Duration(n+1)*k.transfer/2, time.Duration(n)*k.transfer)
}

// throughput returns the requests per second n concurrent requests achieve.
func (k link) throughput(n int) float64 {
	return float64(n) / k.latency(n).Seconds()
}

// capacity returns the most requests per second the link can carry.
func (k link) capacity() float64 {
	return 1 / k.transfer.Seconds()
}

// best returns the highest throughput of up to max concurrent requests.
func (k link) best(max int) float64 {
	var best float64
	for n := 1; n <= max; n++ {
		best = math.Max(best, k.throughput(n))
	}
	return best
}

// simulate runs rounds of requests through the limiter. Each round makes as
// many concurrent requests as the limit allows, all of them taking the time
// the latency model gives for that many. It returns the limit after each
// round.
func simulate(l *adaptiveLimiter, clock *time.Time, rounds int, latency func(n int) time.Duration) []int {
	var limits []int
	for range rounds {
		n := l.currentLimit()
		for range n {
			l.acquire()
		}
		*clock = clock.Add(latency(n))
		for range n {
			l.release(latency(n), false)
		}
		limits = append(limits, l.currentLimit())
	}
	return limits
}

func TestAdaptiveLimiterFillsLink(t *testing.T) {
	// 64KiB chunks over 1 Gbit/s links with different round trip times, and
	// over a 50 Mbit/s link where the initial limit already fills it
	for _, test := range []struct {
		name string
		link link
	}{
		{"1 Gbit/s, 20ms", link{20 * time.Millisecond, 524 * time.Microsecond}},
		{"1 Gbit/s, 50ms", link{50 * time.Millisecond, 524 * time.Microsecond}},
		{"50 Mbit/s, 50ms", link{50 * time.Millisecond, 10500 * time.Microsecond}},
	} {
		t.Run(test.name, func(t *testing.T) {
			l, clock := newTestLimiter(128)
			limits := simulate(l, clock, 200, test.link.latency)
			final := limits[len(limits)-1]
			assert.GreaterOrEqual(t, test.link.throughput(final), 0.95*test.link.best(128), "limits: %v", limits[:20])

			// Not more than twice what the link carries at the lowest latency
			// seen, the one at the initial limit
			bound := math.Ceil(2 * test.link.capacity() * test.link.latency(adaptiveInitialLimit).Seconds())
			assert.LessOrEqual(t, float64(final), bound, "limits: %v", limits[:20])
		})
	}
}

func TestAdaptiveLimiterGrowsToMax(t *testing.T) {
	// A store and network that never slow down
	l, clock := newTestLimiter(128)
	limits := simulate(l, clock, 20, func(int) time.Duration { return 50 * time.Millisecond })
	assert.Equal(t, 128, limits[len(limits)-1])
}

func TestAdaptiveLimiterRecoversFromFailure(t *testing.T) {
	k := link{20 * time.Millisecond, 524 * time.Microsecond}
	l, clock := newTestLimiter(128)
	before := simulate(l, clock, 50, k.latency)

	// Fail a request, then continue as before
	l.acquire()
	l.release(time.Millisecond, true)
	assert.Equal(t, before[len(before)-1]/2, l.currentLimit())

	after := simulate(l, clock, 50, k.latency)
	assert.GreaterOrEqual(t, k.throughput(after[len(after)-1]), 0.95*k.best(128), "limits: %v", after)
}

func TestAdaptiveLimiterFollowsSlowerStore(t *testing.T) {
	// A store that gets slower, but can still handle as many requests at
	// once as before
	l, clock := newTestLimiter(128)
	before := simulate(l, clock, 30, func(int) time.Duration { return 5 * time.Millisecond })
	require.Equal(t, 128, before[len(before)-1])

	after := simulate(l, clock, 50, func(int) time.Duration { return 60 * time.Millisecond })
	assert.Equal(t, 128, after[len(after)-1], "limits: %v", after)
}

func TestAdaptiveLimiterFollowsLongerRoundTrip(t *testing.T) {
	// The round trip time of a link grows, but not by enough to take the
	// limit below adaptiveInitialLimit. It takes a new measurement of the
	// latency to raise it again.
	l, clock := newTestLimiter(128)
	simulate(l, clock, 50, link{20 * time.Millisecond, 524 * time.Microsecond}.latency)

	k := link{80 * time.Millisecond, 524 * time.Microsecond}
	after := simulate(l, clock, 200, k.latency)
	final := after[len(after)-1]
	assert.GreaterOrEqual(t, k.throughput(final), 0.95*k.best(128), "limits: %v", after)
}

func TestAdaptiveLimiterDrainsToMeasureLatency(t *testing.T) {
	l, clock := newTestLimiter(128)
	simulate(l, clock, 20, func(int) time.Duration { return 50 * time.Millisecond })
	require.Equal(t, 128, l.currentLimit())

	// Complete a slower window after the lowest latency expired, with half
	// the requests of the next one in flight
	*clock = clock.Add(adaptiveLatencyExpiry)
	for range 128 {
		l.acquire()
	}
	for range 64 {
		l.release(60*time.Millisecond, false)
	}
	for range 64 {
		l.acquire()
	}
	for range 64 {
		l.release(60*time.Millisecond, false)
	}
	require.Equal(t, adaptiveInitialLimit, l.currentLimit())

	// Nothing more is requested until the last of them completed, even
	// with fewer than the limit in flight
	for range 63 {
		l.release(time.Second, false)
	}
	acquired := make(chan struct{})
	go func() {
		l.acquire()
		close(acquired)
	}()
	select {
	case <-acquired:
		require.FailNow(t, "acquired while draining")
	case <-time.After(50 * time.Millisecond):
	}
	l.release(time.Second, false)
	select {
	case <-acquired:
	case <-time.After(time.Minute):
		require.FailNow(t, "not acquired after draining")
	}
	l.release(50*time.Millisecond, false)

	// The window at the lower limit measures the latency again, and the
	// limit goes back up
	simulate(l, clock, 2, func(int) time.Duration { return 50 * time.Millisecond })
	assert.Equal(t, 50*time.Millisecond, l.lowest)
	assert.Greater(t, l.currentLimit(), adaptiveInitialLimit)
}

func TestAdaptiveLimiterBacksOffOnFailure(t *testing.T) {
	l := newAdaptiveLimiter("test", 128)
	for range 10 {
		l.acquire()
	}
	// Many failures in the same window halve the limit only once
	for range 5 {
		l.release(time.Millisecond, true)
	}
	for range 5 {
		l.release(time.Millisecond, false)
	}
	assert.Equal(t, adaptiveInitialLimit/2, l.currentLimit())
}

func TestAdaptiveLimiterIgnoresWindowsBelowLimit(t *testing.T) {
	l := newAdaptiveLimiter("test", 128)

	// Never more than 5 requests in flight at once, whatever the limit. The
	// store isn't the bottleneck, so there's no reason to change the limit.
	for range 100 {
		for range 5 {
			l.acquire()
		}
		for range 5 {
			l.release(50*time.Millisecond, false)
		}
	}
	assert.Equal(t, adaptiveInitialLimit, l.currentLimit())
}

func TestAdaptiveLimiterBlocksAtLimit(t *testing.T) {
	l := newAdaptiveLimiter("test", 128)
	for range adaptiveInitialLimit {
		l.acquire()
	}

	acquired := make(chan struct{})
	go func() {
		l.acquire()
		close(acquired)
	}()
	select {
	case <-acquired:
		require.FailNow(t, "acquired beyond the limit")
	case <-time.After(50 * time.Millisecond):
	}

	l.release(time.Millisecond, false)
	select {
	case <-acquired:
	case <-time.After(time.Minute):
		require.FailNow(t, "not acquired after a release")
	}
}
