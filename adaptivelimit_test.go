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
		gens := make([]uint64, n)
		for i := range n {
			gens[i] = l.acquire()
		}
		*clock = clock.Add(latency(n))
		for _, gen := range gens {
			l.release(gen, latency(n), false)
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
	l.release(l.acquire(), time.Millisecond, true)
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

func TestAdaptiveLimiterMeasuresLatencyAgain(t *testing.T) {
	l, clock := newTestLimiter(128)
	simulate(l, clock, 20, func(int) time.Duration { return 50 * time.Millisecond })
	require.Equal(t, 128, l.currentLimit())

	// Complete a slower window after the lowest latency expired, with half
	// the requests of the next one in flight
	*clock = clock.Add(adaptiveLatencyExpiry)
	var gens []uint64
	for range 128 {
		gens = append(gens, l.acquire())
	}
	for _, gen := range gens[:64] {
		l.release(gen, 60*time.Millisecond, false)
	}
	gens = gens[64:]
	for range 64 {
		gens = append(gens, l.acquire())
	}
	for _, gen := range gens[:64] {
		l.release(gen, 60*time.Millisecond, false)
	}
	gens = gens[64:]
	require.Equal(t, adaptiveInitialLimit, l.currentLimit())

	// One of the requests made before is slow to complete, the others
	// complete as the new ones are made
	slow := gens[0]
	for _, gen := range gens[1:] {
		l.release(gen, time.Second, false)
	}

	// The slow request doesn't hold up those at the lower limit, and none
	// of the requests made before count towards their latency
	for range 3 {
		var measured []uint64
		for range adaptiveInitialLimit - 1 {
			measured = append(measured, l.acquire())
		}
		*clock = clock.Add(55 * time.Millisecond)
		for _, gen := range measured {
			l.release(gen, 55*time.Millisecond, false)
		}
	}
	l.release(slow, 5*time.Second, false)
	assert.Equal(t, 55*time.Millisecond, l.lowest)
	assert.Greater(t, l.currentLimit(), adaptiveInitialLimit)
}

func TestAdaptiveLimiterBacksOffOnFailure(t *testing.T) {
	l := newAdaptiveLimiter("test", 128)
	var gens []uint64
	for range 10 {
		gens = append(gens, l.acquire())
	}
	// Many failures in the same window halve the limit only once
	for _, gen := range gens[:5] {
		l.release(gen, time.Millisecond, true)
	}
	for _, gen := range gens[5:] {
		l.release(gen, time.Millisecond, false)
	}
	assert.Equal(t, adaptiveInitialLimit/2, l.currentLimit())
}

func TestAdaptiveLimiterIgnoresWindowsBelowLimit(t *testing.T) {
	l := newAdaptiveLimiter("test", 128)

	// Never more than 5 requests in flight at once, whatever the limit. The
	// store isn't the bottleneck, so there's no reason to change the limit.
	for range 100 {
		var gens []uint64
		for range 5 {
			gens = append(gens, l.acquire())
		}
		for _, gen := range gens {
			l.release(gen, 50*time.Millisecond, false)
		}
	}
	assert.Equal(t, adaptiveInitialLimit, l.currentLimit())
}

func TestAdaptiveLimiterBlocksAtLimit(t *testing.T) {
	l := newAdaptiveLimiter("test", 128)
	var gen uint64
	for range adaptiveInitialLimit {
		gen = l.acquire()
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

	l.release(gen, time.Millisecond, false)
	select {
	case <-acquired:
	case <-time.After(time.Minute):
		require.FailNow(t, "not acquired after a release")
	}
}
