package desync

import (
	"math"
	"sync"
	"time"
)

const (
	// adaptiveInitialLimit is the concurrency an adaptive limiter starts
	// with, the same as the default concurrency of the command line tool.
	adaptiveInitialLimit = 10

	// adaptiveMinWindow is the fewest completed requests the limiter bases a
	// change on. Chunks vary in size, and so does the latency of individual
	// requests.
	adaptiveMinWindow = 20

	// adaptiveRateWindows is the number of recent windows the highest
	// throughput is taken from.
	adaptiveRateWindows = 10

	// While ramping up, the limit keeps doubling until throughput grows by
	// less than adaptiveGrowth for adaptiveFlatWindows windows in a row.
	adaptiveGrowth      = 1.25
	adaptiveFlatWindows = 3

	// adaptiveHeadroom is how many times the requests the network and the
	// store can process at once are allowed to be in flight.
	adaptiveHeadroom = 2
)

// adaptiveLimiter limits the number of concurrent requests to a store, and
// adjusts that limit to the throughput and latency of the requests, the way
// TCP BBR sizes its congestion window.
//
// The number of requests the network and the store can process at once is
// the highest throughput times the lowest latency, like the bandwidth-delay
// product of a link. More requests than that only wait. To find the highest
// throughput, the limiter starts at adaptiveInitialLimit and doubles the
// limit after each window of requests, until throughput stops growing. From
// then on, the limit is adaptiveHeadroom times that product. The headroom
// lets throughput grow when the network or the store get faster, and the
// limit follows, without having to probe for it. A failed request halves the
// limit and discards what's been measured of the throughput.
type adaptiveLimiter struct {
	name string           // for logging
	now  func() time.Time // replaceable for tests

	mu       sync.Mutex
	cond     *sync.Cond
	limit    int
	max      int
	inFlight int
	lowest   time.Duration // lowest average latency of a window
	rates    []float64     // throughput of recent windows, in requests per second

	// While ramping up, the throughput the last time it grew and the number
	// of windows since
	rampUp    bool
	rampRate  float64
	flatCount int

	// The window of requests currently being recorded
	start     time.Time
	completed int
	succeeded int
	total     time.Duration // sum of the latencies of successful requests
	peak      int           // most requests in flight at the same time
	failed    bool
}

func newAdaptiveLimiter(name string, maxLimit int) *adaptiveLimiter {
	maxLimit = max(maxLimit, 1)
	l := &adaptiveLimiter{
		name:   name,
		now:    time.Now,
		limit:  min(adaptiveInitialLimit, maxLimit),
		max:    maxLimit,
		rampUp: true,
	}
	l.cond = sync.NewCond(&l.mu)
	l.start = l.now()
	return l
}

// acquire blocks until a request can be made within the current limit. Each
// call needs to be followed by release once the request completed.
func (l *adaptiveLimiter) acquire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.inFlight >= l.limit {
		l.cond.Wait()
	}
	l.inFlight++
	l.peak = max(l.peak, l.inFlight)
}

// release records a completed request, how long it took, and whether it
// failed.
func (l *adaptiveLimiter) release(latency time.Duration, failed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight--
	l.completed++
	if failed {
		// Back off once per window. A store that's overwhelmed tends to fail
		// many requests at once.
		if !l.failed {
			l.failed = true
			l.rampUp = false
			l.rates = nil
			l.setLimit(l.limit / 2)
		}
	} else {
		l.succeeded++
		l.total += latency
	}
	if l.completed >= max(l.limit, adaptiveMinWindow) {
		l.adjust()
		l.resetWindow()
	}
	l.cond.Signal()
}

// adjust sets a new limit based on the window of requests that just ended.
func (l *adaptiveLimiter) adjust() {
	elapsed := l.now().Sub(l.start)
	if l.failed || l.succeeded == 0 || elapsed <= 0 {
		return
	}
	avg := l.total / time.Duration(l.succeeded)
	if avg <= 0 {
		return
	}
	if l.lowest == 0 || avg < l.lowest {
		l.lowest = avg
	}

	// A window that didn't reach the limit shows what the requests needed,
	// not what the network and the store can do. Only use it if it was faster
	// all the same.
	rate := float64(l.succeeded) / elapsed.Seconds()
	limited := l.peak >= l.limit
	highest := l.highestRate()
	if limited || rate > highest {
		l.rates = append(l.rates, rate)
		if len(l.rates) > adaptiveRateWindows {
			l.rates = l.rates[1:]
		}
		highest = max(highest, rate)
	}

	if l.rampUp {
		if !limited {
			return
		}
		if rate >= l.rampRate*adaptiveGrowth {
			l.rampRate = rate
			l.flatCount = 0
		} else {
			l.flatCount++
		}
		if l.flatCount < adaptiveFlatWindows {
			l.setLimit(l.limit * 2)
			return
		}
		l.rampUp = false
	}
	l.setLimit(int(math.Ceil(adaptiveHeadroom * highest * l.lowest.Seconds())))
}

func (l *adaptiveLimiter) highestRate() float64 {
	var highest float64
	for _, r := range l.rates {
		highest = max(highest, r)
	}
	return highest
}

func (l *adaptiveLimiter) resetWindow() {
	l.start = l.now()
	l.completed = 0
	l.succeeded = 0
	l.total = 0
	l.peak = l.inFlight
	l.failed = false
}

func (l *adaptiveLimiter) setLimit(n int) {
	n = min(max(n, 1), l.max)
	if n == l.limit {
		return
	}
	if n > l.limit {
		l.cond.Broadcast()
	}
	l.limit = n
	Log.WithField("store", l.name).WithField("limit", n).Debug("adjusted concurrency")
}
