package throttle

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Limiter provides token-bucket rate limiting, worker concurrency gating,
// and moving-average throughput telemetry for extraction pipelines.
type Limiter struct {
	mu          sync.RWMutex
	rateLimiter *rate.Limiter
	limit       int
	concurrency int
	workerSem   chan struct{}
	activeCount int32

	// Moving-average throughput tracking
	historyMu   sync.Mutex
	windowStart time.Time
	windowRows  int64
	currentRate int64
}

// New creates a new Limiter.
// rateLimit is maximum rows per second (0 = unlimited).
// concurrency is maximum parallel workers (default: 4 if <= 0).
func New(rateLimit int, concurrency int) *Limiter {
	if concurrency <= 0 {
		concurrency = 4
	}

	var rl *rate.Limiter
	if rateLimit > 0 {
		burst := rateLimit
		if burst < 1000 {
			burst = 1000
		}
		rl = rate.NewLimiter(rate.Limit(rateLimit), burst)
	}

	return &Limiter{
		rateLimiter: rl,
		limit:       rateLimit,
		concurrency: concurrency,
		workerSem:   make(chan struct{}, concurrency),
		windowStart: time.Now(),
	}
}

// Acquire blocks until n row tokens are available according to the token bucket.
// If rate limiting is disabled (limit <= 0), it returns immediately.
func (l *Limiter) Acquire(ctx context.Context, n int) error {
	if l == nil || n <= 0 {
		return nil
	}

	l.mu.RLock()
	rl := l.rateLimiter
	l.mu.RUnlock()

	if rl == nil {
		return nil
	}

	burst := rl.Burst()
	if burst <= 0 {
		burst = 1000
	}

	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := rl.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

// AcquireWorker acquires a worker slot from the concurrency semaphore.
func (l *Limiter) AcquireWorker(ctx context.Context) error {
	if l == nil || l.workerSem == nil {
		return nil
	}
	select {
	case l.workerSem <- struct{}{}:
		atomic.AddInt32(&l.activeCount, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseWorker releases a worker slot back to the concurrency semaphore.
func (l *Limiter) ReleaseWorker() {
	if l == nil || l.workerSem == nil {
		return
	}
	select {
	case <-l.workerSem:
		atomic.AddInt32(&l.activeCount, -1)
	default:
	}
}

// ActiveWorkers returns the count of currently active worker goroutines.
func (l *Limiter) ActiveWorkers() int {
	if l == nil {
		return 0
	}
	c := atomic.LoadInt32(&l.activeCount)
	if c < 0 {
		return 0
	}
	return int(c)
}

// Concurrency returns the configured maximum parallel extraction workers.
func (l *Limiter) Concurrency() int {
	if l == nil {
		return 0
	}
	return l.concurrency
}

// RecordRows updates the moving throughput measurement with n processed rows.
func (l *Limiter) RecordRows(n int) {
	if l == nil || n <= 0 {
		return
	}
	l.historyMu.Lock()
	defer l.historyMu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.windowStart)
	if elapsed >= 1*time.Second {
		rateVal := float64(l.windowRows) / elapsed.Seconds()
		atomic.StoreInt64(&l.currentRate, int64(rateVal))
		l.windowStart = now
		l.windowRows = int64(n)
	} else {
		l.windowRows += int64(n)
	}
}

// CurrentRate returns the observed extraction throughput in rows per second.
func (l *Limiter) CurrentRate() int64 {
	if l == nil {
		return 0
	}
	l.historyMu.Lock()
	now := time.Now()
	elapsed := now.Sub(l.windowStart)
	if elapsed >= 2*time.Second && l.windowRows == 0 {
		atomic.StoreInt64(&l.currentRate, 0)
	} else if elapsed >= 1*time.Second {
		rateVal := float64(l.windowRows) / elapsed.Seconds()
		atomic.StoreInt64(&l.currentRate, int64(rateVal))
	}
	r := atomic.LoadInt64(&l.currentRate)
	l.historyMu.Unlock()
	return r
}

// SetLimit dynamically updates the token-bucket rate limit (e.g. during adaptive backoff).
func (l *Limiter) SetLimit(newLimit int) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.limit = newLimit
	if newLimit <= 0 {
		l.rateLimiter = nil
		return
	}

	burst := newLimit
	if burst < 1000 {
		burst = 1000
	}

	if l.rateLimiter == nil {
		l.rateLimiter = rate.NewLimiter(rate.Limit(newLimit), burst)
	} else {
		l.rateLimiter.SetLimit(rate.Limit(newLimit))
		l.rateLimiter.SetBurst(burst)
	}
}

// RateLimit returns the configured rate limit.
func (l *Limiter) RateLimit() int {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.limit
}
