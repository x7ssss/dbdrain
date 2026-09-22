package throttle

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLimiterUnlimited(t *testing.T) {
	ctx := context.Background()
	lim := New(0, 4)

	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := lim.Acquire(ctx, 1000); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("unlimited limiter took too long: %v", elapsed)
	}
}

func TestLimiterThroughputBounding(t *testing.T) {
	ctx := context.Background()
	// 200 tokens per second
	rateLimit := 200
	lim := New(rateLimit, 4)

	// Consume initial burst
	if err := lim.Acquire(ctx, 1000); err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	// Now request 200 more tokens. At 200 tokens/sec, this should take ~1 second.
	start := time.Now()
	if err := lim.Acquire(ctx, 200); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	elapsed := time.Since(start)

	// Allow reasonable tolerance: 200 tokens at 200/sec should take at least 800ms
	if elapsed < 800*time.Millisecond {
		t.Fatalf("throughput bounding not enforced: elapsed %v for 200 tokens at 200/s", elapsed)
	}
}

func TestLimiterWorkerConcurrency(t *testing.T) {
	ctx := context.Background()
	concurrency := 3
	lim := New(0, concurrency)

	if lim.Concurrency() != concurrency {
		t.Fatalf("expected concurrency %d, got %d", concurrency, lim.Concurrency())
	}

	// Acquire up to concurrency
	for i := 0; i < concurrency; i++ {
		if err := lim.AcquireWorker(ctx); err != nil {
			t.Fatalf("failed to acquire worker %d: %v", i, err)
		}
	}
	if lim.ActiveWorkers() != concurrency {
		t.Fatalf("expected active workers %d, got %d", concurrency, lim.ActiveWorkers())
	}

	// Attempting another acquire should block until context timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := lim.AcquireWorker(timeoutCtx)
	if err == nil {
		t.Fatalf("expected timeout error on exceeding concurrency, got nil")
	}

	// Release one worker, now acquire should succeed
	lim.ReleaseWorker()
	if lim.ActiveWorkers() != concurrency-1 {
		t.Fatalf("expected active workers %d, got %d", concurrency-1, lim.ActiveWorkers())
	}

	shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer shortCancel()
	if err := lim.AcquireWorker(shortCtx); err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}

	// Clean up all
	for i := 0; i < concurrency; i++ {
		lim.ReleaseWorker()
	}
	if lim.ActiveWorkers() != 0 {
		t.Fatalf("expected 0 active workers, got %d", lim.ActiveWorkers())
	}
}

func TestLimiterDynamicSetLimit(t *testing.T) {
	lim := New(500, 2)
	if lim.RateLimit() != 500 {
		t.Fatalf("expected rate limit 500, got %d", lim.RateLimit())
	}

	lim.SetLimit(1000)
	if lim.RateLimit() != 1000 {
		t.Fatalf("expected rate limit 1000, got %d", lim.RateLimit())
	}

	lim.SetLimit(0)
	if lim.RateLimit() != 0 {
		t.Fatalf("expected rate limit 0, got %d", lim.RateLimit())
	}
}

func TestLimiterThroughputRecording(t *testing.T) {
	lim := New(0, 4)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				lim.RecordRows(10)
			}
		}()
	}
	wg.Wait()

	_ = lim.CurrentRate()
}
