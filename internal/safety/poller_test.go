package safety

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/x7ssss/dbdrain/internal/db"
)

func TestEvaluateStateMySQL(t *testing.T) {
	thresholds := DefaultThresholds(30 * time.Second)

	// 1. Initial Healthy -> StateNormal
	m := Metrics{
		Engine:            db.EngineMySQL,
		HistoryListLength: 5000,
	}
	state, _ := EvaluateState(m, thresholds, StateNormal)
	if state != StateNormal {
		t.Fatalf("expected StateNormal, got %s", state)
	}

	// 2. HLL > 100,000 -> StateBackoff
	m.HistoryListLength = 150000
	state, reason := EvaluateState(m, thresholds, StateNormal)
	if state != StateBackoff {
		t.Fatalf("expected StateBackoff, got %s (reason: %s)", state, reason)
	}

	// 3. HLL > 1,000,000 -> StatePaused
	m.HistoryListLength = 1200000
	state, reason = EvaluateState(m, thresholds, StateBackoff)
	if state != StatePaused {
		t.Fatalf("expected StatePaused, got %s (reason: %s)", state, reason)
	}

	// 4. Cluster recovers (HLL drops below 100k) -> StateResumed
	m.HistoryListLength = 40000
	state, _ = EvaluateState(m, thresholds, StatePaused)
	if state != StateResumed {
		t.Fatalf("expected StateResumed after recovery, got %s", state)
	}

	// 5. Subsequent poll while healthy -> StateNormal
	state, _ = EvaluateState(m, thresholds, StateResumed)
	if state != StateNormal {
		t.Fatalf("expected StateNormal, got %s", state)
	}

	// 6. Replication lag spike (> 30s) -> StatePaused
	lag := int64(45)
	m.HistoryListLength = 5000
	m.ReplicationLagSec = &lag
	state, reason = EvaluateState(m, thresholds, StateNormal)
	if state != StatePaused {
		t.Fatalf("expected StatePaused on replication lag, got %s (reason: %s)", state, reason)
	}
}

func TestEvaluateStatePostgres(t *testing.T) {
	thresholds := DefaultThresholds(30 * time.Second)

	// 1. Healthy -> StateNormal
	m := Metrics{
		Engine:         db.EnginePostgres,
		ActiveSessions: 5,
		IOWaiters:      2,
		LockWaiters:    0,
		OldestTxAgeSec: 5.0,
	}
	state, _ := EvaluateState(m, thresholds, StateNormal)
	if state != StateNormal {
		t.Fatalf("expected StateNormal, got %s", state)
	}

	// 2. IO waiters >= 10 -> StateBackoff
	m.IOWaiters = 12
	state, reason := EvaluateState(m, thresholds, StateNormal)
	if state != StateBackoff {
		t.Fatalf("expected StateBackoff on IO waiters, got %s (reason: %s)", state, reason)
	}

	// 3. Lock waiters spike (>= 5) -> StatePaused
	m.IOWaiters = 2
	m.LockWaiters = 6
	state, reason = EvaluateState(m, thresholds, StateBackoff)
	if state != StatePaused {
		t.Fatalf("expected StatePaused on lock waiters, got %s (reason: %s)", state, reason)
	}

	// 4. Lock waiters clear -> StateResumed
	m.LockWaiters = 0
	state, _ = EvaluateState(m, thresholds, StatePaused)
	if state != StateResumed {
		t.Fatalf("expected StateResumed, got %s", state)
	}

	// 5. Normal again
	state, _ = EvaluateState(m, thresholds, StateResumed)
	if state != StateNormal {
		t.Fatalf("expected StateNormal, got %s", state)
	}

	// 6. Oldest transaction exceeds max lag (> 30s) -> StatePaused
	m.OldestTxAgeSec = 35.0
	state, reason = EvaluateState(m, thresholds, StateNormal)
	if state != StatePaused {
		t.Fatalf("expected StatePaused on tx age, got %s (reason: %s)", state, reason)
	}
}

func TestWaitIfPaused(t *testing.T) {
	poller := NewHealthPoller("postgres://localhost", db.EnginePostgres, DefaultThresholds(30*time.Second))

	// When StateNormal, WaitIfPaused returns immediately
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := poller.WaitIfPaused(ctx); err != nil {
		t.Fatalf("expected nil error when normal, got: %v", err)
	}

	// Transition to StatePaused
	poller.SetStateForTesting(StatePaused, "test pause")

	resumed := make(chan bool)
	go func() {
		err := poller.WaitIfPaused(context.Background())
		resumed <- (err == nil)
	}()

	// Verify it is blocking
	select {
	case <-resumed:
		t.Fatalf("WaitIfPaused should block while StatePaused")
	case <-time.After(50 * time.Millisecond):
	}

	// Transition back to StateResumed
	poller.SetStateForTesting(StateResumed, "")

	select {
	case ok := <-resumed:
		if !ok {
			t.Fatalf("expected clean resume")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("WaitIfPaused did not wake up on resume")
	}
}

func TestIsLockTimeout(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}
	if !IsLockTimeout(pgErr) {
		t.Fatalf("expected IsLockTimeout to be true for pgconn 55P03")
	}

	wrappedErr := fmt.Errorf("query failed: %w", pgErr)
	if !IsLockTimeout(wrappedErr) {
		t.Fatalf("expected IsLockTimeout to be true for wrapped error")
	}

	stringErr := errors.New("pq: canceling statement due to lock timeout")
	if !IsLockTimeout(stringErr) {
		t.Fatalf("expected IsLockTimeout to be true for error string")
	}

	normalErr := errors.New("connection reset by peer")
	if IsLockTimeout(normalErr) {
		t.Fatalf("expected IsLockTimeout to be false for normal error")
	}
}

func TestExecuteWithBackoffJitter(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	lockErr := &pgconn.PgError{Code: "55P03", Message: "lock timeout"}

	// Test retry succeeds on 3rd attempt
	err := ExecuteWithBackoffJitter(ctx, 3, 10*time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return lockErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success on 3rd attempt, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}

	// Test retry exhausts and fails fast with alert
	attempts = 0
	err = ExecuteWithBackoffJitter(ctx, 2, 10*time.Millisecond, func() error {
		attempts++
		return lockErr
	})
	if err == nil {
		t.Fatalf("expected fail-fast error, got nil")
	}
	if attempts != 3 { // initial + 2 retries = 3
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}
