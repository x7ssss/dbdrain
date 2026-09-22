package safety

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	PostgresStatementTimeout       = "30s"
	PostgresLockTimeout            = "100ms"
	PostgresIdleInTxSessionTimeout = "60s"
	PostgresApplicationName        = "dbdrain-worker"
)

// PostgresGuardsSQL returns the session guard statements to execute on PostgreSQL extraction transactions.
func PostgresGuardsSQL() []string {
	return []string{
		"SET LOCAL statement_timeout = '30s'",
		"SET LOCAL lock_timeout = '100ms'",
		"SET LOCAL idle_in_transaction_session_timeout = '60s'",
		"SET LOCAL application_name = 'dbdrain-worker'",
	}
}

// ApplyPostgresGuards applies fail-fast session safety guards to a PostgreSQL transaction.
func ApplyPostgresGuards(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return fmt.Errorf("postgres transaction is nil")
	}

	for _, g := range PostgresGuardsSQL() {
		if _, err := tx.Exec(ctx, g); err != nil {
			// Some older PostgreSQL versions or cloud proxies might not support idle_in_transaction_session_timeout
			if strings.Contains(strings.ToLower(err.Error()), "unrecognized configuration parameter") &&
				strings.Contains(g, "idle_in_transaction_session_timeout") {
				continue
			}
			return fmt.Errorf("apply session guard %q: %w", g, err)
		}
	}
	return nil
}

// IsLockTimeout checks if an error was caused by a database lock timeout.
// In PostgreSQL, lock timeout is SQLSTATE 55P03 (lock_not_available).
// In MySQL, lock wait timeout is error code 1205.
func IsLockTimeout(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "55P03" { // lock_not_available
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "55p03") ||
		strings.Contains(msg, "lock timeout") ||
		strings.Contains(msg, "lock wait timeout") ||
		strings.Contains(msg, "canceling statement due to lock timeout")
}

// ExecuteWithBackoffJitter executes an operation with backoff and jitter if a lock timeout is encountered.
// If lock timeouts persist, it fails fast with a descriptive error alerting about concurrent DDL / migrations.
func ExecuteWithBackoffJitter(ctx context.Context, maxRetries int, baseDelay time.Duration, op func() error) error {
	if baseDelay <= 0 {
		baseDelay = 50 * time.Millisecond
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := op()
		if err == nil {
			return nil
		}

		if !IsLockTimeout(err) {
			return err
		}

		if attempt == maxRetries {
			return fmt.Errorf("lock timeout detected (concurrent DDL or migration on source): %w", err)
		}

		// Jittered exponential backoff: baseDelay * 2^attempt + jitter
		jitter := time.Duration(rand.Int63n(int64(baseDelay)))
		backoff := (baseDelay * (1 << attempt)) + jitter

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
