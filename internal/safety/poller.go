package safety

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/x7ssss/dbdrain/internal/db"
)

// HealthState represents the current health and throttling state of the source database.
type HealthState string

const (
	StateNormal  HealthState = "Normal"
	StateBackoff HealthState = "Backoff"
	StatePaused  HealthState = "Paused"
	StateResumed HealthState = "Resumed"
)

// Metrics holds health statistics collected during a poll cycle.
type Metrics struct {
	Engine db.EngineType

	// MySQL specific
	HistoryListLength int64
	ReplicationLagSec *int64 // nil if not a replica

	// PostgreSQL specific
	ActiveSessions  int
	IOWaiters       int
	LockWaiters     int
	OldestTxAgeSec  float64
	SlotWALLagBytes int64

	CollectedAt time.Time
	Err         error
}

// Thresholds defines multi-level safety limits for pausing or backing off.
type Thresholds struct {
	MaxLag           time.Duration // default: 30s
	MySQLHLLBackoff  int64         // 100,000
	MySQLHLLPause    int64         // 1,000,000
	PGLockWaitersCap int           // 5
	PGIOWaitersCap   int           // 10
	PollInterval     time.Duration // 5s
}

// DefaultThresholds returns default safety thresholds with the provided max lag.
func DefaultThresholds(maxLag time.Duration) Thresholds {
	if maxLag <= 0 {
		maxLag = 30 * time.Second
	}
	return Thresholds{
		MaxLag:           maxLag,
		MySQLHLLBackoff:  100_000,
		MySQLHLLPause:    1_000_000,
		PGLockWaitersCap: 5,
		PGIOWaitersCap:   10,
		PollInterval:     5 * time.Second,
	}
}

// EvaluateState calculates the next HealthState based on current metrics and thresholds.
func EvaluateState(m Metrics, t Thresholds, prevState HealthState) (HealthState, string) {
	if m.Err != nil {
		// Do not transition to paused on transient monitoring probe errors, maintain previous state
		return prevState, ""
	}

	switch m.Engine {
	case db.EngineMySQL:
		// Pause condition 1: History List Length > 1,000,000
		if m.HistoryListLength > t.MySQLHLLPause {
			return StatePaused, fmt.Sprintf("history list > %dk", t.MySQLHLLPause/1000)
		}
		// Pause condition 2: Replication lag spikes
		if m.ReplicationLagSec != nil && *m.ReplicationLagSec > int64(t.MaxLag.Seconds()) {
			return StatePaused, fmt.Sprintf("replica lag (%ds) > max-lag (%s)", *m.ReplicationLagSec, t.MaxLag)
		}
		// Backoff condition: History List Length > 100,000
		if m.HistoryListLength > t.MySQLHLLBackoff {
			return StateBackoff, fmt.Sprintf("history list > %dk", t.MySQLHLLBackoff/1000)
		}

	case db.EnginePostgres:
		// Pause condition 1: Lock waiters spike
		if m.LockWaiters >= t.PGLockWaitersCap {
			return StatePaused, fmt.Sprintf("lock waiters spike (%d >= %d)", m.LockWaiters, t.PGLockWaitersCap)
		}
		// Pause condition 2: Oldest transaction exceeds max lag
		if m.OldestTxAgeSec > t.MaxLag.Seconds() {
			return StatePaused, fmt.Sprintf("oldest tx age (%.0fs) > max-lag (%s)", m.OldestTxAgeSec, t.MaxLag)
		}
		// Backoff condition: IO waiters elevated or transaction age > max-lag / 2
		if m.IOWaiters >= t.PGIOWaitersCap || m.OldestTxAgeSec > t.MaxLag.Seconds()/2 {
			return StateBackoff, fmt.Sprintf("elevated load (IO waiters: %d, tx age: %.0fs)", m.IOWaiters, m.OldestTxAgeSec)
		}
	}

	// Healthy state evaluation
	if prevState == StatePaused {
		return StateResumed, ""
	}
	return StateNormal, ""
}

// HealthPoller runs an independent background monitor that polls cluster health via short autocommit connections.
type HealthPoller struct {
	sourceURI  string
	engine     db.EngineType
	thresholds Thresholds

	mu          sync.RWMutex
	state       HealthState
	metrics     Metrics
	pauseReason string
	unpauseCh   chan struct{}

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewHealthPoller creates a HealthPoller.
func NewHealthPoller(sourceURI string, engine db.EngineType, thresholds Thresholds) *HealthPoller {
	return &HealthPoller{
		sourceURI:  sourceURI,
		engine:     engine,
		thresholds: thresholds,
		state:      StateNormal,
		unpauseCh:  make(chan struct{}),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Start begins the background health polling goroutine.
func (p *HealthPoller) Start(ctx context.Context) {
	// Immediate initial poll
	p.pollOnce(ctx)

	go func() {
		defer close(p.doneCh)
		interval := p.thresholds.PollInterval
		if interval <= 0 {
			interval = 5 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-p.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.pollOnce(ctx)
			}
		}
	}()
}

// Stop stops the background polling monitor.
func (p *HealthPoller) Stop() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
		<-p.doneCh
	}
}

// State returns the current HealthState.
func (p *HealthPoller) State() HealthState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// EngineState returns a human-readable state string, e.g. "Healthy", "Backoff", "Paused".
func (p *HealthPoller) EngineState() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch p.state {
	case StateNormal, StateResumed:
		return "Healthy"
	case StateBackoff:
		return "Backoff"
	case StatePaused:
		return "Paused"
	default:
		return string(p.state)
	}
}

// IsPaused returns true if the cluster health is in StatePaused.
func (p *HealthPoller) IsPaused() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state == StatePaused
}

// PauseReason returns the reason why extraction was paused.
func (p *HealthPoller) PauseReason() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pauseReason
}

// Metrics returns the latest measured metrics.
func (p *HealthPoller) Metrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metrics
}

// HistoryListLength returns the current history list length (or 0 for Postgres).
func (p *HealthPoller) HistoryListLength() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metrics.HistoryListLength
}

// WaitIfPaused blocks while the health state is StatePaused.
// It returns nil as soon as health recovers (StateResumed or StateNormal), or ctx.Err() if cancelled.
func (p *HealthPoller) WaitIfPaused(ctx context.Context) error {
	for {
		p.mu.RLock()
		if p.state != StatePaused {
			p.mu.RUnlock()
			return nil
		}
		ch := p.unpauseCh
		p.mu.RUnlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			// Loop and verify state
		}
	}
}

// SetStateForTesting sets the poller state directly (for unit testing).
func (p *HealthPoller) SetStateForTesting(state HealthState, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prevState := p.state
	p.state = state
	p.pauseReason = reason
	if prevState == StatePaused && (state == StateResumed || state == StateNormal) {
		close(p.unpauseCh)
		p.unpauseCh = make(chan struct{})
	}
}

// pollOnce executes a single health poll over a short autocommit connection.
func (p *HealthPoller) pollOnce(ctx context.Context) {
	var m Metrics
	m.Engine = p.engine
	m.CollectedAt = time.Now()

	pollCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	if p.engine == db.EngineMySQL {
		m = p.pollMySQL(pollCtx)
	} else if p.engine == db.EnginePostgres {
		m = p.pollPostgres(pollCtx)
	}

	p.mu.Lock()
	prevState := p.state
	newState, reason := EvaluateState(m, p.thresholds, prevState)
	p.state = newState
	p.metrics = m
	p.pauseReason = reason

	if prevState == StatePaused && (newState == StateResumed || newState == StateNormal) {
		close(p.unpauseCh)
		p.unpauseCh = make(chan struct{})
	}
	p.mu.Unlock()
}

func (p *HealthPoller) pollMySQL(ctx context.Context) Metrics {
	m := Metrics{Engine: db.EngineMySQL, CollectedAt: time.Now()}

	dsn, _, err := db.ParseMySQLDSN(p.sourceURI)
	if err != nil {
		m.Err = err
		return m
	}

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		m.Err = err
		return m
	}
	defer sqlDB.Close()

	// Short autocommit connection
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		m.Err = err
		return m
	}
	defer conn.Close()

	// 1. History List Length from INNODB_METRICS
	var hll int64
	row := conn.QueryRowContext(ctx, "SELECT count FROM information_schema.INNODB_METRICS WHERE name = 'trx_rseg_history_len';")
	if err := row.Scan(&hll); err == nil {
		m.HistoryListLength = hll
	} else {
		// Fallback: parse SHOW ENGINE INNODB STATUS
		statusRow := conn.QueryRowContext(ctx, "SHOW ENGINE INNODB STATUS;")
		var typeCol, nameCol, statusText string
		if scanErr := statusRow.Scan(&typeCol, &nameCol, &statusText); scanErr == nil {
			re := regexp.MustCompile(`History list length\s+(\d+)`)
			if matches := re.FindStringSubmatch(statusText); len(matches) > 1 {
				if parsed, parseErr := strconv.ParseInt(matches[1], 10, 64); parseErr == nil {
					m.HistoryListLength = parsed
				}
			}
		}
	}

	// 2. Replication Lag from SHOW REPLICA STATUS (or fallback SHOW SLAVE STATUS)
	var lagSec *int64
	replicaRows, err := conn.QueryContext(ctx, "SHOW REPLICA STATUS;")
	if err != nil {
		replicaRows, err = conn.QueryContext(ctx, "SHOW SLAVE STATUS;")
	}
	if err == nil {
		defer replicaRows.Close()
		cols, _ := replicaRows.Columns()
		if replicaRows.Next() {
			vals := make([]any, len(cols))
			valPtrs := make([]any, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := replicaRows.Scan(valPtrs...); err == nil {
				for i, col := range cols {
					colLower := strings.ToLower(col)
					if colLower == "seconds_behind_source" || colLower == "seconds_behind_master" {
						if vals[i] != nil {
							var valInt int64
							switch v := vals[i].(type) {
							case int64:
								valInt = v
							case []byte:
								valInt, _ = strconv.ParseInt(string(v), 10, 64)
							case string:
								valInt, _ = strconv.ParseInt(v, 10, 64)
							}
							lagSec = &valInt
						}
						break
					}
				}
			}
		}
	}
	m.ReplicationLagSec = lagSec
	return m
}

func (p *HealthPoller) pollPostgres(ctx context.Context) Metrics {
	m := Metrics{Engine: db.EnginePostgres, CollectedAt: time.Now()}

	conn, err := pgx.Connect(ctx, p.sourceURI)
	if err != nil {
		m.Err = err
		return m
	}
	defer conn.Close(ctx)

	// 1. pg_stat_activity: active sessions, IO waiters, lock waiters, transaction age
	statQuery := `
		SELECT
			COALESCE(COUNT(*) FILTER (WHERE state = 'active'), 0) AS active_sessions,
			COALESCE(COUNT(*) FILTER (WHERE wait_event_type = 'IO'), 0) AS io_waiters,
			COALESCE(COUNT(*) FILTER (WHERE wait_event_type = 'Lock'), 0) AS lock_waiters,
			COALESCE(MAX(EXTRACT(EPOCH FROM (now() - xact_start))), 0) AS oldest_tx_age_sec
		FROM pg_stat_activity
		WHERE pid <> pg_backend_pid();
	`
	var activeSessions, ioWaiters, lockWaiters int
	var oldestTxAge float64

	row := conn.QueryRow(ctx, statQuery)
	if err := row.Scan(&activeSessions, &ioWaiters, &lockWaiters, &oldestTxAge); err == nil {
		m.ActiveSessions = activeSessions
		m.IOWaiters = ioWaiters
		m.LockWaiters = lockWaiters
		m.OldestTxAgeSec = oldestTxAge
	} else {
		m.Err = err
		return m
	}

	// 2. Replication slot WAL lag
	walSlotQuery := `
		SELECT COALESCE(MAX(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)
		FROM pg_replication_slots;
	`
	var walLag int64
	if err := conn.QueryRow(ctx, walSlotQuery).Scan(&walLag); err == nil {
		m.SlotWALLagBytes = walLag
	}

	return m
}
