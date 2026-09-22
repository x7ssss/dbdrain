package drain

import (
	"sync"

	"github.com/x7ssss/dbdrain/internal/introspect"
)

// slicePool manages recycled []any slices to ensure zero heap churn per row.
type slicePool struct {
	pool sync.Pool
}

var globalSlicePool = &slicePool{
	pool: sync.Pool{
		New: func() any {
			s := make([]any, 0, 32)
			return &s
		},
	},
}

func (p *slicePool) Get(n int) []any {
	ptr := p.pool.Get().(*[]any)
	s := *ptr
	if cap(s) < n {
		s = make([]any, n)
	} else {
		s = s[:n]
	}
	return s
}

func (p *slicePool) Put(s []any) {
	for i := range s {
		s[i] = nil // clear references for GC safety
	}
	s = s[:0]
	p.pool.Put(&s)
}

// RowIterator abstracts over pgx.Rows allowing CursorCopySource to work over live cursors
// or mock iterators during unit testing.
type RowIterator interface {
	Next() bool
	Values() ([]any, error)
	Err() error
	Close()
}

// CursorCopySource implements pgx.CopyFromSource directly over a live source cursor (pgx.Rows).
// It streams rows on the fly, applies deterministic PII masking and visited-set deduplication,
// and recycles row value buffers using sync.Pool to guarantee flat O(1) memory usage.
type CursorCopySource struct {
	rows       RowIterator
	table      string
	cols       []introspect.Column
	colNames   []string
	pkCols     []string
	exporter   *Exporter
	progressFn func(table string, n int)

	pool        *slicePool
	currentVals []any
	err         error
	closed      bool
}

// NewCursorCopySource constructs a new CursorCopySource.
func NewCursorCopySource(
	rows RowIterator,
	table string,
	cols []introspect.Column,
	colNames []string,
	pkCols []string,
	exporter *Exporter,
	progressFn func(table string, n int),
) *CursorCopySource {
	return &CursorCopySource{
		rows:       rows,
		table:      table,
		cols:       cols,
		colNames:   colNames,
		pkCols:     pkCols,
		exporter:   exporter,
		progressFn: progressFn,
		pool:       globalSlicePool,
	}
}

// Next advances to the next unvisited row, recycling the previous row buffer through sync.Pool.
func (s *CursorCopySource) Next() bool {
	if s.closed || s.rows == nil {
		return false
	}

	// Recycle previous row values back to the pool
	if s.currentVals != nil {
		s.pool.Put(s.currentVals)
		s.currentVals = nil
	}

	for s.rows.Next() {
		rawVals, err := s.rows.Values()
		if err != nil {
			s.err = err
			return false
		}

		pk := buildPK(s.pkCols, s.colNames, rawVals)
		if s.exporter != nil && s.exporter.markVisited(s.table, pk) {
			continue
		}

		// Allocate/reuse slice from pool
		buf := s.pool.Get(len(rawVals))
		copy(buf, rawVals)

		if s.exporter != nil {
			s.exporter.applyMasking(s.table, s.cols, buf)
			s.exporter.coerceDanglingNulls(s.table, s.cols, buf)
			s.exporter.recordFKValues(s.table, s.cols, buf)
			s.exporter.capturePolyData(s.table, s.colNames, buf)
			s.exporter.RowCounts[s.table]++
		}

		if s.progressFn != nil {
			s.progressFn(s.table, 1)
		}

		s.currentVals = buf
		return true
	}

	if err := s.rows.Err(); err != nil {
		s.err = err
	}
	s.Close()
	return false
}

// Values returns the current row's values.
func (s *CursorCopySource) Values() ([]any, error) {
	return s.currentVals, s.err
}

// Err returns the error encountered during iteration, if any.
func (s *CursorCopySource) Err() error {
	return s.err
}

// Close releases the cursor and returns any retained buffers to the pool.
func (s *CursorCopySource) Close() {
	if !s.closed {
		s.closed = true
		if s.currentVals != nil {
			s.pool.Put(s.currentVals)
			s.currentVals = nil
		}
		if s.rows != nil {
			s.rows.Close()
		}
	}
}
