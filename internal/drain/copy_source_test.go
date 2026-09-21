package drain

import (
	"errors"
	"reflect"
	"testing"

	"github.com/x7ssss/dbdrain/internal/config"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// mockRowIterator simulates a live pgx.Rows cursor for unit testing.
type mockRowIterator struct {
	rows   [][]any
	index  int
	closed bool
	err    error
}

func (m *mockRowIterator) Next() bool {
	if m.closed || m.err != nil {
		return false
	}
	if m.index < len(m.rows) {
		m.index++
		return true
	}
	return false
}

func (m *mockRowIterator) Values() ([]any, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.rows[m.index-1], nil
}

func (m *mockRowIterator) Err() error {
	return m.err
}

func (m *mockRowIterator) Close() {
	m.closed = true
}

func TestCursorCopySourceStreaming(t *testing.T) {
	cols := []introspect.Column{
		{Name: "id", DataType: "bigint", Ordinal: 1},
		{Name: "email", DataType: "text", Ordinal: 2},
	}
	colNames := []string{"id", "email"}
	pkCols := []string{"id"}

	schema := makeSchema(map[string][]string{
		"users": {"id", "email"},
	}, nil)
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{
		AnonymizePII: true,
		Salt:         "test-salt",
	})

	mockRows := [][]any{
		{int64(1), "alice@example.com"},
		{int64(2), "bob@example.com"},
		{int64(3), "carol@example.com"},
	}

	iter := &mockRowIterator{rows: mockRows}
	var progressReported []string
	progressFn := func(table string, n int) {
		progressReported = append(progressReported, table)
	}

	src := NewCursorCopySource(iter, "users", cols, colNames, pkCols, exp, progressFn)

	count := 0
	for src.Next() {
		vals, err := src.Values()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(vals) != 2 {
			t.Fatalf("expected 2 values, got %d", len(vals))
		}
		emailStr, ok := vals[1].(string)
		if !ok {
			t.Fatalf("expected string email, got %T", vals[1])
		}
		// Masking should have been applied
		if emailStr == "alice@example.com" || emailStr == "bob@example.com" || emailStr == "carol@example.com" {
			t.Errorf("expected anonymized email, got raw: %s", emailStr)
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 streamed rows, got %d", count)
	}
	if len(progressReported) != 3 {
		t.Errorf("expected 3 progress callbacks, got %d", len(progressReported))
	}
	if !iter.closed {
		t.Errorf("expected underlying iterator to be closed")
	}
}

func TestCursorCopySourceDeduplication(t *testing.T) {
	cols := []introspect.Column{
		{Name: "id", DataType: "bigint", Ordinal: 1},
		{Name: "name", DataType: "text", Ordinal: 2},
	}
	colNames := []string{"id", "name"}
	pkCols := []string{"id"}

	schema := makeSchema(map[string][]string{
		"users": {"id", "name"},
	}, nil)
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{})

	// Mark id=1 as already visited
	exp.markVisited("users", "1")

	mockRows := [][]any{
		{int64(1), "Alice"}, // duplicate, should be skipped
		{int64(2), "Bob"},   // fresh, should be streamed
	}

	iter := &mockRowIterator{rows: mockRows}
	src := NewCursorCopySource(iter, "users", cols, colNames, pkCols, exp, nil)

	var streamedIDs []int64
	for src.Next() {
		vals, _ := src.Values()
		streamedIDs = append(streamedIDs, vals[0].(int64))
	}

	if !reflect.DeepEqual(streamedIDs, []int64{2}) {
		t.Errorf("expected only id 2 to be streamed, got %v", streamedIDs)
	}
}

func TestCursorCopySourceSyncPoolReuse(t *testing.T) {
	// Verify that slicePool properly recycles slices and clears contents
	pool := &slicePool{
		pool: globalSlicePool.pool,
	}

	s1 := pool.Get(4)
	s1[0] = "test-value"
	pool.Put(s1)

	s2 := pool.Get(4)
	if s2[0] != nil {
		t.Errorf("expected pooled slice elements to be nil after Put, got %v", s2[0])
	}
}

func TestCursorCopySourceErrorPropagation(t *testing.T) {
	cols := []introspect.Column{{Name: "id", DataType: "bigint", Ordinal: 1}}
	colNames := []string{"id"}
	pkCols := []string{"id"}

	expectedErr := errors.New("simulated network read failure")
	iter := &mockRowIterator{err: expectedErr}

	src := NewCursorCopySource(iter, "users", cols, colNames, pkCols, nil, nil)
	if src.Next() {
		t.Fatal("expected Next() to return false on iterator error")
	}
	if src.Err() != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, src.Err())
	}
}

func TestCursorCopySourceMaskingRules(t *testing.T) {
	cols := []introspect.Column{
		{Name: "id", DataType: "int", Ordinal: 1},
		{Name: "secret_token", DataType: "text", Ordinal: 2},
	}
	colNames := []string{"id", "secret_token"}
	pkCols := []string{"id"}

	schema := makeSchema(map[string][]string{
		"sessions": {"id", "secret_token"},
	}, nil)
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{
		AnonymizePII: true,
		Salt:         "salt",
		Rules: []config.Rule{
			{Table: "*", Column: "*_token", Transform: config.TransformRedact},
		},
	})

	mockRows := [][]any{
		{1, "super_secret_session_token"},
	}
	iter := &mockRowIterator{rows: mockRows}
	src := NewCursorCopySource(iter, "sessions", cols, colNames, pkCols, exp, nil)

	if !src.Next() {
		t.Fatal("expected row to stream")
	}
	vals, _ := src.Values()
	if vals[1] != "[REDACTED]" {
		t.Errorf("expected [REDACTED] from rule transform, got %v", vals[1])
	}
}
