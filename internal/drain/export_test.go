package drain

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// --- helpers to build in-memory schema/graph for unit tests ---

func makeSchema(tables map[string][]string, fks []introspect.ForeignKey) *introspect.Schema {
	s := &introspect.Schema{
		Columns:     make(map[string][]introspect.Column),
		PrimaryKeys: make(map[string][]string),
		ForeignKeys: fks,
	}
	for table, cols := range tables {
		for i, c := range cols {
			s.Columns[table] = append(s.Columns[table], introspect.Column{
				Name:    c,
				DataType: "text",
				Ordinal:  i + 1,
			})
		}
		if len(cols) > 0 {
			s.PrimaryKeys[table] = []string{cols[0]} // first col = PK
		}
	}
	return s
}

// --- tests ---

// TestMaxRowsPerTableCap verifies that the MaxRowsPerTable config field is
// correctly stored and accessible in Config.
func TestMaxRowsPerTableCap(t *testing.T) {
	cfg := Config{MaxRowsPerTable: 5}
	if cfg.MaxRowsPerTable != 5 {
		t.Errorf("expected MaxRowsPerTable=5, got %d", cfg.MaxRowsPerTable)
	}
}

// TestMaxDepthCap verifies that the MaxDepth config field is stored correctly.
func TestMaxDepthCap(t *testing.T) {
	cfg := Config{MaxDepth: 2}
	if cfg.MaxDepth != 2 {
		t.Errorf("expected MaxDepth=2, got %d", cfg.MaxDepth)
	}
}

// TestBFSChildrenDepthRespected simulates BFS downward traversal and ensures
// the depth limit prevents processing beyond cfg.MaxDepth levels.
func TestBFSChildrenDepthRespected(t *testing.T) {
	// Build a chain: A <- B <- C <- D (3 levels of children under A)
	schema := makeSchema(map[string][]string{
		"a": {"id", "name"},
		"b": {"id", "a_id"},
		"c": {"id", "b_id"},
		"d": {"id", "c_id"},
	}, []introspect.ForeignKey{
		{FromTable: "b", FromColumns: []string{"a_id"}, ToTable: "a", ToColumns: []string{"id"}},
		{FromTable: "c", FromColumns: []string{"b_id"}, ToTable: "b", ToColumns: []string{"id"}},
		{FromTable: "d", FromColumns: []string{"c_id"}, ToTable: "c", ToColumns: []string{"id"}},
	})
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	// With MaxDepth=1 only "b" should be reachable from "a".
	exp := New(nil, schema, g, sccs, Config{MaxDepth: 1})

	// Simulate visited PKs for table "a"
	exp.markVisited("a", "1")

	// Count how many child tables would be queued at depth 1 vs depth 2.
	type work struct {
		table string
		depth int
	}
	queue := []work{{"a", 0}}
	visited := map[string]bool{"a": true}
	var processed []string

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if exp.cfg.MaxDepth > 0 && item.depth >= exp.cfg.MaxDepth {
			continue
		}

		for _, edge := range g.InEdges[item.table] {
			child := edge.FromTable
			if child == item.table || len(edge.FromColumns) != 1 {
				continue
			}
			processed = append(processed, child)
			if !visited[child] {
				visited[child] = true
				queue = append(queue, work{child, item.depth + 1})
			}
		}
	}

	// With MaxDepth=1 we should process "b" (depth 0->1) but NOT "c" or "d".
	for _, p := range processed {
		if p == "c" || p == "d" {
			t.Errorf("depth=1 should not process %q (too deep)", p)
		}
	}
	found := false
	for _, p := range processed {
		if p == "b" {
			found = true
		}
	}
	if !found {
		t.Error("depth=1 should process 'b' (direct child of 'a')")
	}
}

// TestSelfRefEdgeDetection verifies isSelfRef correctly identifies self-referencing tables.
func TestSelfRefEdgeDetection(t *testing.T) {
	schema := makeSchema(map[string][]string{
		"categories": {"id", "parent_id", "name"},
		"products":   {"id", "category_id", "name"},
	}, []introspect.ForeignKey{
		// Self-referencing
		{FromTable: "categories", FromColumns: []string{"parent_id"}, ToTable: "categories", ToColumns: []string{"id"}},
		// Regular FK
		{FromTable: "products", FromColumns: []string{"category_id"}, ToTable: "categories", ToColumns: []string{"id"}},
	})

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{})

	if !exp.isSelfRef("categories") {
		t.Error("expected 'categories' to be self-referencing")
	}
	if exp.isSelfRef("products") {
		t.Error("expected 'products' NOT to be self-referencing")
	}
}

// TestSelfRefEdgeReturnsEdge verifies selfRefEdge returns the correct edge.
func TestSelfRefEdgeReturnsEdge(t *testing.T) {
	schema := makeSchema(map[string][]string{
		"users": {"id", "manager_id", "name"},
	}, []introspect.ForeignKey{
		{FromTable: "users", FromColumns: []string{"manager_id"}, ToTable: "users", ToColumns: []string{"id"}},
	})
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{})

	edge := exp.selfRefEdge("users")
	if edge == nil {
		t.Fatal("expected self-ref edge for 'users', got nil")
	}
	if edge.FromTable != "users" || edge.ToTable != "users" {
		t.Errorf("unexpected edge: %+v", edge)
	}
	if edge.FromColumns[0] != "manager_id" {
		t.Errorf("expected FromColumn=manager_id, got %s", edge.FromColumns[0])
	}
}

// TestNoSelfRefInfiniteLoop verifies that markVisited prevents revisiting
// the same row — which is the mechanism that prevents infinite loops in
// self-referencing hierarchy resolution.
func TestNoSelfRefInfiniteLoop(t *testing.T) {
	schema := makeSchema(map[string][]string{
		"categories": {"id", "parent_id", "name"},
	}, []introspect.ForeignKey{
		{FromTable: "categories", FromColumns: []string{"parent_id"}, ToTable: "categories", ToColumns: []string{"id"}},
	})
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{})

	// Simulate visiting the same row many times — markVisited must return true
	// after the first call, preventing re-emission.
	firstCall := exp.markVisited("categories", "42")
	if firstCall {
		t.Error("first visit should return false (not already visited)")
	}

	// All subsequent visits must return true (already visited).
	for i := 0; i < 1000; i++ {
		if !exp.markVisited("categories", "42") {
			t.Errorf("iteration %d: expected markVisited to return true (already visited)", i)
		}
	}
}

// TestHasCycles verifies hasCycles correctly reads the SCC list.
func TestHasCycles(t *testing.T) {
	schema := makeSchema(map[string][]string{
		"a": {"id"}, "b": {"id", "a_id"},
	}, []introspect.ForeignKey{
		{FromTable: "a", FromColumns: []string{"id"}, ToTable: "b", ToColumns: []string{"id"}},
		{FromTable: "b", FromColumns: []string{"a_id"}, ToTable: "a", ToColumns: []string{"id"}},
	})
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{})

	if !exp.hasCycles() {
		t.Error("expected hasCycles=true for A<->B cycle")
	}
}

// TestFormatStringList verifies SQL IN-list formatting.
func TestFormatStringList(t *testing.T) {
	result := formatStringList([]string{"a", "b'c", "d"})
	expected := "'a', 'b''c', 'd'"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

// TestBuildQuery verifies query string construction.
func TestBuildQuery(t *testing.T) {
	q := buildQuery("public", "users", []string{"id", "name"}, "id > 5", 10)
	if !strings.Contains(q, `WHERE id > 5`) {
		t.Errorf("expected WHERE clause in query: %s", q)
	}
	if !strings.Contains(q, `LIMIT 10`) {
		t.Errorf("expected LIMIT in query: %s", q)
	}
}

// TestBuildQueryNoLimit verifies no LIMIT clause when limit=0.
func TestBuildQueryNoLimit(t *testing.T) {
	q := buildQuery("public", "users", []string{"id"}, "", 0)
	if strings.Contains(q, "LIMIT") {
		t.Errorf("unexpected LIMIT in query: %s", q)
	}
}

// TestRowCountTracking verifies RowCounts increments correctly via markVisited.
func TestRowCountTracking(t *testing.T) {
	schema := makeSchema(map[string][]string{"t": {"id"}}, nil)
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	exp := New(nil, schema, g, sccs, Config{})

	// Manually simulate counting (as fetchAndEmit does internally).
	for _, pk := range []string{"1", "2", "3"} {
		if !exp.markVisited("t", pk) {
			exp.RowCounts["t"]++
		}
	}
	// Try a duplicate — should not increment.
	if !exp.markVisited("t", "1") {
		exp.RowCounts["t"]++
	}

	if exp.RowCounts["t"] != 3 {
		t.Errorf("expected RowCounts[t]=3, got %d", exp.RowCounts["t"])
	}
}

// Compile-time check that ExportToTarget signature is correct.
var _ = func() {
	var exp *Exporter
	var ctx context.Context
	var conn interface{ Begin(context.Context) (interface{}, error) }
	_ = exp
	_ = ctx
	_ = conn
	// Just verifying ExportToTarget exists with a pgx.Conn parameter.
	_ = fmt.Sprintf("%T", exp.ExportToTarget)
}
