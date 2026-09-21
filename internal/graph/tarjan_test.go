package graph

import (
	"sort"
	"testing"

	"github.com/x7ssss/dbdrain/internal/introspect"
)

func makeGraph(edges [][2]string) *Graph {
	schema := &introspect.Schema{
		Columns:     make(map[string][]introspect.Column),
		PrimaryKeys: make(map[string][]string),
	}
	nodes := map[string]bool{}
	for _, e := range edges {
		nodes[e[0]] = true
		nodes[e[1]] = true
		schema.ForeignKeys = append(schema.ForeignKeys, introspect.ForeignKey{
			FromTable: e[0],
			ToTable:   e[1],
		})
	}
	for n := range nodes {
		schema.Columns[n] = nil
	}
	return New(schema)
}

func TestTarjanNoCycle(t *testing.T) {
	// A -> B -> C (no cycle)
	g := makeGraph([][2]string{{"A", "B"}, {"B", "C"}})
	sccs := TarjanSCC(g)
	for _, scc := range sccs {
		if scc.HasCycle {
			t.Errorf("expected no cycle, got cycle in SCC: %v", scc.Tables)
		}
	}
}

func TestTarjanSimpleCycle(t *testing.T) {
	// A -> B -> A (cycle)
	g := makeGraph([][2]string{{"A", "B"}, {"B", "A"}})
	sccs := TarjanSCC(g)
	var cyclic []SCC
	for _, scc := range sccs {
		if scc.HasCycle {
			cyclic = append(cyclic, scc)
		}
	}
	if len(cyclic) != 1 {
		t.Fatalf("expected 1 cyclic SCC, got %d", len(cyclic))
	}
	sorted := append([]string{}, cyclic[0].Tables...)
	sort.Strings(sorted)
	if sorted[0] != "A" || sorted[1] != "B" {
		t.Errorf("expected A,B in cycle, got %v", sorted)
	}
}

func TestTarjanSelfReferential(t *testing.T) {
	// A -> A (self-ref)
	g := makeGraph([][2]string{{"A", "A"}})
	sccs := TarjanSCC(g)
	var found bool
	for _, scc := range sccs {
		if scc.HasCycle {
			found = true
		}
	}
	if !found {
		t.Error("expected self-referential cycle to be detected")
	}
}

func TestTarjanComplexCycle(t *testing.T) {
	// A->B->C->A (3-node cycle), D->E (no cycle)
	g := makeGraph([][2]string{
		{"A", "B"}, {"B", "C"}, {"C", "A"},
		{"D", "E"},
	})
	sccs := TarjanSCC(g)
	var cycleCount int
	var cycleTables []string
	for _, scc := range sccs {
		if scc.HasCycle {
			cycleCount++
			cycleTables = append(cycleTables, scc.Tables...)
		}
	}
	if cycleCount != 1 {
		t.Fatalf("expected 1 cyclic SCC, got %d", cycleCount)
	}
	if len(cycleTables) != 3 {
		t.Errorf("expected 3 tables in cycle, got %d: %v", len(cycleTables), cycleTables)
	}
}

func TestTopoOrder(t *testing.T) {
	// A->B->C: C should come before B, B before A
	g := makeGraph([][2]string{{"A", "B"}, {"B", "C"}})
	sccs := TarjanSCC(g)
	order := TopoOrder(sccs)
	pos := map[string]int{}
	for i, t2 := range order {
		pos[t2] = i
	}
	if pos["C"] >= pos["B"] {
		t.Errorf("expected C before B in topo order, got order: %v", order)
	}
	if pos["B"] >= pos["A"] {
		t.Errorf("expected B before A in topo order, got order: %v", order)
	}
}
