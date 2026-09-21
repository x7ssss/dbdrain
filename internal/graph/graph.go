package graph

import (
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// Node represents a table in the dependency graph.
type Node struct {
	Table string
}

// Edge represents a foreign-key directed edge: From -> To.
type Edge struct {
	FromTable   string
	ToTable     string
	FromColumns []string
	ToColumns   []string
	FK          introspect.ForeignKey
}

// Graph is an adjacency-list directed graph of table dependencies.
type Graph struct {
	Nodes     map[string]*Node
	OutEdges  map[string][]Edge // from -> list of edges
	InEdges   map[string][]Edge // to -> list of edges (reverse)
}

// New builds a dependency graph from the introspected schema.
func New(schema *introspect.Schema) *Graph {
	g := &Graph{
		Nodes:    make(map[string]*Node),
		OutEdges: make(map[string][]Edge),
		InEdges:  make(map[string][]Edge),
	}

	for table := range schema.Columns {
		g.Nodes[table] = &Node{Table: table}
	}

	for _, fk := range schema.ForeignKeys {
		// Ensure nodes exist even if not in columns (cross-schema edge)
		if _, ok := g.Nodes[fk.FromTable]; !ok {
			g.Nodes[fk.FromTable] = &Node{Table: fk.FromTable}
		}
		if _, ok := g.Nodes[fk.ToTable]; !ok {
			g.Nodes[fk.ToTable] = &Node{Table: fk.ToTable}
		}
		e := Edge{
			FromTable:   fk.FromTable,
			ToTable:     fk.ToTable,
			FromColumns: fk.FromColumns,
			ToColumns:   fk.ToColumns,
			FK:          fk,
		}
		g.OutEdges[fk.FromTable] = append(g.OutEdges[fk.FromTable], e)
		g.InEdges[fk.ToTable] = append(g.InEdges[fk.ToTable], e)
	}

	return g
}

// Tables returns all table names in the graph.
func (g *Graph) Tables() []string {
	tables := make([]string, 0, len(g.Nodes))
	for t := range g.Nodes {
		tables = append(tables, t)
	}
	return tables
}
