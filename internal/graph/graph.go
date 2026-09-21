package graph

import (
	"strings"

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

// TableColumn references a specific column in a specific table.
type TableColumn struct {
	Table  string
	Column string
}

// VirtualEdge represents a polymorphic (non-schema) relationship between tables.
// A single child column may point to different parent tables depending on a
// runtime discriminator column value.
//
// Example (Rails polymorphic):
//
//	child_table:  comments
//	child_column: commentable_id   ← FK value
//	discriminator: commentable_type ← runtime type column
//	mappings: "post" → {posts, id}, "video" → {videos, id}
type VirtualEdge struct {
	ChildTable       string
	ChildColumn      string              // FK value column (e.g. commentable_id)
	DiscriminatorCol string              // type discriminator column (e.g. commentable_type)
	Mappings         map[string]TableColumn // discriminator value → parent table.column
}

// VirtualFKInput is the schema-agnostic input type used to register virtual FKs.
// It mirrors config.VirtualForeignKey but avoids an import cycle.
type VirtualFKInput struct {
	ChildTable        string
	ChildColumn       string
	DiscriminatorCol  string
	Mappings          map[string]string // "post" → "posts.id"
}

// Graph is an adjacency-list directed graph of table dependencies.
type Graph struct {
	Nodes        map[string]*Node
	OutEdges     map[string][]Edge        // from → edges (FK child → FK parent direction)
	InEdges      map[string][]Edge        // to   → edges (FK parent → FK child direction)
	VirtualEdges map[string][]VirtualEdge // child_table → virtual edges
}

// New builds a dependency graph from the introspected schema and optional virtual FKs.
func New(schema *introspect.Schema, virtualFKs ...[]VirtualFKInput) *Graph {
	g := &Graph{
		Nodes:        make(map[string]*Node),
		OutEdges:     make(map[string][]Edge),
		InEdges:      make(map[string][]Edge),
		VirtualEdges: make(map[string][]VirtualEdge),
	}

	for table := range schema.Columns {
		g.Nodes[table] = &Node{Table: table}
	}

	for _, fk := range schema.ForeignKeys {
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

	// Register virtual FK edges.
	for _, vfks := range virtualFKs {
		for _, vfk := range vfks {
			ve := VirtualEdge{
				ChildTable:       vfk.ChildTable,
				ChildColumn:      vfk.ChildColumn,
				DiscriminatorCol: vfk.DiscriminatorCol,
				Mappings:         make(map[string]TableColumn, len(vfk.Mappings)),
			}
			for discVal, tableCol := range vfk.Mappings {
				tc := parseTableCol(tableCol)
				ve.Mappings[discVal] = tc
				// Register parent table as a node if not already present.
				if _, ok := g.Nodes[tc.Table]; !ok {
					g.Nodes[tc.Table] = &Node{Table: tc.Table}
				}
			}
			// Register child table as a node if not already present.
			if _, ok := g.Nodes[vfk.ChildTable]; !ok {
				g.Nodes[vfk.ChildTable] = &Node{Table: vfk.ChildTable}
			}
			g.VirtualEdges[vfk.ChildTable] = append(g.VirtualEdges[vfk.ChildTable], ve)
		}
	}

	return g
}

// parseTableCol splits a "table.column" string into TableColumn.
// Returns TableColumn{Table: s} if no dot is found.
func parseTableCol(s string) TableColumn {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) == 2 {
		return TableColumn{Table: parts[0], Column: parts[1]}
	}
	return TableColumn{Table: s}
}

// Tables returns all table names in the graph.
func (g *Graph) Tables() []string {
	tables := make([]string, 0, len(g.Nodes))
	for t := range g.Nodes {
		tables = append(tables, t)
	}
	return tables
}
