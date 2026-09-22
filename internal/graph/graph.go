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
		if schema != nil && (schema.IsChildPartition(table) || schema.IsPhysicalShard(table)) {
			continue
		}
		g.Nodes[table] = &Node{Table: table}
	}

	if schema != nil {
		for root := range schema.PartitionedTables {
			if _, ok := g.Nodes[root]; !ok {
				g.Nodes[root] = &Node{Table: root}
			}
		}
	}

	for _, fk := range schema.ForeignKeys {
		if schema != nil && (schema.IsPhysicalShard(fk.FromTable) || schema.IsPhysicalShard(fk.ToTable)) {
			continue
		}
		fromTable := fk.FromTable
		toTable := fk.ToTable
		if schema != nil {
			fromTable = schema.RootPartition(fromTable)
			toTable = schema.RootPartition(toTable)
		}

		if _, ok := g.Nodes[fromTable]; !ok {
			g.Nodes[fromTable] = &Node{Table: fromTable}
		}
		if _, ok := g.Nodes[toTable]; !ok {
			g.Nodes[toTable] = &Node{Table: toTable}
		}

		// Deduplicate identical edges mapped to root partitions
		exists := false
		for _, existing := range g.OutEdges[fromTable] {
			if existing.ToTable == toTable &&
				sliceEqual(existing.FromColumns, fk.FromColumns) &&
				sliceEqual(existing.ToColumns, fk.ToColumns) {
				exists = true
				break
			}
		}
		if exists {
			continue
		}

		e := Edge{
			FromTable:   fromTable,
			ToTable:     toTable,
			FromColumns: fk.FromColumns,
			ToColumns:   fk.ToColumns,
			FK:          fk,
		}
		g.OutEdges[fromTable] = append(g.OutEdges[fromTable], e)
		g.InEdges[toTable] = append(g.InEdges[toTable], e)
	}

	// Register virtual FK edges.
	for _, vfks := range virtualFKs {
		for _, vfk := range vfks {
			childTable := vfk.ChildTable
			if schema != nil {
				childTable = schema.RootPartition(childTable)
			}
			ve := VirtualEdge{
				ChildTable:       childTable,
				ChildColumn:      vfk.ChildColumn,
				DiscriminatorCol: vfk.DiscriminatorCol,
				Mappings:         make(map[string]TableColumn, len(vfk.Mappings)),
			}
			for discVal, tableCol := range vfk.Mappings {
				tc := parseTableCol(tableCol)
				if schema != nil {
					tc.Table = schema.RootPartition(tc.Table)
				}
				ve.Mappings[discVal] = tc
				// Register parent table as a node if not already present.
				if _, ok := g.Nodes[tc.Table]; !ok {
					g.Nodes[tc.Table] = &Node{Table: tc.Table}
				}
			}
			// Register child table as a node if not already present.
			if _, ok := g.Nodes[childTable]; !ok {
				g.Nodes[childTable] = &Node{Table: childTable}
			}
			g.VirtualEdges[childTable] = append(g.VirtualEdges[childTable], ve)
		}
	}

	return g
}

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
