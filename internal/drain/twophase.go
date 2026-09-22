package drain

import (
	"fmt"
	"strings"

	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// PendingUpdate represents a Phase 2 update statement to reinstate a cyclic foreign key value.
type PendingUpdate struct {
	Table       string
	FKCols      []string
	FKVals      []any
	FKColInfos  []introspect.Column
	PKCols      []string
	PKVals      []any
	PKColInfos  []introspect.Column
}

// TwoPhaseResolver coordinates Two-Phase insert and update resolution for cyclic SCC clusters,
// avoiding foreign key constraint violations in MySQL/MariaDB without requiring FOREIGN_KEY_CHECKS = 0.
type TwoPhaseResolver struct {
	schema      *introspect.Schema
	g           *graph.Graph
	sccs        []graph.SCC
	engine      db.EngineType
	schemaScope string

	// cyclicNullableFKs: table -> set of column names that are nullable and part of an intra-SCC cycle
	cyclicNullableFKs map[string]map[string]bool

	// pendingUpdates: collected updates to be applied in Phase 2
	pendingUpdates []PendingUpdate
}

// NewTwoPhaseResolver builds a TwoPhaseResolver for the given graph and SCC components.
func NewTwoPhaseResolver(schema *introspect.Schema, g *graph.Graph, sccs []graph.SCC, engine db.EngineType, schemaScope string) *TwoPhaseResolver {
	r := &TwoPhaseResolver{
		schema:            schema,
		g:                 g,
		sccs:              sccs,
		engine:            engine,
		schemaScope:       schemaScope,
		cyclicNullableFKs: make(map[string]map[string]bool),
	}

	r.detectCyclicNullableFKs()
	return r
}

// detectCyclicNullableFKs identifies intra-SCC foreign keys whose child column is nullable.
func (r *TwoPhaseResolver) detectCyclicNullableFKs() {
	if r.g == nil || r.schema == nil {
		return
	}

	for _, scc := range r.sccs {
		if !scc.HasCycle {
			continue
		}

		// Set of all tables in this strongly connected component
		sccTables := make(map[string]bool, len(scc.Tables))
		for _, t := range scc.Tables {
			sccTables[t] = true
		}

		for _, table := range scc.Tables {
			for _, edge := range r.g.OutEdges[table] {
				// Only inspect intra-SCC edges (cycles between tables in the SCC or self-references)
				if !sccTables[edge.ToTable] {
					continue
				}

				tableCols := r.schema.Columns[table]
				for _, fromColName := range edge.FromColumns {
					col := findColumnByName(tableCols, fromColName)
					if col != nil && col.IsNullable {
						if r.cyclicNullableFKs[table] == nil {
							r.cyclicNullableFKs[table] = make(map[string]bool)
						}
						r.cyclicNullableFKs[table][fromColName] = true
					}
				}
			}
		}
	}
}

// IsCyclicNullableFK returns true if the column participates in a cycle and is nullable.
func (r *TwoPhaseResolver) IsCyclicNullableFK(table, colName string) bool {
	if cols, ok := r.cyclicNullableFKs[table]; ok {
		return cols[colName]
	}
	return false
}

// TransformRow inspects a row's values. If any column is a cyclic nullable foreign key,
// its value in insertVals is coerced to nil (NULL) and a PendingUpdate is generated for Phase 2.
func (r *TwoPhaseResolver) TransformRow(table string, cols []introspect.Column, vals []any) ([]any, []PendingUpdate) {
	insertVals := make([]any, len(vals))
	copy(insertVals, vals)

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	pkCols := r.schema.PrimaryKeys[table]
	pkVals := make([]any, 0, len(pkCols))
	pkColInfos := make([]introspect.Column, 0, len(pkCols))
	for _, pk := range pkCols {
		for j, col := range cols {
			if col.Name == pk {
				pkVals = append(pkVals, vals[j])
				pkColInfos = append(pkColInfos, col)
				break
			}
		}
	}

	var updates []PendingUpdate

	for i, col := range cols {
		if !r.IsCyclicNullableFK(table, col.Name) {
			continue
		}

		val := vals[i]
		// If original value is nil or string "NULL", no update is needed later
		if val == nil || fmt.Sprintf("%v", val) == "<nil>" || fmt.Sprintf("%v", val) == "NULL" {
			continue
		}

		// Coerce to NULL for Phase 1 insert
		insertVals[i] = nil

		// Queue update for Phase 2
		updates = append(updates, PendingUpdate{
			Table:       table,
			FKCols:      []string{col.Name},
			FKVals:      []any{val},
			FKColInfos:  []introspect.Column{col},
			PKCols:      pkCols,
			PKVals:      pkVals,
			PKColInfos:  pkColInfos,
		})
	}

	return insertVals, updates
}

// GeneratePhase1Insert generates the Phase 1 INSERT statement for a row, coercing cyclic nullable FKs to NULL.
func (r *TwoPhaseResolver) GeneratePhase1Insert(table string, cols []introspect.Column, vals []any) (string, []PendingUpdate) {
	insertVals, updates := r.TransformRow(table, cols, vals)

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = db.QuoteIdent(r.engine, c.Name)
	}

	quotedTable := db.QuoteTable(r.engine, r.schemaScope, table)
	valuesClause := FormatValues(r.engine, cols, insertVals)

	var stmt string
	if r.engine == db.EngineMySQL {
		stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			quotedTable,
			strings.Join(colNames, ", "),
			valuesClause,
		)
	} else {
		stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING;",
			quotedTable,
			strings.Join(colNames, ", "),
			valuesClause,
		)
	}

	return stmt, updates
}

// GeneratePhase2Update generates the UPDATE statement to restore a row's cyclic foreign key.
// Format: UPDATE <table> SET <fk_col> = <val> WHERE <pk_col> = <pk_val>;
func (r *TwoPhaseResolver) GeneratePhase2Update(u PendingUpdate) string {
	quotedTable := db.QuoteTable(r.engine, r.schemaScope, u.Table)

	setClauses := make([]string, len(u.FKCols))
	for i, col := range u.FKCols {
		var colInfo introspect.Column
		if i < len(u.FKColInfos) {
			colInfo = u.FKColInfos[i]
		}
		valStr := FormatValue(r.engine, colInfo, u.FKVals[i])
		setClauses[i] = fmt.Sprintf("%s = %s", db.QuoteIdent(r.engine, col), valStr)
	}

	whereClauses := make([]string, len(u.PKCols))
	for i, pk := range u.PKCols {
		var colInfo introspect.Column
		if i < len(u.PKColInfos) {
			colInfo = u.PKColInfos[i]
		}
		valStr := FormatValue(r.engine, colInfo, u.PKVals[i])
		whereClauses[i] = fmt.Sprintf("%s = %s", db.QuoteIdent(r.engine, pk), valStr)
	}

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;",
		quotedTable,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
}

// GeneratePhase2Updates converts a slice of PendingUpdate into SQL UPDATE statements.
func (r *TwoPhaseResolver) GeneratePhase2Updates(updates []PendingUpdate) []string {
	stmts := make([]string, len(updates))
	for i, u := range updates {
		stmts[i] = r.GeneratePhase2Update(u)
	}
	return stmts
}

// RecordPendingUpdates registers updates to be flushed or executed later.
func (r *TwoPhaseResolver) RecordPendingUpdates(updates []PendingUpdate) {
	r.pendingUpdates = append(r.pendingUpdates, updates...)
}

// FlushPendingUpdates returns the formatted SQL statements for all recorded updates and clears the list.
func (r *TwoPhaseResolver) FlushPendingUpdates() []string {
	stmts := r.GeneratePhase2Updates(r.pendingUpdates)
	r.pendingUpdates = nil
	return stmts
}

// PendingCount returns the number of recorded pending updates.
func (r *TwoPhaseResolver) PendingCount() int {
	return len(r.pendingUpdates)
}

// GetPendingUpdates returns a copy of pending updates.
func (r *TwoPhaseResolver) GetPendingUpdates() []PendingUpdate {
	out := make([]PendingUpdate, len(r.pendingUpdates))
	copy(out, r.pendingUpdates)
	return out
}
