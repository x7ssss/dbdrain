package introspect

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Column holds metadata about a single table column.
type Column struct {
	Name       string
	DataType   string
	IsNullable bool
	Ordinal    int
}

// ForeignKey represents a foreign key constraint between two tables.
type ForeignKey struct {
	ConstraintName    string
	FromTable         string
	FromColumns       []string
	ToTable           string
	ToColumns         []string
	Deferrable        bool
	InitiallyDeferred bool
}

// PartitionBound describes a child partition and its bound definition.
type PartitionBound struct {
	PartitionName string
	Bound         string // e.g. "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
}

// PartitionedTable represents a declarative partitioned table and its child partitions.
type PartitionedTable struct {
	RootTable  string
	Strategy   string // e.g. "RANGE", "LIST", "HASH"
	KeyDef     string // e.g. "RANGE (created_at)" or "(created_at)"
	Partitions []PartitionBound
}

// Schema holds the full introspected schema for a given schema name.
type Schema struct {
	Columns              map[string][]Column          // table -> columns
	PrimaryKeys          map[string][]string          // table -> pk column names
	ForeignKeys          []ForeignKey
	TableEngines         map[string]string            // table -> storage engine (e.g. InnoDB, MyISAM)
	Warnings             []string                     // introspection warnings
	PartitionedTables    map[string]*PartitionedTable // root_table -> partition metadata
	ChildToRootPartition map[string]string            // child_partition -> root_table
}

// IsChildPartition returns true if table is a child partition of a declarative partitioned table.
func (s *Schema) IsChildPartition(table string) bool {
	if s == nil || s.ChildToRootPartition == nil {
		return false
	}
	_, ok := s.ChildToRootPartition[table]
	return ok
}

// RootPartition returns the logical root partitioned table name if table is a child partition,
// or returns table itself if it is not a partition.
func (s *Schema) RootPartition(table string) string {
	if s == nil || s.ChildToRootPartition == nil {
		return table
	}
	if root, ok := s.ChildToRootPartition[table]; ok {
		return root
	}
	return table
}

// IsPartitionedRoot returns true if table is a declarative partitioned table root.
func (s *Schema) IsPartitionedRoot(table string) bool {
	if s == nil || s.PartitionedTables == nil {
		return false
	}
	_, ok := s.PartitionedTables[table]
	return ok
}

// Load introspects the PostgreSQL schema using information_schema and pg_constraint.
func Load(ctx context.Context, conn *pgx.Conn, schemaName string) (*Schema, error) {
	return LoadPostgres(ctx, conn, schemaName)
}

// LoadPostgres introspects the PostgreSQL schema using information_schema and pg_constraint.
func LoadPostgres(ctx context.Context, conn *pgx.Conn, schemaName string) (*Schema, error) {
	s := &Schema{
		Columns:              make(map[string][]Column),
		PrimaryKeys:          make(map[string][]string),
		TableEngines:         make(map[string]string),
		PartitionedTables:    make(map[string]*PartitionedTable),
		ChildToRootPartition: make(map[string]string),
	}

	// Load columns
	colRows, err := conn.Query(ctx, `
		SELECT table_name, column_name, data_type, is_nullable, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1
		ORDER BY table_name, ordinal_position
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var tableName, colName, dataType, isNullable string
		var ordinal int
		if err := colRows.Scan(&tableName, &colName, &dataType, &isNullable, &ordinal); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		s.Columns[tableName] = append(s.Columns[tableName], Column{
			Name:       colName,
			DataType:   dataType,
			IsNullable: isNullable == "YES",
			Ordinal:    ordinal,
		})
	}
	if err := colRows.Err(); err != nil {
		return nil, fmt.Errorf("columns rows: %w", err)
	}

	// Load primary keys
	pkRows, err := conn.Query(ctx, `
		SELECT
			kcu.table_name,
			array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS pk_columns
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON kcu.constraint_name = tc.constraint_name
			AND kcu.table_schema = tc.table_schema
			AND kcu.table_name = tc.table_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
		GROUP BY kcu.table_name
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query primary keys: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var tableName string
		var pkCols []string
		if err := pkRows.Scan(&tableName, &pkCols); err != nil {
			return nil, fmt.Errorf("scan pk: %w", err)
		}
		s.PrimaryKeys[tableName] = pkCols
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("pk rows: %w", err)
	}

	// Load foreign keys via pg_constraint
	fkRows, err := conn.Query(ctx, `
		SELECT
			con.conname,
			src.relname AS from_table,
			array_agg(src_att.attname::text ORDER BY u.pos) AS from_cols,
			dst.relname AS to_table,
			array_agg(dst_att.attname::text ORDER BY u.pos) AS to_cols,
			con.condeferrable,
			con.condeferred
		FROM pg_constraint con
		JOIN pg_class src ON src.oid = con.conrelid
		JOIN pg_class dst ON dst.oid = con.confrelid
		JOIN pg_namespace ns ON ns.oid = src.relnamespace
		CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS u(src_col, dst_col, pos)
		JOIN pg_attribute src_att ON src_att.attrelid = src.oid AND src_att.attnum = u.src_col
		JOIN pg_attribute dst_att ON dst_att.attrelid = dst.oid AND dst_att.attnum = u.dst_col
		WHERE con.contype = 'f'
			AND ns.nspname = $1
		GROUP BY con.conname, src.relname, dst.relname, con.condeferrable, con.condeferred
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query foreign keys: %w", err)
	}
	defer fkRows.Close()

	for fkRows.Next() {
		var fk ForeignKey
		if err := fkRows.Scan(
			&fk.ConstraintName,
			&fk.FromTable,
			&fk.FromColumns,
			&fk.ToTable,
			&fk.ToColumns,
			&fk.Deferrable,
			&fk.InitiallyDeferred,
		); err != nil {
			return nil, fmt.Errorf("scan fk: %w", err)
		}
		s.ForeignKeys = append(s.ForeignKeys, fk)
	}
	if err := fkRows.Err(); err != nil {
		return nil, fmt.Errorf("fk rows: %w", err)
	}

	// Load declarative table partitions (PostgreSQL 10+)
	partRows, err := conn.Query(ctx, `
		SELECT
			parent.relname AS root_table,
			COALESCE(child.relname, '') AS partition_name,
			COALESCE(pg_get_expr(child.relpartbound, child.oid), '') AS partition_bound,
			CASE pt.partstrat
				WHEN 'r' THEN 'RANGE'
				WHEN 'l' THEN 'LIST'
				WHEN 'h' THEN 'HASH'
				ELSE pt.partstrat::text
			END AS partition_strategy,
			COALESCE(pg_get_partkeydef(parent.oid), '') AS partition_key_def
		FROM pg_partitioned_table pt
		JOIN pg_class parent ON parent.oid = pt.partrelid
		JOIN pg_namespace ns ON ns.oid = parent.relnamespace
		LEFT JOIN pg_inherits inh ON inh.inhparent = parent.oid
		LEFT JOIN pg_class child ON child.oid = inh.inhrelid
		WHERE ns.nspname = $1
		ORDER BY parent.relname, child.relname
	`, schemaName)
	if err == nil {
		defer partRows.Close()
		for partRows.Next() {
			var rootTable, partName, partBound, strategy, keyDef string
			if err := partRows.Scan(&rootTable, &partName, &partBound, &strategy, &keyDef); err != nil {
				continue
			}
			pt, exists := s.PartitionedTables[rootTable]
			if !exists {
				pt = &PartitionedTable{
					RootTable: rootTable,
					Strategy:  strategy,
					KeyDef:    keyDef,
				}
				s.PartitionedTables[rootTable] = pt
			}
			if partName != "" {
				pt.Partitions = append(pt.Partitions, PartitionBound{
					PartitionName: partName,
					Bound:         partBound,
				})
				s.ChildToRootPartition[partName] = rootTable
			}
		}

		// Ensure root partitioned table columns are populated from children if information_schema.columns missed them
		for root, pt := range s.PartitionedTables {
			if len(s.Columns[root]) == 0 {
				for _, pb := range pt.Partitions {
					if len(s.Columns[pb.PartitionName]) > 0 {
						s.Columns[root] = s.Columns[pb.PartitionName]
						break
					}
				}
			}
		}
	}

	return s, nil
}

// GeneratePostgresTableDDL emits CREATE TABLE DDL for a table in PostgreSQL,
// including the PARTITION BY clause and child partition creation/attachment if it is a partitioned table.
func GeneratePostgresTableDDL(schema *Schema, table string) string {
	if schema == nil {
		return ""
	}
	cols, exists := schema.Columns[table]
	if !exists || len(cols) == 0 {
		return ""
	}

	var colDefs []string
	for _, col := range cols {
		def := fmt.Sprintf("  %s %s", quotePostgresIdent(col.Name), col.DataType)
		if !col.IsNullable {
			def += " NOT NULL"
		}
		colDefs = append(colDefs, def)
	}

	if pks, ok := schema.PrimaryKeys[table]; ok && len(pks) > 0 {
		quotedPKs := make([]string, len(pks))
		for i, pk := range pks {
			quotedPKs[i] = quotePostgresIdent(pk)
		}
		colDefs = append(colDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quotedPKs, ", ")))
	}

	pt, isPartitioned := schema.PartitionedTables[table]
	if !isPartitioned {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
			quotePostgresIdent(table),
			strings.Join(colDefs, ",\n"),
		)
	}

	// Partitioned table DDL
	partClause := pt.KeyDef
	trimmedUpper := strings.ToUpper(strings.TrimSpace(partClause))
	if !strings.HasPrefix(trimmedUpper, "RANGE") &&
		!strings.HasPrefix(trimmedUpper, "LIST") &&
		!strings.HasPrefix(trimmedUpper, "HASH") {
		partClause = fmt.Sprintf("%s %s", pt.Strategy, pt.KeyDef)
	}

	rootDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) PARTITION BY %s;",
		quotePostgresIdent(table),
		strings.Join(colDefs, ",\n"),
		partClause,
	)

	var childDDLs []string
	for _, pb := range pt.Partitions {
		childDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s %s;",
			quotePostgresIdent(pb.PartitionName),
			quotePostgresIdent(table),
			pb.Bound,
		)
		childDDLs = append(childDDLs, childDDL)
	}

	if len(childDDLs) > 0 {
		return rootDDL + "\n" + strings.Join(childDDLs, "\n")
	}
	return rootDDL
}

func quotePostgresIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
