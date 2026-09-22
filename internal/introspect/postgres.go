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

// CitusTableType represents the distribution category of a table in a Citus cluster.
type CitusTableType string

const (
	// CitusTableDistributed indicates a sharded distributed table.
	CitusTableDistributed CitusTableType = "Distributed"
	// CitusTableReference indicates a reference table replicated to all worker nodes.
	CitusTableReference CitusTableType = "Reference"
	// CitusTableLocal indicates a standard PostgreSQL table residing on the coordinator.
	CitusTableLocal CitusTableType = "Local"
)

// CitusTable holds distribution, colocation, and partitioning metadata for Citus tables.
type CitusTable struct {
	TableName          string         `json:"table_name"`
	TableType          CitusTableType `json:"table_type"`          // "Distributed", "Reference", "Local"
	DistributionColumn string         `json:"distribution_column"` // e.g. "tenant_id"
	DistributionMethod string         `json:"distribution_method"` // "hash", "reference", "range", "append"
	ColocationID       int            `json:"colocation_id"`       // colocation group ID
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
	HasCitus             bool                         // true if Citus extension is installed
	CitusTables          map[string]*CitusTable       // table_name -> Citus distribution metadata
	CitusPhysicalShards  map[string]bool              // set of physical shard table names (e.g. users_102008)
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

// IsCitusDistributed returns true if table is a Citus distributed table.
func (s *Schema) IsCitusDistributed(table string) bool {
	if s == nil || s.CitusTables == nil {
		return false
	}
	ct, ok := s.CitusTables[table]
	return ok && ct.TableType == CitusTableDistributed
}

// IsCitusReference returns true if table is a Citus reference table.
func (s *Schema) IsCitusReference(table string) bool {
	if s == nil || s.CitusTables == nil {
		return false
	}
	ct, ok := s.CitusTables[table]
	return ok && ct.TableType == CitusTableReference
}

// IsPhysicalShard returns true if table is a physical worker shard table (e.g. users_102008)
// that should never be extracted directly, but rather through its coordinator logical relation.
func (s *Schema) IsPhysicalShard(table string) bool {
	if s == nil {
		return false
	}
	if s.CitusPhysicalShards != nil && s.CitusPhysicalShards[table] {
		return true
	}
	// Fallback heuristic: if table ends with _<digits> and the prefix matches a known distributed table
	if s.HasCitus && s.CitusTables != nil {
		for distTable := range s.CitusTables {
			prefix := distTable + "_"
			if strings.HasPrefix(table, prefix) {
				suffix := strings.TrimPrefix(table, prefix)
				if isAllDigits(suffix) {
					return true
				}
			}
		}
	}
	return false
}

// ValidateCitusTopology validates that foreign keys between distributed tables include
// the distribution column and belong to the same colocation group.
func (s *Schema) ValidateCitusTopology() []string {
	if s == nil || !s.HasCitus || len(s.CitusTables) == 0 {
		return nil
	}
	var warnings []string
	for _, fk := range s.ForeignKeys {
		fromCt, fromDist := s.CitusTables[fk.FromTable]
		toCt, toDist := s.CitusTables[fk.ToTable]

		if fromDist && toDist && fromCt.TableType == CitusTableDistributed && toCt.TableType == CitusTableDistributed {
			// 1. Colocation validation: colocated tables must share colocation_id
			if fromCt.ColocationID != 0 && toCt.ColocationID != 0 && fromCt.ColocationID != toCt.ColocationID {
				warnings = append(warnings, fmt.Sprintf(
					"citus topology warning: foreign key %s between distributed tables %s (colocation %d) and %s (colocation %d) spans different colocation groups",
					fk.ConstraintName, fk.FromTable, fromCt.ColocationID, fk.ToTable, toCt.ColocationID,
				))
			}

			// 2. Distribution column validation: FK must include the distribution column
			if fromCt.DistributionColumn != "" && !containsCol(fk.FromColumns, fromCt.DistributionColumn) {
				warnings = append(warnings, fmt.Sprintf(
					"citus topology warning: foreign key %s on table %s does not include distribution column %q",
					fk.ConstraintName, fk.FromTable, fromCt.DistributionColumn,
				))
			}
			if toCt.DistributionColumn != "" && !containsCol(fk.ToColumns, toCt.DistributionColumn) {
				warnings = append(warnings, fmt.Sprintf(
					"citus topology warning: foreign key %s referencing table %s does not include distribution column %q",
					fk.ConstraintName, fk.ToTable, toCt.DistributionColumn,
				))
			}
		}
	}
	return warnings
}

// Load introspects the PostgreSQL schema using information_schema and pg_constraint.
func Load(ctx context.Context, conn *pgx.Conn, schemaName string) (*Schema, error) {
	return LoadPostgres(ctx, conn, schemaName)
}

// LoadPostgres introspects the PostgreSQL schema using information_schema, pg_constraint,
// pg_partitioned_table, and Citus distribution catalogs if present.
func LoadPostgres(ctx context.Context, conn *pgx.Conn, schemaName string) (*Schema, error) {
	s := &Schema{
		Columns:              make(map[string][]Column),
		PrimaryKeys:          make(map[string][]string),
		TableEngines:         make(map[string]string),
		PartitionedTables:    make(map[string]*PartitionedTable),
		ChildToRootPartition: make(map[string]string),
		CitusTables:          make(map[string]*CitusTable),
		CitusPhysicalShards:  make(map[string]bool),
	}

	// 0. Detect Citus extension or coordinator catalogs
	var citusInstalled bool
	_ = conn.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'citus')
		    OR EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'citus_tables')
		    OR EXISTS(SELECT 1 FROM pg_class WHERE relname = 'citus_tables');
	`).Scan(&citusInstalled)
	s.HasCitus = citusInstalled

	if s.HasCitus {
		// Discover Citus distributed and reference tables via citus_tables or pg_dist_partition
		citusRows, err := conn.Query(ctx, `
			SELECT
				c.relname AS table_name,
				CASE
					WHEN t.citus_table_type ILIKE '%reference%' THEN 'Reference'
					ELSE 'Distributed'
				END AS table_type,
				COALESCE(t.distribution_column, '') AS distribution_column,
				CASE
					WHEN t.citus_table_type ILIKE '%reference%' THEN 'reference'
					WHEN COALESCE(p.partmethod, '') = 'r' THEN 'range'
					WHEN COALESCE(p.partmethod, '') = 'a' THEN 'append'
					ELSE 'hash'
				END AS distribution_method,
				COALESCE(t.colocation_id, 0) AS colocation_id
			FROM citus_tables t
			JOIN pg_class c ON c.oid = t.table_name::regclass
			JOIN pg_namespace ns ON ns.oid = c.relnamespace
			LEFT JOIN pg_dist_partition p ON p.logicalrelid = c.oid
			WHERE ns.nspname = $1
		`, schemaName)
		if err != nil {
			// Fallback to pg_dist_partition catalog table
			citusRows, err = conn.Query(ctx, `
				SELECT
					c.relname AS table_name,
					CASE
						WHEN p.partmethod = 'n' THEN 'Reference'
						ELSE 'Distributed'
					END AS table_type,
					COALESCE(pg_get_expr(p.partkey, p.logicalrelid), '') AS distribution_column,
					CASE p.partmethod
						WHEN 'h' THEN 'hash'
						WHEN 'n' THEN 'reference'
						WHEN 'r' THEN 'range'
						WHEN 'a' THEN 'append'
						ELSE 'hash'
					END AS distribution_method,
					COALESCE(p.colocationid, 0) AS colocation_id
				FROM pg_dist_partition p
				JOIN pg_class c ON c.oid = p.logicalrelid
				JOIN pg_namespace ns ON ns.oid = c.relnamespace
				WHERE ns.nspname = $1
			`, schemaName)
		}
		if err == nil {
			defer citusRows.Close()
			for citusRows.Next() {
				var tblName, tblType, distCol, distMethod string
				var colocationID int
				if err := citusRows.Scan(&tblName, &tblType, &distCol, &distMethod, &colocationID); err == nil {
					s.CitusTables[tblName] = &CitusTable{
						TableName:          tblName,
						TableType:          CitusTableType(tblType),
						DistributionColumn: distCol,
						DistributionMethod: distMethod,
						ColocationID:       colocationID,
					}
				}
			}
		}

		// Discover physical worker shards to ensure dbdrain never extracts directly from shards
		shardRows, err := conn.Query(ctx, `
			SELECT DISTINCT c.relname
			FROM pg_dist_shard s
			JOIN pg_class c ON c.relname LIKE ('%_' || s.shardid::text)
		`)
		if err == nil {
			defer shardRows.Close()
			for shardRows.Next() {
				var shardRel string
				if err := shardRows.Scan(&shardRel); err == nil && shardRel != "" {
					s.CitusPhysicalShards[shardRel] = true
				}
			}
		}
	}

	// 1. Load columns (filtering out physical worker shards)
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
		if s.IsPhysicalShard(tableName) {
			continue
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

	// 2. Load primary keys (filtering out physical worker shards)
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
		if s.IsPhysicalShard(tableName) {
			continue
		}
		s.PrimaryKeys[tableName] = pkCols
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("pk rows: %w", err)
	}

	// 3. Load foreign keys via pg_constraint (filtering out physical worker shards)
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
		if s.IsPhysicalShard(fk.FromTable) || s.IsPhysicalShard(fk.ToTable) {
			continue
		}
		s.ForeignKeys = append(s.ForeignKeys, fk)
	}
	if err := fkRows.Err(); err != nil {
		return nil, fmt.Errorf("fk rows: %w", err)
	}

	// 4. Load declarative table partitions (PostgreSQL 10+)
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

	// 5. Categorize remaining tables as Local if Citus is present, and validate topology
	if s.HasCitus {
		for tbl := range s.Columns {
			if _, exists := s.CitusTables[tbl]; !exists && !s.IsChildPartition(tbl) && !s.IsPhysicalShard(tbl) {
				s.CitusTables[tbl] = &CitusTable{
					TableName: tbl,
					TableType: CitusTableLocal,
				}
			}
		}
		s.Warnings = append(s.Warnings, s.ValidateCitusTopology()...)
	}

	return s, nil
}

// GenerateCitusTableDDL returns the Citus distribution DDL for a table
// (e.g. SELECT create_distributed_table(...) or SELECT create_reference_table(...)),
// or empty string if table is not a Citus distributed or reference table.
func GenerateCitusTableDDL(schema *Schema, table string) string {
	if schema == nil || !schema.HasCitus || schema.CitusTables == nil {
		return ""
	}
	ct, ok := schema.CitusTables[table]
	if !ok {
		return ""
	}
	switch ct.TableType {
	case CitusTableReference:
		return fmt.Sprintf("SELECT create_reference_table('%s');", table)
	case CitusTableDistributed:
		if ct.DistributionColumn != "" {
			return fmt.Sprintf("SELECT create_distributed_table('%s', '%s');", table, ct.DistributionColumn)
		}
	}
	return ""
}

// GeneratePostgresTableDDL emits CREATE TABLE DDL for a table in PostgreSQL,
// including PARTITION BY clauses, child partitions, and Citus distribution commands if applicable.
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

	var ddl string
	pt, isPartitioned := schema.PartitionedTables[table]
	if !isPartitioned {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
			quotePostgresIdent(table),
			strings.Join(colDefs, ",\n"),
		)
	} else {
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
			ddl = rootDDL + "\n" + strings.Join(childDDLs, "\n")
		} else {
			ddl = rootDDL
		}
	}

	// Append Citus distribution DDL if applicable
	citusDDL := GenerateCitusTableDDL(schema, table)
	if citusDDL != "" {
		ddl = ddl + "\n" + citusDDL
	}

	return ddl
}

func quotePostgresIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func containsCol(cols []string, target string) bool {
	for _, c := range cols {
		if strings.EqualFold(c, target) {
			return true
		}
	}
	return false
}
