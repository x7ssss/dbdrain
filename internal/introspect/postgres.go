package introspect

import (
	"context"
	"fmt"

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
	ConstraintName string
	FromTable      string
	FromColumns    []string
	ToTable        string
	ToColumns      []string
	Deferrable     bool
	InitiallyDeferred bool
}

// Schema holds the full introspected schema for a given schema name.
type Schema struct {
	Columns     map[string][]Column    // table -> columns
	PrimaryKeys map[string][]string    // table -> pk column names
	ForeignKeys []ForeignKey
}

// Load introspects the PostgreSQL schema using information_schema and pg_constraint.
func Load(ctx context.Context, conn *pgx.Conn, schemaName string) (*Schema, error) {
	s := &Schema{
		Columns:     make(map[string][]Column),
		PrimaryKeys: make(map[string][]string),
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
			cc.table_name,
			array_agg(cc.column_name::text ORDER BY cc.ordinal_position) AS pk_columns
		FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage cc
			ON cc.constraint_name = tc.constraint_name
			AND cc.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
		GROUP BY cc.table_name
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

	return s, nil
}
