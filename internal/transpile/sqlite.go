package transpile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// MapToSQLiteType maps raw SQL data types to standard SQLite storage affinities.
func MapToSQLiteType(dataType string) string {
	rawType := strings.ToLower(strings.TrimSpace(dataType))

	// Strip length/precision modifiers like varchar(255), decimal(10,2), int(11)
	baseType := rawType
	if idx := strings.Index(baseType, "("); idx != -1 {
		baseType = strings.TrimSpace(baseType[:idx])
	}

	switch {
	// UUID, JSON/JSONB, ENUM, ARRAY, INET -> TEXT
	case baseType == "uuid",
		strings.Contains(baseType, "json"),
		baseType == "enum",
		strings.Contains(baseType, "array") || strings.HasSuffix(baseType, "[]"),
		baseType == "inet",
		baseType == "cidr",
		baseType == "macaddr":
		return "TEXT"

	// TIMESTAMPTZ / DATETIME / TIMESTAMP / DATE / TIME -> TEXT
	case strings.Contains(baseType, "timestamp"),
		strings.Contains(baseType, "datetime"),
		baseType == "date",
		baseType == "time":
		return "TEXT"

	// BOOLEAN -> INTEGER
	case baseType == "boolean", baseType == "bool":
		return "INTEGER"

	// NUMERIC / DECIMAL -> NUMERIC
	case strings.Contains(baseType, "numeric"),
		strings.Contains(baseType, "decimal"):
		return "NUMERIC"

	// Integers
	case baseType == "serial", baseType == "bigserial", baseType == "smallserial",
		baseType == "int", baseType == "integer", baseType == "bigint",
		baseType == "smallint", baseType == "tinyint", baseType == "mediumint",
		baseType == "int4", baseType == "int8", baseType == "int2":
		return "INTEGER"

	// Floating point
	case baseType == "float", baseType == "double", baseType == "real",
		baseType == "double precision", baseType == "float4", baseType == "float8":
		return "REAL"

	// Binary / BLOB
	case baseType == "bytea", baseType == "blob",
		strings.Contains(baseType, "binary"):
		return "BLOB"

	// Text / Character
	case baseType == "text", strings.Contains(baseType, "char"), strings.Contains(baseType, "clob"):
		return "TEXT"

	default:
		return "TEXT"
	}
}

// TranspileColumnDefinition produces the SQLite column definition snippet (e.g. "id" INTEGER PRIMARY KEY AUTOINCREMENT).
func TranspileColumnDefinition(col introspect.Column, isSinglePK bool) string {
	rawType := strings.ToLower(strings.TrimSpace(col.DataType))
	baseType := rawType
	if idx := strings.Index(baseType, "("); idx != -1 {
		baseType = strings.TrimSpace(baseType[:idx])
	}

	quotedName := db.QuoteIdent(db.EngineSQLite, col.Name)

	if isSinglePK {
		switch baseType {
		case "serial", "bigserial", "smallserial", "auto_increment",
			"int", "integer", "bigint", "smallint", "tinyint", "mediumint", "int4", "int8", "int2":
			return fmt.Sprintf("%s INTEGER PRIMARY KEY AUTOINCREMENT", quotedName)
		default:
			// Non-integer PKs (e.g. UUID, varchar, text) -> TEXT PRIMARY KEY (omit AUTOINCREMENT)
			affinity := MapToSQLiteType(col.DataType)
			return fmt.Sprintf("%s %s PRIMARY KEY", quotedName, affinity)
		}
	}

	affinity := MapToSQLiteType(col.DataType)
	def := fmt.Sprintf("%s %s", quotedName, affinity)
	if !col.IsNullable {
		def += " NOT NULL"
	}
	return def
}

// GenerateTableDDL generates the CREATE TABLE statement for a single table in SQLite.
func GenerateTableDDL(schema *introspect.Schema, table string) string {
	cols := schema.Columns[table]
	pks := schema.PrimaryKeys[table]
	isSinglePK := len(pks) == 1

	var elements []string

	for _, col := range cols {
		isColSinglePK := isSinglePK && (col.Name == pks[0])
		elements = append(elements, TranspileColumnDefinition(col, isColSinglePK))
	}

	// Compound primary keys: PRIMARY KEY ("pk1", "pk2")
	if len(pks) > 1 {
		quotedPKs := make([]string, len(pks))
		for i, pk := range pks {
			quotedPKs[i] = db.QuoteIdent(db.EngineSQLite, pk)
		}
		elements = append(elements, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quotedPKs, ", ")))
	}

	// Foreign keys for this table
	for _, fk := range schema.ForeignKeys {
		if fk.FromTable != table {
			continue
		}
		quotedFrom := make([]string, len(fk.FromColumns))
		for i, c := range fk.FromColumns {
			quotedFrom[i] = db.QuoteIdent(db.EngineSQLite, c)
		}
		quotedTo := make([]string, len(fk.ToColumns))
		for i, c := range fk.ToColumns {
			quotedTo[i] = db.QuoteIdent(db.EngineSQLite, c)
		}
		fkClause := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)",
			strings.Join(quotedFrom, ", "),
			db.QuoteTable(db.EngineSQLite, "", fk.ToTable),
			strings.Join(quotedTo, ", "),
		)
		elements = append(elements, fkClause)
	}

	quotedTable := db.QuoteTable(db.EngineSQLite, "", table)
	lines := make([]string, len(elements))
	for i, el := range elements {
		lines[i] = "  " + el
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);", quotedTable, strings.Join(lines, ",\n"))
}

// GenerateSchemaDDL generates the full DDL for the specified tables (or all tables in schema).
func GenerateSchemaDDL(schema *introspect.Schema, tables []string) string {
	if len(tables) == 0 {
		for tbl := range schema.Columns {
			tables = append(tables, tbl)
		}
		sort.Strings(tables)
	}

	var ddlStatements []string
	for _, tbl := range tables {
		if _, exists := schema.Columns[tbl]; exists {
			ddlStatements = append(ddlStatements, GenerateTableDDL(schema, tbl))
		}
	}

	return strings.Join(ddlStatements, "\n\n")
}
