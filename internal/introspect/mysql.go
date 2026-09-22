package introspect

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
)

// RowScanner provides an abstraction over database query row iterators (such as *sql.Rows).
type RowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// DBQuerier abstracts standard database query execution for catalog introspection.
type DBQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// LoadMySQL introspects the MySQL 8.0+ or MariaDB schema using information_schema tables.
func LoadMySQL(ctx context.Context, q DBQuerier, schemaName string) (*Schema, error) {
	s := &Schema{
		Columns:      make(map[string][]Column),
		PrimaryKeys:  make(map[string][]string),
		TableEngines: make(map[string]string),
	}

	// 1. Table storage engines via information_schema.TABLES
	tableRows, err := q.QueryContext(ctx, `
		SELECT TABLE_NAME, COALESCE(ENGINE, '')
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query table storage engines: %w", err)
	}
	defer tableRows.Close()

	engines, warnings, err := ParseTableEngines(tableRows)
	if err != nil {
		return nil, fmt.Errorf("parse table storage engines: %w", err)
	}
	s.TableEngines = engines
	s.Warnings = warnings

	// 2. Columns via information_schema.COLUMNS
	colRows, err := q.QueryContext(ctx, `
		SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME, ORDINAL_POSITION
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer colRows.Close()

	cols, err := ParseColumns(colRows)
	if err != nil {
		return nil, fmt.Errorf("parse columns: %w", err)
	}
	s.Columns = cols

	// 3. Primary keys via TABLE_CONSTRAINTS and KEY_COLUMN_USAGE
	pkRows, err := q.QueryContext(ctx, `
		SELECT tc.TABLE_NAME, kcu.COLUMN_NAME
		FROM information_schema.TABLE_CONSTRAINTS tc
		JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
			AND tc.TABLE_NAME = kcu.TABLE_NAME
			AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		WHERE tc.CONSTRAINT_SCHEMA = ?
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query primary keys: %w", err)
	}
	defer pkRows.Close()

	pks, err := ParsePrimaryKeys(pkRows)
	if err != nil {
		return nil, fmt.Errorf("parse primary keys: %w", err)
	}
	s.PrimaryKeys = pks

	// 4. Foreign keys via REFERENTIAL_CONSTRAINTS and KEY_COLUMN_USAGE
	fkRows, err := q.QueryContext(ctx, `
		SELECT
			rc.CONSTRAINT_NAME,
			rc.TABLE_NAME,
			kcu.COLUMN_NAME,
			rc.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			kcu.ORDINAL_POSITION
		FROM information_schema.REFERENTIAL_CONSTRAINTS rc
		JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
			AND rc.TABLE_NAME = kcu.TABLE_NAME
			AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		WHERE rc.CONSTRAINT_SCHEMA = ?
		ORDER BY rc.TABLE_NAME, rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query foreign keys: %w", err)
	}
	defer fkRows.Close()

	fks, err := ParseForeignKeys(fkRows)
	if err != nil {
		return nil, fmt.Errorf("parse foreign keys: %w", err)
	}
	s.ForeignKeys = fks

	return s, nil
}

// ParseTableEngines parses table engine names and flags non-transactional/non-FK engines (e.g. MyISAM).
func ParseTableEngines(scanner RowScanner) (map[string]string, []string, error) {
	engines := make(map[string]string)
	var warnings []string

	for scanner.Next() {
		var tableName, engine string
		if err := scanner.Scan(&tableName, &engine); err != nil {
			return nil, nil, fmt.Errorf("scan table engine: %w", err)
		}
		engines[tableName] = engine

		engLower := strings.ToLower(engine)
		switch engLower {
		case "myisam", "memory", "heap", "csv", "archive", "blackhole", "federated":
			warn := fmt.Sprintf("table %q uses storage engine %q which lacks transactions and foreign keys", tableName, engine)
			warnings = append(warnings, warn)
			fmt.Fprintf(os.Stderr, "WARNING: %s\n", warn)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("table engine rows: %w", err)
	}
	return engines, warnings, nil
}

// ParseColumns parses column metadata and tracks nullability (IS_NULLABLE = 'YES').
func ParseColumns(scanner RowScanner) (map[string][]Column, error) {
	columns := make(map[string][]Column)

	for scanner.Next() {
		var tableName, colName, dataType, isNullable string
		var ordinal int
		if err := scanner.Scan(&tableName, &colName, &dataType, &isNullable, &ordinal); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		columns[tableName] = append(columns[tableName], Column{
			Name:       colName,
			DataType:   dataType,
			IsNullable: strings.EqualFold(isNullable, "YES"),
			Ordinal:    ordinal,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("column rows: %w", err)
	}
	return columns, nil
}

// ParsePrimaryKeys parses primary key columns preserving ORDINAL_POSITION.
func ParsePrimaryKeys(scanner RowScanner) (map[string][]string, error) {
	pks := make(map[string][]string)

	for scanner.Next() {
		var tableName, colName string
		if err := scanner.Scan(&tableName, &colName); err != nil {
			return nil, fmt.Errorf("scan pk: %w", err)
		}
		pks[tableName] = append(pks[tableName], colName)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("pk rows: %w", err)
	}
	return pks, nil
}

type fkColumnPart struct {
	fromCol string
	toCol   string
	ordinal int
}

// ParseForeignKeys parses foreign key constraints, strictly preserving compound foreign key
// ordering by ORDINAL_POSITION and mapping to referenced tables via REFERENTIAL_CONSTRAINTS.
func ParseForeignKeys(scanner RowScanner) ([]ForeignKey, error) {
	type fkGroupKey struct {
		fromTable      string
		constraintName string
	}

	type fkGroupData struct {
		fromTable      string
		toTable        string
		constraintName string
		parts          []fkColumnPart
	}

	groups := make(map[fkGroupKey]*fkGroupData)
	var groupOrder []fkGroupKey

	for scanner.Next() {
		var constraintName, tableName, colName, refTableName, refColName string
		var ordinal int
		if err := scanner.Scan(&constraintName, &tableName, &colName, &refTableName, &refColName, &ordinal); err != nil {
			return nil, fmt.Errorf("scan foreign key: %w", err)
		}

		key := fkGroupKey{fromTable: tableName, constraintName: constraintName}
		grp, exists := groups[key]
		if !exists {
			grp = &fkGroupData{
				fromTable:      tableName,
				toTable:        refTableName,
				constraintName: constraintName,
			}
			groups[key] = grp
			groupOrder = append(groupOrder, key)
		}

		grp.parts = append(grp.parts, fkColumnPart{
			fromCol: colName,
			toCol:   refColName,
			ordinal: ordinal,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("foreign key rows: %w", err)
	}

	var fks []ForeignKey
	for _, key := range groupOrder {
		grp := groups[key]
		// Strictly preserve compound FK ordering by ORDINAL_POSITION
		sort.SliceStable(grp.parts, func(i, j int) bool {
			return grp.parts[i].ordinal < grp.parts[j].ordinal
		})

		fromCols := make([]string, len(grp.parts))
		toCols := make([]string, len(grp.parts))
		for i, part := range grp.parts {
			fromCols[i] = part.fromCol
			toCols[i] = part.toCol
		}

		fks = append(fks, ForeignKey{
			ConstraintName:    grp.constraintName,
			FromTable:         grp.fromTable,
			FromColumns:       fromCols,
			ToTable:           grp.toTable,
			ToColumns:         toCols,
			Deferrable:        false, // MySQL does not support deferrable foreign keys
			InitiallyDeferred: false,
		})
	}

	return fks, nil
}
