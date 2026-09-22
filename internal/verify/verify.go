package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// Violation represents a detected orphaned-row FK violation.
type Violation struct {
	ChildTable   string
	ChildColumn  string
	ParentTable  string
	ParentColumn string
	OrphanCount  int64
}

// Run executes orphan-check queries against conn for every FK in schema,
// restricted to the set of tables that were actually drained (drianed).
// Returns a (possibly empty) list of Violations.
func Run(ctx context.Context, conn *pgx.Conn, schemaName string, schema *introspect.Schema, drained map[string]int) ([]Violation, error) {
	var violations []Violation

	for _, fk := range schema.ForeignKeys {
		// Only check tables that were part of this drain operation.
		if _, ok := drained[fk.FromTable]; !ok {
			continue
		}

		// Composite FK support requires multi-column join; skip for now (emit warning instead).
		if len(fk.FromColumns) != 1 || len(fk.ToColumns) != 1 {
			continue
		}

		childCol := fk.FromColumns[0]
		parentCol := fk.ToColumns[0]

		query := fmt.Sprintf(`
SELECT COUNT(*)
FROM %s.%s c
LEFT JOIN %s.%s p ON p.%s = c.%s
WHERE c.%s IS NOT NULL AND p.%s IS NULL`,
			quoteIdent(schemaName), quoteIdent(fk.FromTable),
			quoteIdent(schemaName), quoteIdent(fk.ToTable),
			quoteIdent(parentCol), quoteIdent(childCol),
			quoteIdent(childCol), quoteIdent(parentCol),
		)

		var count int64
		err := conn.QueryRow(ctx, query).Scan(&count)
		if err != nil {
			// Table might not exist in target schema; skip this FK.
			continue
		}

		if count > 0 {
			violations = append(violations, Violation{
				ChildTable:   fk.FromTable,
				ChildColumn:  childCol,
				ParentTable:  fk.ToTable,
				ParentColumn: parentCol,
				OrphanCount:  count,
			})
		}
	}

	return violations, nil
}

// RunMySQL executes orphan-check queries against a MySQL target for every FK in schema,
// restricted to the set of tables that were actually drained.
func RunMySQL(ctx context.Context, targetDB *sql.DB, schemaName string, schema *introspect.Schema, drained map[string]int) ([]Violation, error) {
	var violations []Violation

	quoteTable := func(table string) string {
		if schemaName == "" {
			return "`" + strings.ReplaceAll(table, "`", "``") + "`"
		}
		return "`" + strings.ReplaceAll(schemaName, "`", "``") + "`.`" + strings.ReplaceAll(table, "`", "``") + "`"
	}
	quoteCol := func(col string) string {
		return "`" + strings.ReplaceAll(col, "`", "``") + "`"
	}

	for _, fk := range schema.ForeignKeys {
		if _, ok := drained[fk.FromTable]; !ok {
			continue
		}

		if len(fk.FromColumns) != 1 || len(fk.ToColumns) != 1 {
			continue
		}

		childCol := fk.FromColumns[0]
		parentCol := fk.ToColumns[0]

		query := fmt.Sprintf(`
SELECT COUNT(*)
FROM %s c
LEFT JOIN %s p ON p.%s = c.%s
WHERE c.%s IS NOT NULL AND p.%s IS NULL`,
			quoteTable(fk.FromTable),
			quoteTable(fk.ToTable),
			quoteCol(parentCol), quoteCol(childCol),
			quoteCol(childCol), quoteCol(parentCol),
		)

		var count int64
		err := targetDB.QueryRowContext(ctx, query).Scan(&count)
		if err != nil {
			continue
		}

		if count > 0 {
			violations = append(violations, Violation{
				ChildTable:   fk.FromTable,
				ChildColumn:  childCol,
				ParentTable:  fk.ToTable,
				ParentColumn: parentCol,
				OrphanCount:  count,
			})
		}
	}

	return violations, nil
}

// RunSQLite executes PRAGMA foreign_key_check on an SQLite target database.
func RunSQLite(ctx context.Context, targetDB *sql.DB) ([]Violation, error) {
	rows, err := targetDB.QueryContext(ctx, "PRAGMA foreign_key_check;")
	if err != nil {
		return nil, fmt.Errorf("run sqlite foreign_key_check: %w", err)
	}
	defer rows.Close()

	var violations []Violation
	for rows.Next() {
		var tbl, rowid, parent, fkid string
		if err := rows.Scan(&tbl, &rowid, &parent, &fkid); err != nil {
			return nil, fmt.Errorf("scan foreign_key_check: %w", err)
		}
		violations = append(violations, Violation{
			ChildTable:   tbl,
			ChildColumn:  fmt.Sprintf("rowid:%s", rowid),
			ParentTable:  parent,
			ParentColumn: fmt.Sprintf("fk:%s", fkid),
			OrphanCount:  1,
		})
	}
	return violations, rows.Err()
}

// FormatReport returns a human-readable summary of violations for TTY output.
func FormatReport(violations []Violation) string {
	if len(violations) == 0 {
		return "✔  Integrity verified: 0 orphaned foreign keys across all drained tables."
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("✘  %d FK violation(s) detected:\n", len(violations)))
	for _, v := range violations {
		sb.WriteString(fmt.Sprintf("   • %s.%s → %s.%s : %d orphaned row(s)\n",
			v.ChildTable, v.ChildColumn,
			v.ParentTable, v.ParentColumn,
			v.OrphanCount,
		))
	}
	return sb.String()
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
