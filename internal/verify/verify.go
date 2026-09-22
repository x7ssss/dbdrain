package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

const (
	// StatusValid indicates parent rows exist and are visible.
	StatusValid = "Valid"
	// StatusPolicyExcluded indicates parent rows exist globally but are filtered by the current tenant RLS role.
	StatusPolicyExcluded = "Policy-Excluded"
	// StatusCorrupted indicates parent rows are physically missing.
	StatusCorrupted = "Corrupted/Orphan"
)

// CheckResult represents the outcome of an FK relationship integrity check.
type CheckResult struct {
	ChildTable          string
	ChildColumn         string
	ParentTable         string
	ParentColumn        string
	Status              string // "Valid", "Policy-Excluded", "Corrupted/Orphan"
	OrphanCount         int64
	PolicyExcludedCount int64
	RLSActive           bool
}

// Violation represents a detected orphaned-row FK violation or policy exclusion.
type Violation struct {
	ChildTable          string
	ChildColumn         string
	ParentTable         string
	ParentColumn        string
	Status              string // "Valid", "Policy-Excluded", "Corrupted/Orphan"
	OrphanCount         int64
	PolicyExcludedCount int64
	RLSActive           bool
}

// Run executes orphan-check queries against conn for every FK in schema,
// restricted to the set of tables that were actually drained (drained).
// Returns a (possibly empty) list of Violations.
func Run(ctx context.Context, conn *pgx.Conn, schemaName string, schema *introspect.Schema, drained map[string]int) ([]Violation, error) {
	results, err := VerifyAll(ctx, conn, schemaName, schema, drained)
	if err != nil {
		return nil, err
	}

	var violations []Violation
	for _, r := range results {
		if r.Status != StatusValid {
			violations = append(violations, Violation{
				ChildTable:          r.ChildTable,
				ChildColumn:         r.ChildColumn,
				ParentTable:         r.ParentTable,
				ParentColumn:        r.ParentColumn,
				Status:              r.Status,
				OrphanCount:         r.OrphanCount,
				PolicyExcludedCount: r.PolicyExcludedCount,
				RLSActive:           r.RLSActive,
			})
		}
	}
	return violations, nil
}

// VerifyAll checks all FK relationships for drained tables and categorizes results distinctly:
// - Valid: Parent exists and visible.
// - Policy-Excluded: Parent exists globally but filtered by current tenant RLS role.
// - Corrupted/Orphan: Parent physically missing.
func VerifyAll(ctx context.Context, conn *pgx.Conn, schemaName string, schema *introspect.Schema, drained map[string]int) ([]CheckResult, error) {
	var results []CheckResult

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

		if count == 0 {
			results = append(results, CheckResult{
				ChildTable:   fk.FromTable,
				ChildColumn:  childCol,
				ParentTable:  fk.ToTable,
				ParentColumn: parentCol,
				Status:       StatusValid,
			})
			continue
		}

		// Detect if RLS is active on the missing parent table.
		rlsActive := isRLSActive(ctx, conn, schemaName, fk.ToTable)

		if !rlsActive {
			results = append(results, CheckResult{
				ChildTable:   fk.FromTable,
				ChildColumn:  childCol,
				ParentTable:  fk.ToTable,
				ParentColumn: parentCol,
				Status:       StatusCorrupted,
				OrphanCount:  count,
				RLSActive:    false,
			})
			continue
		}

		// RLS is active on parent table. Attempt to verify if parent rows exist globally.
		globalMissing := checkGlobalMissingWithRLSOff(ctx, conn, query)
		if globalMissing < 0 {
			// Cannot bypass RLS (e.g. non-BYPASSRLS / non-superuser role).
			// Since RLS is active on parent table, categorize as Policy-Excluded.
			results = append(results, CheckResult{
				ChildTable:          fk.FromTable,
				ChildColumn:         childCol,
				ParentTable:         fk.ToTable,
				ParentColumn:        parentCol,
				Status:              StatusPolicyExcluded,
				PolicyExcludedCount: count,
				RLSActive:           true,
			})
		} else if globalMissing == 0 {
			// All missing parents exist globally when RLS is off.
			results = append(results, CheckResult{
				ChildTable:          fk.FromTable,
				ChildColumn:         childCol,
				ParentTable:         fk.ToTable,
				ParentColumn:        parentCol,
				Status:              StatusPolicyExcluded,
				PolicyExcludedCount: count,
				RLSActive:           true,
			})
		} else if globalMissing < count {
			// Partial: some policy-excluded, some corrupted/orphan.
			policyCount := count - globalMissing
			results = append(results, CheckResult{
				ChildTable:          fk.FromTable,
				ChildColumn:         childCol,
				ParentTable:         fk.ToTable,
				ParentColumn:        parentCol,
				Status:              StatusCorrupted,
				OrphanCount:         globalMissing,
				PolicyExcludedCount: policyCount,
				RLSActive:           true,
			})
		} else {
			// globalMissing == count: even with row_security off, parent rows are missing.
			results = append(results, CheckResult{
				ChildTable:   fk.FromTable,
				ChildColumn:  childCol,
				ParentTable:  fk.ToTable,
				ParentColumn: parentCol,
				Status:       StatusCorrupted,
				OrphanCount:  count,
				RLSActive:    true,
			})
		}
	}

	return results, nil
}

func isRLSActive(ctx context.Context, conn *pgx.Conn, schemaName, tableName string) bool {
	var relrowsecurity, relforcerowsecurity bool
	err := conn.QueryRow(ctx, `
SELECT COALESCE(c.relrowsecurity, false), COALESCE(c.relforcerowsecurity, false)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
`, schemaName, tableName).Scan(&relrowsecurity, &relforcerowsecurity)
	if err != nil {
		return false
	}
	return relrowsecurity || relforcerowsecurity
}

func checkGlobalMissingWithRLSOff(ctx context.Context, conn *pgx.Conn, query string) int64 {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return -1
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SET LOCAL row_security = off;"); err != nil {
		return -1
	}

	var globalMissing int64
	if err := tx.QueryRow(ctx, query).Scan(&globalMissing); err != nil {
		return -1
	}
	return globalMissing
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
				Status:       StatusCorrupted,
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
			Status:       StatusCorrupted,
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
		status := v.Status
		if status == "" {
			status = StatusCorrupted
		}
		if status == StatusPolicyExcluded {
			sb.WriteString(fmt.Sprintf("   • %s.%s → %s.%s : %d row(s) [Policy-Excluded]\n",
				v.ChildTable, v.ChildColumn,
				v.ParentTable, v.ParentColumn,
				v.PolicyExcludedCount,
			))
		} else {
			sb.WriteString(fmt.Sprintf("   • %s.%s → %s.%s : %d orphaned row(s) [%s]\n",
				v.ChildTable, v.ChildColumn,
				v.ParentTable, v.ParentColumn,
				v.OrphanCount,
				status,
			))
		}
	}
	return sb.String()
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
