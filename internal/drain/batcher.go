package drain

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// anyArrayThreshold is the threshold up to which = ANY($1::type[]) parameterized arrays are used.
const anyArrayThreshold = graph.MaxArrayBatchSize

// pgArrayType returns the PostgreSQL array type name for a column's data type.
func pgArrayType(col introspect.Column) string {
	return graph.PGArrayType(col.DataType)
}

// idsToANYParam converts string IDs to a typed Go slice for parameterized binding.
func idsToANYParam(ids []string, arrayType string) any {
	return graph.FormatANYParam(ids, arrayType)
}

// buildANYQuery returns a SELECT query using WHERE col = ANY($1::type[]) form.
func buildANYQuery(schema, table string, colNames []string, fkCol, arrayType string, limit int) string {
	return graph.BuildANYQuery(schema, table, colNames, fkCol, arrayType, limit)
}

// findColumnByName returns the first Column with the given name, or nil.
func findColumnByName(cols []introspect.Column, name string) *introspect.Column {
	for i := range cols {
		if cols[i].Name == name {
			return &cols[i]
		}
	}
	return nil
}

// queryByIDs selects rows from schema.table where fkCol is in ids.
// Routing logic:
//   - 0 IDs           → returns nil, nil (no-op)
//   - 1–10,000 IDs    → WHERE fkCol = ANY($1::type[]) parameterized array
//   - >10,000 IDs     → UNLOGGED temp table + binary COPY + indexed INNER JOIN
func queryByIDs(
	ctx context.Context,
	tx pgx.Tx,
	schema, table string,
	colNames []string,
	fkCol string,
	fkColInfo *introspect.Column,
	ids []string,
	limit int,
) (pgx.Rows, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	if len(ids) > anyArrayThreshold {
		return queryByIDsTempTable(ctx, tx, schema, table, colNames, fkCol, ids, limit)
	}

	arrayType := "text"
	if fkColInfo != nil {
		arrayType = pgArrayType(*fkColInfo)
	}

	q := buildANYQuery(schema, table, colNames, fkCol, arrayType, limit)
	param := idsToANYParam(ids, arrayType)
	return tx.Query(ctx, q, param)
}

// queryByIDsTempTable handles >10,000 IDs by streaming them into a temporary
// UNLOGGED table via binary COPY and running an INNER JOIN query.
func queryByIDsTempTable(
	ctx context.Context,
	tx pgx.Tx,
	schema, table string,
	colNames []string,
	fkCol string,
	ids []string,
	limit int,
) (pgx.Rows, error) {
	tmpName := "_dbdrain_k_" + sanitizeIdent(table)

	// Create temp key table for this batch
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`CREATE TEMP TABLE IF NOT EXISTS %s (key text PRIMARY KEY) ON COMMIT DROP`,
		quoteIdent(tmpName),
	)); err != nil {
		return nil, fmt.Errorf("create temp key table for %s: %w", table, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`TRUNCATE %s`, quoteIdent(tmpName))); err != nil {
		return nil, fmt.Errorf("truncate temp key table: %w", err)
	}

	// Stream IDs into the temp table via binary COPY
	rows := make([][]any, len(ids))
	for i, id := range ids {
		rows[i] = []any{id}
	}
	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{tmpName},
		[]string{"key"},
		pgx.CopyFromRows(rows),
	); err != nil {
		return nil, fmt.Errorf("copy ids into temp table %s: %w", tmpName, err)
	}

	q := graph.BuildTempTableJoinQuery(schema, table, tmpName, fkCol, colNames, limit)
	return tx.Query(ctx, q)
}

// sanitizeIdent converts an arbitrary string to a safe SQL identifier suffix.
func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
