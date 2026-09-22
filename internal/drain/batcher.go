package drain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/safety"
)

// anyArrayThreshold is the threshold up to which = ANY($1::type[]) parameterized arrays are used.
const anyArrayThreshold = graph.MaxArrayBatchSize

// mysqlChunkSize is the maximum number of items in a single MySQL IN (...) clause.
const mysqlChunkSize = 2000

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

// StratifiedSampling configures proportional child sampling per parent entity.
type StratifiedSampling struct {
	ChildrenPerParent int
	Seed              string
	PKCol             string
}

// queryByIDs selects rows from schema.table where fkCol is in ids, routing according to engine.
// Routing logic:
//   - 0 IDs           → returns nil, nil (no-op)
//   - MySQL           → WHERE col IN ('id1', 'id2', ...) (chunked in batches of 2000)
//   - Postgres <= 10k → WHERE fkCol = ANY($1::type[]) parameterized array
//   - Postgres > 10k  → UNLOGGED temp table + binary COPY + indexed INNER JOIN
func queryByIDs(
	ctx context.Context,
	tx db.SourceTx,
	engine db.EngineType,
	schema, table string,
	colNames []string,
	fkCol string,
	fkColInfo *introspect.Column,
	ids []string,
	limit int,
) (db.RowIterator, error) {
	return queryByIDsWithSampling(ctx, tx, engine, schema, table, colNames, fkCol, fkColInfo, ids, limit, StratifiedSampling{})
}

// queryByIDsWithSampling executes batch queries with optional stratified child sampling using window ranking.
func queryByIDsWithSampling(
	ctx context.Context,
	tx db.SourceTx,
	engine db.EngineType,
	schema, table string,
	colNames []string,
	fkCol string,
	fkColInfo *introspect.Column,
	ids []string,
	limit int,
	sampling StratifiedSampling,
) (db.RowIterator, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	if engine == db.EngineMySQL {
		return queryByIDsMySQLWithSampling(ctx, tx, schema, table, colNames, fkCol, ids, limit, sampling)
	}

	// Postgres path
	if len(ids) > anyArrayThreshold {
		return queryByIDsTempTableWithSampling(ctx, tx, schema, table, colNames, fkCol, ids, limit, sampling)
	}

	arrayType := "text"
	if fkColInfo != nil {
		arrayType = pgArrayType(*fkColInfo)
	}

	var q string
	var args []any
	param := idsToANYParam(ids, arrayType)

	if sampling.ChildrenPerParent > 0 {
		seed := sampling.Seed
		if seed == "" {
			seed = "dbdrain-sampling"
		}
		q = graph.BuildStratifiedChildQuery(schema, table, colNames, fkCol, sampling.PKCol, arrayType, sampling.ChildrenPerParent, limit)
		args = []any{param, seed}
	} else {
		q = buildANYQuery(schema, table, colNames, fkCol, arrayType, limit)
		args = []any{param}
	}

	var it db.RowIterator
	err := safety.ExecuteWithBackoffJitter(ctx, 3, 50*time.Millisecond, func() error {
		var qErr error
		it, qErr = tx.Query(ctx, q, args...)
		return qErr
	})
	return it, err
}

// queryByIDsMySQL executes batch queries for MySQL using IN (...) clauses, chunking if necessary.
func queryByIDsMySQL(
	ctx context.Context,
	tx db.SourceTx,
	schema, table string,
	colNames []string,
	fkCol string,
	ids []string,
	limit int,
) (db.RowIterator, error) {
	return queryByIDsMySQLWithSampling(ctx, tx, schema, table, colNames, fkCol, ids, limit, StratifiedSampling{})
}

// queryByIDsMySQLWithSampling executes batch queries for MySQL with optional stratified child sampling.
func queryByIDsMySQLWithSampling(
	ctx context.Context,
	tx db.SourceTx,
	schema, table string,
	colNames []string,
	fkCol string,
	ids []string,
	limit int,
	sampling StratifiedSampling,
) (db.RowIterator, error) {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = db.QuoteIdent(db.EngineMySQL, c)
	}

	buildChunkQuery := func(chunk []string) (string, []any) {
		if sampling.ChildrenPerParent > 0 {
			placeholders := make([]string, len(chunk))
			args := make([]any, 0, len(chunk)+1)
			for i, id := range chunk {
				placeholders[i] = "?"
				args = append(args, id)
			}
			seed := sampling.Seed
			if seed == "" {
				seed = "dbdrain-sampling"
			}
			args = append(args, seed)
			q := graph.BuildStratifiedChildQueryMySQL(schema, table, colNames, fkCol, sampling.PKCol, strings.Join(placeholders, ", "), sampling.ChildrenPerParent, limit)
			return q, args
		}

		escapedIDs := make([]string, len(chunk))
		for i, id := range chunk {
			escapedIDs[i] = "'" + escapeMySQLString(id) + "'"
		}
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
			strings.Join(quotedCols, ", "),
			db.QuoteTable(db.EngineMySQL, schema, table),
			db.QuoteIdent(db.EngineMySQL, fkCol),
			strings.Join(escapedIDs, ", "),
		)
		if limit > 0 {
			q += fmt.Sprintf(" LIMIT %d", limit)
		}
		return q, nil
	}

	if len(ids) <= mysqlChunkSize {
		chunkSQL, args := buildChunkQuery(ids)
		var it db.RowIterator
		err := safety.ExecuteWithBackoffJitter(ctx, 3, 50*time.Millisecond, func() error {
			var qErr error
			it, qErr = tx.Query(ctx, chunkSQL, args...)
			return qErr
		})
		return it, err
	}

	var iterators []db.RowIterator
	for i := 0; i < len(ids); i += mysqlChunkSize {
		end := i + mysqlChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		var it db.RowIterator
		chunkSQL, args := buildChunkQuery(ids[i:end])
		err := safety.ExecuteWithBackoffJitter(ctx, 3, 50*time.Millisecond, func() error {
			var qErr error
			it, qErr = tx.Query(ctx, chunkSQL, args...)
			return qErr
		})
		if err != nil {
			for _, prev := range iterators {
				prev.Close()
			}
			return nil, err
		}
		iterators = append(iterators, it)
	}

	return &multiRowIterator{iterators: iterators}, nil
}

type multiRowIterator struct {
	iterators []db.RowIterator
	current   int
}

func (m *multiRowIterator) Next() bool {
	for m.current < len(m.iterators) {
		if m.iterators[m.current].Next() {
			return true
		}
		m.current++
	}
	return false
}

func (m *multiRowIterator) Values() ([]any, error) {
	if m.current < len(m.iterators) {
		return m.iterators[m.current].Values()
	}
	return nil, nil
}

func (m *multiRowIterator) Err() error {
	for _, it := range m.iterators {
		if err := it.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiRowIterator) Close() {
	for _, it := range m.iterators {
		it.Close()
	}
}

// queryByIDsTempTable handles >10,000 IDs for Postgres by streaming them into a temporary
// UNLOGGED table via binary COPY and running an INNER JOIN query.
func queryByIDsTempTable(
	ctx context.Context,
	tx db.SourceTx,
	schema, table string,
	colNames []string,
	fkCol string,
	ids []string,
	limit int,
) (db.RowIterator, error) {
	return queryByIDsTempTableWithSampling(ctx, tx, schema, table, colNames, fkCol, ids, limit, StratifiedSampling{})
}

// queryByIDsTempTableWithSampling handles >10,000 IDs for Postgres with optional stratified child sampling.
func queryByIDsTempTableWithSampling(
	ctx context.Context,
	tx db.SourceTx,
	schema, table string,
	colNames []string,
	fkCol string,
	ids []string,
	limit int,
	sampling StratifiedSampling,
) (db.RowIterator, error) {
	pgxProvider, ok := tx.(interface{ PGX() pgx.Tx })
	if !ok {
		// Fallback to standard ANY query if not direct pgx
		arrayType := "text"
		q := buildANYQuery(schema, table, colNames, fkCol, arrayType, limit)
		return tx.Query(ctx, q, ids)
	}
	pgTx := pgxProvider.PGX()

	tmpName := "_dbdrain_k_" + sanitizeIdent(table)

	if _, err := pgTx.Exec(ctx, fmt.Sprintf(
		`CREATE TEMP TABLE IF NOT EXISTS %s (key text PRIMARY KEY) ON COMMIT DROP`,
		quoteIdent(tmpName),
	)); err != nil {
		return nil, fmt.Errorf("create temp key table for %s: %w", table, err)
	}
	if _, err := pgTx.Exec(ctx, fmt.Sprintf(`TRUNCATE %s`, quoteIdent(tmpName))); err != nil {
		return nil, fmt.Errorf("truncate temp key table: %w", err)
	}

	rows := make([][]any, len(ids))
	for i, id := range ids {
		rows[i] = []any{id}
	}
	if _, err := pgTx.CopyFrom(ctx,
		pgx.Identifier{tmpName},
		[]string{"key"},
		pgx.CopyFromRows(rows),
	); err != nil {
		return nil, fmt.Errorf("copy ids into temp table %s: %w", tmpName, err)
	}

	var q string
	var args []any
	if sampling.ChildrenPerParent > 0 {
		seed := sampling.Seed
		if seed == "" {
			seed = "dbdrain-sampling"
		}
		quotedCols := make([]string, len(colNames))
		for i, c := range colNames {
			quotedCols[i] = graph.QuoteIdent(c)
		}
		pkExpr := graph.QuoteIdent(sampling.PKCol)
		if sampling.PKCol == "" {
			pkExpr = graph.QuoteIdent(fkCol)
		}
		limitClause := ""
		if limit > 0 {
			limitClause = fmt.Sprintf(" LIMIT %d", limit)
		}
		q = fmt.Sprintf(`WITH ranked AS (
  SELECT c.*,
         ROW_NUMBER() OVER (
           PARTITION BY c.%s
           ORDER BY MD5(CAST(c.%s AS text) || $1)
         ) AS rn,
         COUNT(*) OVER (PARTITION BY c.%s) AS total_children
  FROM %s.%s c
  INNER JOIN %s k ON k.key = c.%s::text
)
SELECT %s FROM ranked
WHERE rn = 1 OR (rn <= %d AND total_children > 1)%s`,
			graph.QuoteIdent(fkCol),
			pkExpr,
			graph.QuoteIdent(fkCol),
			graph.QuoteIdent(schema),
			graph.QuoteIdent(table),
			graph.QuoteIdent(tmpName),
			graph.QuoteIdent(fkCol),
			strings.Join(quotedCols, ", "),
			sampling.ChildrenPerParent,
			limitClause,
		)
		args = []any{seed}
	} else {
		q = graph.BuildTempTableJoinQuery(schema, table, tmpName, fkCol, colNames, limit)
	}

	var it db.RowIterator
	err := safety.ExecuteWithBackoffJitter(ctx, 3, 50*time.Millisecond, func() error {
		var qErr error
		it, qErr = tx.Query(ctx, q, args...)
		return qErr
	})
	return it, err
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
