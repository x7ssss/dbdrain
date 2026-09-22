package graph

import (
	"fmt"
	"strconv"
	"strings"
)

// MaxArrayBatchSize is the threshold up to which parameterized array binding
// (WHERE col = ANY($1::type[])) is used. Above this threshold, keys are staged
// in an UNLOGGED scratch table with binary COPY and JOIN to avoid GEQO planner degradation.
const MaxArrayBatchSize = 10_000

// PGArrayType maps PostgreSQL column data types to their corresponding array cast type.
func PGArrayType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "integer", "int", "int4", "smallint", "int2",
		"serial", "bigint", "int8", "bigserial":
		return "bigint"
	case "uuid":
		return "uuid"
	default:
		return "text"
	}
}

// BuildANYQuery constructs a parameterized SELECT query using WHERE col = ANY($1::type[]).
// This preserves prepared statement caching and avoids triggering GEQO planner degradation.
func BuildANYQuery(schema, table string, colNames []string, fkCol, arrayType string, limit int) string {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = QuoteIdent(c)
	}

	q := fmt.Sprintf(
		`SELECT %s FROM %s.%s WHERE %s = ANY($1::%s[])`,
		strings.Join(quotedCols, ", "),
		QuoteIdent(schema),
		QuoteIdent(table),
		QuoteIdent(fkCol),
		arrayType,
	)
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return q
}

// BuildANYQueryWithCondition constructs a parameterized SELECT query using WHERE col = ANY($1::type[])
// and an optional additional SQL predicate condition.
func BuildANYQueryWithCondition(schema, table string, colNames []string, fkCol, arrayType string, limit int, condition string) string {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = QuoteIdent(c)
	}

	q := fmt.Sprintf(
		`SELECT %s FROM %s.%s WHERE %s = ANY($1::%s[])`,
		strings.Join(quotedCols, ", "),
		QuoteIdent(schema),
		QuoteIdent(table),
		QuoteIdent(fkCol),
		arrayType,
	)
	if condition != "" {
		q += fmt.Sprintf(" AND (%s)", condition)
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return q
}

// FormatANYParam converts string IDs into typed Go values suitable for pgx driver encoding.
// For bigint columns, it parses string IDs to int64 slice.
// If any ID cannot be parsed as int64, it safely falls back to a []string slice.
func FormatANYParam(ids []string, arrayType string) any {
	if arrayType == "bigint" {
		out := make([]int64, 0, len(ids))
		for _, id := range ids {
			n, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
			if err != nil {
				return ids
			}
			out = append(out, n)
		}
		return out
	}
	return ids
}

// BuildTempTableJoinQuery constructs the JOIN query used when key sets exceed MaxArrayBatchSize.
func BuildTempTableJoinQuery(schema, table, tempTable, fkCol string, colNames []string, limit int) string {
	aliasedCols := make([]string, len(colNames))
	for i, c := range colNames {
		aliasedCols[i] = "t." + QuoteIdent(c)
	}

	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}

	return fmt.Sprintf(
		`SELECT %s FROM %s.%s t INNER JOIN %s k ON k.key = t.%s::text%s`,
		strings.Join(aliasedCols, ", "),
		QuoteIdent(schema),
		QuoteIdent(table),
		QuoteIdent(tempTable),
		QuoteIdent(fkCol),
		limitClause,
	)
}

// QuoteIdent properly quotes a PostgreSQL identifier.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// BuildStratifiedChildQuery constructs a proportional child sampling query using window ranking.
// When childrenPerParent > 0, it ensures that every parent with at least 1 child retains at least 1 child,
// while capping excess children up to childrenPerParent uniformly.
// Ordering is deterministically hashed using MD5(CAST(c.<pk_col> AS text) || $2).
func BuildStratifiedChildQuery(
	schema, table string,
	colNames []string,
	fkCol, pkCol string,
	arrayType string,
	childrenPerParent int,
	limit int,
) string {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = QuoteIdent(c)
	}

	pkExpr := QuoteIdent(pkCol)
	if pkCol == "" {
		pkExpr = QuoteIdent(fkCol)
	}

	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}

	return fmt.Sprintf(`WITH ranked AS (
  SELECT c.*,
         ROW_NUMBER() OVER (
           PARTITION BY c.%s
           ORDER BY MD5(CAST(c.%s AS text) || $2)
         ) AS rn,
         COUNT(*) OVER (PARTITION BY c.%s) AS total_children
  FROM %s.%s c
  WHERE c.%s = ANY($1::%s[])
)
SELECT %s FROM ranked
WHERE rn = 1 OR (rn <= %d AND total_children > 1)%s`,
		QuoteIdent(fkCol),
		pkExpr,
		QuoteIdent(fkCol),
		QuoteIdent(schema),
		QuoteIdent(table),
		QuoteIdent(fkCol),
		arrayType,
		strings.Join(quotedCols, ", "),
		childrenPerParent,
		limitClause,
	)
}

// BuildStratifiedChildQueryMySQL constructs the equivalent proportional child sampling query for MySQL 8.0+.
func BuildStratifiedChildQueryMySQL(
	schema, table string,
	colNames []string,
	fkCol, pkCol string,
	inPlaceholders string,
	childrenPerParent int,
	limit int,
) string {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
	}

	pkExpr := "`" + strings.ReplaceAll(pkCol, "`", "``") + "`"
	if pkCol == "" {
		pkExpr = "`" + strings.ReplaceAll(fkCol, "`", "``") + "`"
	}

	fkQuoted := "`" + strings.ReplaceAll(fkCol, "`", "``") + "`"
	tableQuoted := "`" + strings.ReplaceAll(table, "`", "``") + "`"
	if schema != "" {
		tableQuoted = "`" + strings.ReplaceAll(schema, "`", "``") + "`.`" + strings.ReplaceAll(table, "`", "``") + "`"
	}

	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}

	return fmt.Sprintf(`WITH ranked AS (
  SELECT c.*,
         ROW_NUMBER() OVER (
           PARTITION BY c.%s
           ORDER BY MD5(CONCAT(CAST(c.%s AS CHAR), ?))
         ) AS rn,
         COUNT(*) OVER (PARTITION BY c.%s) AS total_children
  FROM %s c
  WHERE c.%s IN (%s)
)
SELECT %s FROM ranked
WHERE rn = 1 OR (rn <= %d AND total_children > 1)%s`,
		fkQuoted,
		pkExpr,
		fkQuoted,
		tableQuoted,
		fkQuoted,
		inPlaceholders,
		strings.Join(quotedCols, ", "),
		childrenPerParent,
		limitClause,
	)
}
