package drain

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/x7ssss/dbdrain/internal/anonymize"
	"github.com/x7ssss/dbdrain/internal/config"
	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/throttle"
)

// SafetyController abstracts background health monitoring and adaptive backpressure.
type SafetyController interface {
	WaitIfPaused(ctx context.Context) error
	IsPaused() bool
	EngineState() string
	PauseReason() string
}

// Anchor defines an extraction root seed query.
type Anchor struct {
	Table string
	Where string
	Limit int
}

// Config holds export configuration.
type Config struct {
	Schema            string
	AnonymizePII      bool
	Salt              string
	MaxRowsPerTable   int                  // 0 = unlimited
	MaxDepth          int                  // 0 = unlimited (downward traversal only)
	Rules             []config.Rule        // declarative masking rules from dbdrain.yaml
	Limiter           *throttle.Limiter
	Safety            SafetyController
	ChildrenPerParent int                  // 0 = unlimited; uniform child quota per parent entity
	SamplingSeed      string               // seed for deterministic hash sampling
	Upstream          bool                 // strict upward-only reverse slicing (anchor is leaf)
	Associations      []config.Association // declarative relationship restrictions
	BypassRLS         bool                 // disable Row-Level Security on PostgreSQL
}

// polyPair holds a (discriminatorValue, fkValue) pair captured during row emission.
type polyPair struct {
	discVal string // e.g. "post"
	fkVal   string // e.g. "123"
}

// Exporter orchestrates database slicing, dependency closure, and data transfer across Postgres and MySQL.
type Exporter struct {
	sourceDB    db.SourceDB
	engine      db.EngineType
	schema      *introspect.Schema
	g           *graph.Graph
	sccs        []graph.SCC
	cfg         Config
	masker      *anonymize.Masker
	visited     map[string]map[string]bool            // table -> set of row PKs
	fkData      map[string]map[string]map[string]bool // table -> col -> set of FK values
	polyData    map[string]map[string][]polyPair
	RowCounts   map[string]int
	resolver    *TwoPhaseResolver
	keysetQueue *graph.KeysetQueue
}

// New creates a new Exporter. It accepts either *pgx.Conn, *sql.DB, or db.SourceDB.
func New(source any, schema *introspect.Schema, g *graph.Graph, sccs []graph.SCC, cfg Config) *Exporter {
	var masker *anonymize.Masker
	if cfg.AnonymizePII {
		masker = anonymize.New(cfg.Salt)
	}

	var sourceDB db.SourceDB
	engine := db.EnginePostgres

	switch s := source.(type) {
	case *pgx.Conn:
		sourceDB = db.NewPostgresSource(s)
		engine = db.EnginePostgres
	case *sql.DB:
		sourceDB = db.NewMySQLSource(s)
		engine = db.EngineMySQL
	case db.SourceDB:
		sourceDB = s
		engine = s.Engine()
	}

	var resolver *TwoPhaseResolver
	if schema != nil && g != nil {
		resolver = NewTwoPhaseResolver(schema, g, sccs, engine, cfg.Schema)
	}

	return &Exporter{
		sourceDB:    sourceDB,
		engine:      engine,
		schema:      schema,
		g:           g,
		sccs:        sccs,
		cfg:         cfg,
		masker:      masker,
		visited:     make(map[string]map[string]bool),
		fkData:      make(map[string]map[string]map[string]bool),
		polyData:    make(map[string]map[string][]polyPair),
		RowCounts:   make(map[string]int),
		resolver:    resolver,
		keysetQueue: graph.NewKeysetQueue(),
	}
}

// hasCycles returns true if any SCC has a cycle.
func (e *Exporter) hasCycles() bool {
	for _, scc := range e.sccs {
		if scc.HasCycle {
			return true
		}
	}
	return false
}

// BeforeChunk acquires worker concurrency and rate limiter tokens,
// and enforces that if the cluster is paused, any open transaction is finished/rolled back
// before sleeping, satisfying the critical production safety rule.
func (e *Exporter) BeforeChunk(ctx context.Context, txPtr *db.SourceTx, count int) error {
	if e.cfg.Limiter != nil {
		if err := e.cfg.Limiter.AcquireWorker(ctx); err != nil {
			return err
		}
		if err := e.cfg.Limiter.Acquire(ctx, count); err != nil {
			e.cfg.Limiter.ReleaseWorker()
			return err
		}
	}

	if e.cfg.Safety != nil && e.cfg.Safety.IsPaused() {
		// CRITICAL RULE: When paused, the extractor must finish or roll back the current bounded chunk;
		// never sleep while holding an open consistent-read transaction.
		if txPtr != nil && *txPtr != nil {
			_ = (*txPtr).Rollback(ctx)
			*txPtr = nil
		}
		if err := e.cfg.Safety.WaitIfPaused(ctx); err != nil {
			if e.cfg.Limiter != nil {
				e.cfg.Limiter.ReleaseWorker()
			}
			return err
		}
	}

	if txPtr != nil && *txPtr == nil && e.sourceDB != nil {
		newTx, err := e.sourceDB.BeginSnapshot(ctx)
		if err != nil {
			if e.cfg.Limiter != nil {
				e.cfg.Limiter.ReleaseWorker()
			}
			return err
		}
		*txPtr = newTx
	}

	return nil
}

// AfterChunk releases worker concurrency, records extracted rows,
// and if cluster health is paused, immediately rolls back the open transaction
// to avoid sleeping while holding an open snapshot.
func (e *Exporter) AfterChunk(ctx context.Context, txPtr *db.SourceTx, rowsCount int) {
	if e.cfg.Limiter != nil {
		e.cfg.Limiter.RecordRows(rowsCount)
		e.cfg.Limiter.ReleaseWorker()
	}

	if e.cfg.Safety != nil && e.cfg.Safety.IsPaused() {
		// CRITICAL RULE: Finish or roll back chunk; never sleep holding open transaction.
		if txPtr != nil && *txPtr != nil {
			_ = (*txPtr).Rollback(ctx)
			*txPtr = nil
		}
	}
}

// -------------------------------------------------------------------------
// SQL emission mode (Writing SQL to file or stdout)
// -------------------------------------------------------------------------

// ExportAnchors writes the full SQL dump to w, starting from the given anchor tables/queries.
func (e *Exporter) ExportAnchors(ctx context.Context, w io.Writer, anchors []Anchor) error {
	fmt.Fprintf(w, "-- Generated by dbdrain v0.9.0 on %s\n", time.Now().UTC().Format(time.RFC3339))
	for _, a := range anchors {
		fmt.Fprintf(w, "-- Anchor: %s WHERE %s LIMIT %d\n", a.Table, a.Where, a.Limit)
	}
	fmt.Fprintln(w, "")

	if e.engine == db.EngineMySQL {
		fmt.Fprintln(w, "START TRANSACTION;")
		fmt.Fprintln(w, "")
	} else {
		fmt.Fprintln(w, "BEGIN;")
		if e.hasCycles() {
			fmt.Fprintln(w, "SET CONSTRAINTS ALL DEFERRED;")
		}
		fmt.Fprintln(w, "")

		// When generating DDL, emit root table creation with partition strategy and recreate child partitions
		if e.schema != nil && len(e.schema.PartitionedTables) > 0 {
			for root := range e.schema.PartitionedTables {
				ddl := introspect.GeneratePostgresTableDDL(e.schema, root)
				if ddl != "" {
					fmt.Fprintln(w, ddl)
					fmt.Fprintln(w, "")
				}
			}
		}
	}

	if e.sourceDB == nil {
		return fmt.Errorf("source database not configured")
	}

	tx, err := e.sourceDB.BeginSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("begin source snapshot: %w", err)
	}
	defer tx.Rollback(ctx)

	rootTables := make([]string, 0, len(anchors))
	for _, a := range anchors {
		if err := e.resolveSelfRef(ctx, tx, w, a.Table, a.Where, a.Limit); err != nil {
			return fmt.Errorf("resolve self-ref (%s): %w", a.Table, err)
		}
		if _, err := e.fetchAndEmit(ctx, tx, w, a.Table, a.Where, a.Limit); err != nil {
			return fmt.Errorf("fetch anchor %s: %w", a.Table, err)
		}
		rootTables = append(rootTables, a.Table)
	}

	if e.cfg.Upstream {
		if err := e.traverseUpstream(ctx, tx, w, rootTables); err != nil {
			return fmt.Errorf("traverse upstream: %w", err)
		}
	} else {
		if err := e.bfsParents(ctx, tx, w, rootTables); err != nil {
			return fmt.Errorf("traverse parents: %w", err)
		}
		if err := e.bfsChildren(ctx, tx, w, rootTables, 0); err != nil {
			return fmt.Errorf("traverse children: %w", err)
		}
	}

	// Two-Phase Phase 2 for MySQL: emit UPDATEs for cyclic foreign keys
	if e.engine == db.EngineMySQL && e.resolver != nil {
		updates := e.resolver.FlushPendingUpdates()
		if len(updates) > 0 {
			fmt.Fprintln(w, "")
			fmt.Fprintln(w, "-- Phase 2: Resolving cyclic foreign keys")
			for _, u := range updates {
				fmt.Fprintln(w, u)
			}
		}
	}

	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "COMMIT;")
	return nil
}

// Export writes the full SQL dump to w, starting from the anchor table/query.
func (e *Exporter) Export(ctx context.Context, w io.Writer, anchorTable string, anchorWhere string, anchorLimit int) error {
	return e.ExportAnchors(ctx, w, []Anchor{{Table: anchorTable, Where: anchorWhere, Limit: anchorLimit}})
}

// -------------------------------------------------------------------------
// Direct target streaming mode: Postgres (Zero-Copy pgx.CopyFromSource with O(1) RAM)
// -------------------------------------------------------------------------

// ExportAnchorsToTarget executes zero-copy streaming hydration directly against targetConn for multiple anchors.
func (e *Exporter) ExportAnchorsToTarget(
	ctx context.Context,
	targetConn *pgx.Conn,
	anchors []Anchor,
	progressFn func(table string, n int),
) error {
	sourceTx, err := e.sourceDB.BeginSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("begin source snapshot: %w", err)
	}
	defer sourceTx.Rollback(ctx)

	targetTx, err := targetConn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin target tx: %w", err)
	}
	defer targetTx.Rollback(ctx)

	if _, err := targetTx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED;"); err != nil {
		return fmt.Errorf("set deferred on target: %w", err)
	}

	// Ensure root partitioned tables and child partitions exist on target before streaming
	if e.schema != nil && len(e.schema.PartitionedTables) > 0 {
		for root := range e.schema.PartitionedTables {
			ddl := introspect.GeneratePostgresTableDDL(e.schema, root)
			if ddl != "" {
				if _, err := targetTx.Exec(ctx, ddl); err != nil {
					// Partition tables might already exist on target; continue
				}
			}
		}
	}

	rootTables := make([]string, 0, len(anchors))
	for _, a := range anchors {
		if err := e.streamSelfRefToTarget(ctx, sourceTx, targetTx, a.Table, a.Where, a.Limit, progressFn); err != nil {
			return fmt.Errorf("stream self-ref to target (%s): %w", a.Table, err)
		}

		if err := e.streamAnchorToTarget(ctx, sourceTx, targetTx, a.Table, a.Where, a.Limit, progressFn); err != nil {
			return fmt.Errorf("stream anchor to target (%s): %w", a.Table, err)
		}
		rootTables = append(rootTables, a.Table)
	}

	if e.cfg.Upstream {
		if err := e.traverseUpstreamToTarget(ctx, sourceTx, targetTx, rootTables, progressFn); err != nil {
			return fmt.Errorf("stream upstream to target: %w", err)
		}
	} else {
		if err := e.bfsParentsToTarget(ctx, sourceTx, targetTx, rootTables, progressFn); err != nil {
			return fmt.Errorf("stream parents to target: %w", err)
		}

		if err := e.bfsChildrenToTarget(ctx, sourceTx, targetTx, rootTables, progressFn); err != nil {
			return fmt.Errorf("stream children to target: %w", err)
		}
	}

	return targetTx.Commit(ctx)
}

// ExportToTarget executes zero-copy streaming hydration directly against targetConn.
func (e *Exporter) ExportToTarget(
	ctx context.Context,
	targetConn *pgx.Conn,
	anchorTable string,
	anchorWhere string,
	anchorLimit int,
	progressFn func(table string, n int),
) error {
	return e.ExportAnchorsToTarget(ctx, targetConn, []Anchor{{Table: anchorTable, Where: anchorWhere, Limit: anchorLimit}}, progressFn)
}

func (e *Exporter) streamToTarget(
	ctx context.Context,
	rows db.RowIterator,
	targetTx pgx.Tx,
	table string,
	progressFn func(table string, n int),
) error {
	if rows == nil {
		return nil
	}
	defer rows.Close()

	cols := e.schema.Columns[table]
	if len(cols) == 0 {
		return nil
	}
	colNames := colNameSlice(cols)
	pkCols := e.schema.PrimaryKeys[table]

	src := NewCursorCopySource(rows, table, cols, colNames, pkCols, e, progressFn)
	defer src.Close()

	_, err := targetTx.CopyFrom(
		ctx,
		pgx.Identifier{e.cfg.Schema, table},
		colNames,
		src,
	)
	if err != nil {
		return fmt.Errorf("copy into %s: %w", table, err)
	}
	return nil
}

func (e *Exporter) streamAnchorToTarget(
	ctx context.Context,
	sourceTx db.SourceTx,
	targetTx pgx.Tx,
	table string,
	where string,
	limit int,
	progressFn func(table string, n int),
) error {
	cols := e.schema.Columns[table]
	if len(cols) == 0 {
		return nil
	}
	colNames := colNameSlice(cols)
	query := e.buildQuery(e.cfg.Schema, table, colNames, where, limit)

	rows, err := sourceTx.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query anchor %s: %w", table, err)
	}

	return e.streamToTarget(ctx, rows, targetTx, table, progressFn)
}

func (e *Exporter) streamSelfRefToTarget(
	ctx context.Context,
	sourceTx db.SourceTx,
	targetTx pgx.Tx,
	table string,
	anchorWhere string,
	limit int,
	progressFn func(table string, n int),
) error {
	edge := e.selfRefEdge(table)
	if edge == nil || len(edge.FromColumns) != 1 || len(edge.ToColumns) != 1 {
		return nil
	}
	childCol, parentCol := edge.FromColumns[0], edge.ToColumns[0]
	cols := e.schema.Columns[table]
	if len(cols) == 0 {
		return nil
	}
	colNames := colNameSlice(cols)
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = "t." + db.QuoteIdent(e.engine, c)
	}
	selectCols := strings.Join(quotedCols, ", ")
	seedWhere, seedLimit := buildSeedClauses(anchorWhere, limit)

	cteSQL := fmt.Sprintf(`
WITH RECURSIVE hierarchy AS (
  SELECT %s FROM %s t %s %s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
)
SELECT DISTINCT * FROM hierarchy`,
		selectCols, db.QuoteTable(e.engine, e.cfg.Schema, table), seedWhere, seedLimit,
		selectCols, db.QuoteTable(e.engine, e.cfg.Schema, table), db.QuoteIdent(e.engine, parentCol), db.QuoteIdent(e.engine, childCol),
		selectCols, db.QuoteTable(e.engine, e.cfg.Schema, table), db.QuoteIdent(e.engine, childCol), db.QuoteIdent(e.engine, parentCol),
	)

	rows, err := sourceTx.Query(ctx, cteSQL)
	if err != nil {
		return fmt.Errorf("recursive CTE for %s: %w", table, err)
	}

	return e.streamToTarget(ctx, rows, targetTx, table, progressFn)
}

func (e *Exporter) bfsParentsToTarget(
	ctx context.Context,
	sourceTx db.SourceTx,
	targetTx pgx.Tx,
	rootTables []string,
	progressFn func(table string, n int),
) error {
	queue := make([]string, 0, len(rootTables))
	enqueued := make(map[string]bool, len(rootTables))
	for _, t := range rootTables {
		if !enqueued[t] {
			enqueued[t] = true
			queue = append(queue, t)
		}
	}

	for len(queue) > 0 {
		table := queue[0]
		queue = queue[1:]

		for _, edge := range e.g.OutEdges[table] {
			parent := edge.ToTable
			if parent == table || len(edge.ToColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(table, parent)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			fkVals := e.getFKValuesForEdge(table, edge)
			if len(fkVals) == 0 {
				continue
			}

			parentCols := e.schema.Columns[parent]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, edge.ToColumns[0])
			condition := ""
			if assoc != nil {
				condition = assoc.Condition()
			}

			rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
			if err != nil {
				return fmt.Errorf("query parents of %s: %w", parent, err)
			}

			if err := e.streamToTarget(ctx, rows, targetTx, parent, progressFn); err != nil {
				return err
			}

			if !enqueued[parent] {
				enqueued[parent] = true
				queue = append(queue, parent)
			}
		}

		// Virtual FK (polymorphic) parents
		for _, ve := range e.g.VirtualEdges[table] {
			grouped := map[string][]string{}
			for _, pp := range e.polyData[table][ve.DiscriminatorCol] {
				grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
			}
			for discVal, fkVals := range grouped {
				tc, ok := ve.Mappings[discVal]
				if !ok {
					continue
				}
				parentCols := e.schema.Columns[tc.Table]
				colNames := colNameSlice(parentCols)
				colInfo := findColumnByName(parentCols, tc.Column)

				rows, err := queryByIDs(ctx, sourceTx, e.engine, e.cfg.Schema, tc.Table, colNames, tc.Column, colInfo, fkVals, 0)
				if err != nil {
					return fmt.Errorf("virtual FK query (%s→%s): %w", table, tc.Table, err)
				}

				if err := e.streamToTarget(ctx, rows, targetTx, tc.Table, progressFn); err != nil {
					return err
				}

				if !enqueued[tc.Table] {
					enqueued[tc.Table] = true
					queue = append(queue, tc.Table)
				}
			}
		}
	}
	return nil
}

func (e *Exporter) bfsChildrenToTarget(
	ctx context.Context,
	sourceTx db.SourceTx,
	targetTx pgx.Tx,
	rootTables []string,
	progressFn func(table string, n int),
) error {
	type work struct {
		table string
		depth int
	}
	queue := make([]work, 0, len(rootTables))
	enqueued := make(map[string]bool, len(rootTables))
	for _, t := range rootTables {
		if !enqueued[t] {
			enqueued[t] = true
			queue = append(queue, work{table: t, depth: 0})
		}
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if e.cfg.MaxDepth > 0 && item.depth >= e.cfg.MaxDepth {
			continue
		}

		for _, edge := range e.g.InEdges[item.table] {
			child := edge.FromTable
			if child == item.table || len(edge.FromColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(child, item.table)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			fkVals := e.visitedPKs(item.table)
			if len(fkVals) == 0 {
				continue
			}

			childCols := e.schema.Columns[child]
			colNames := colNameSlice(childCols)
			colInfo := findColumnByName(childCols, edge.FromColumns[0])

			sampling := e.samplingConfig(child, edge.FromColumns[0])
			rows, err := queryByIDsWithSampling(ctx, sourceTx, e.engine, e.cfg.Schema, child, colNames, edge.FromColumns[0], colInfo, fkVals, e.cfg.MaxRowsPerTable, sampling)
			if err != nil {
				return fmt.Errorf("query children %s: %w", child, err)
			}

			if err := e.streamToTarget(ctx, rows, targetTx, child, progressFn); err != nil {
				return err
			}

			if !enqueued[child] {
				enqueued[child] = true
				queue = append(queue, work{child, item.depth + 1})
			}
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Reverse Subsetting & Upstream Ancestry Traversal
// -------------------------------------------------------------------------

func (e *Exporter) traverseUpstream(ctx context.Context, tx db.SourceTx, w io.Writer, rootTables []string) error {
	var walk func(current string, visitedPath []string) error
	walk = func(current string, visitedPath []string) error {
		for _, edge := range e.g.OutEdges[current] {
			parent := edge.ToTable
			if parent == current || len(edge.ToColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(current, parent)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			if containsPath(visitedPath, parent) {
				continue
			}

			fkVals := e.getFKValuesForEdge(current, edge)
			if len(fkVals) == 0 {
				continue
			}

			parentCols := e.schema.Columns[parent]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, edge.ToColumns[0])
			condition := ""
			if assoc != nil {
				condition = assoc.Condition()
			}

			rows, err := queryByIDsWithCondition(ctx, tx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
			if err != nil {
				return fmt.Errorf("query upstream parent (%s): %w", parent, err)
			}

			pks, err := e.emitFromRows(rows, w, parent)
			if err != nil {
				return err
			}

			if len(pks) > 0 {
				if err := walk(parent, append(visitedPath, parent)); err != nil {
					return err
				}
			}
		}

		// Virtual FK parents
		for _, ve := range e.g.VirtualEdges[current] {
			grouped := map[string][]string{}
			for _, pp := range e.polyData[current][ve.DiscriminatorCol] {
				grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
			}
			for discVal, fkVals := range grouped {
				tc, ok := ve.Mappings[discVal]
				if !ok {
					continue
				}
				parent := tc.Table
				assoc := e.findAssociation(current, parent)
				if assoc != nil && assoc.IsSkipped() {
					continue
				}
				if containsPath(visitedPath, parent) {
					continue
				}

				parentCols := e.schema.Columns[parent]
				colNames := colNameSlice(parentCols)
				colInfo := findColumnByName(parentCols, tc.Column)
				condition := ""
				if assoc != nil {
					condition = assoc.Condition()
				}

				rows, err := queryByIDsWithCondition(ctx, tx, e.engine, e.cfg.Schema, parent, colNames, tc.Column, colInfo, fkVals, 0, condition)
				if err != nil {
					return fmt.Errorf("query upstream virtual parent (%s): %w", parent, err)
				}

				pks, err := e.emitFromRows(rows, w, parent)
				if err != nil {
					return err
				}

				if len(pks) > 0 {
					if err := walk(parent, append(visitedPath, parent)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	for _, root := range rootTables {
		if err := walk(root, []string{root}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) traverseUpstreamToTarget(
	ctx context.Context,
	sourceTx db.SourceTx,
	targetTx pgx.Tx,
	rootTables []string,
	progressFn func(table string, n int),
) error {
	var walk func(current string, visitedPath []string) error
	walk = func(current string, visitedPath []string) error {
		for _, edge := range e.g.OutEdges[current] {
			parent := edge.ToTable
			if parent == current || len(edge.ToColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(current, parent)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			if containsPath(visitedPath, parent) {
				continue
			}

			fkVals := e.getFKValuesForEdge(current, edge)
			if len(fkVals) == 0 {
				continue
			}

			parentCols := e.schema.Columns[parent]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, edge.ToColumns[0])
			condition := ""
			if assoc != nil {
				condition = assoc.Condition()
			}

			rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
			if err != nil {
				return fmt.Errorf("query upstream parent (%s): %w", parent, err)
			}

			prevCount := e.RowCounts[parent]
			if err := e.streamToTarget(ctx, rows, targetTx, parent, progressFn); err != nil {
				return err
			}

			if e.RowCounts[parent] > prevCount {
				if err := walk(parent, append(visitedPath, parent)); err != nil {
					return err
				}
			}
		}

		for _, ve := range e.g.VirtualEdges[current] {
			grouped := map[string][]string{}
			for _, pp := range e.polyData[current][ve.DiscriminatorCol] {
				grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
			}
			for discVal, fkVals := range grouped {
				tc, ok := ve.Mappings[discVal]
				if !ok {
					continue
				}
				parent := tc.Table
				assoc := e.findAssociation(current, parent)
				if assoc != nil && assoc.IsSkipped() {
					continue
				}
				if containsPath(visitedPath, parent) {
					continue
				}

				parentCols := e.schema.Columns[parent]
				colNames := colNameSlice(parentCols)
				colInfo := findColumnByName(parentCols, tc.Column)
				condition := ""
				if assoc != nil {
					condition = assoc.Condition()
				}

				rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, tc.Column, colInfo, fkVals, 0, condition)
				if err != nil {
					return fmt.Errorf("query upstream virtual parent (%s): %w", parent, err)
				}

				prevCount := e.RowCounts[parent]
				if err := e.streamToTarget(ctx, rows, targetTx, parent, progressFn); err != nil {
					return err
				}

				if e.RowCounts[parent] > prevCount {
					if err := walk(parent, append(visitedPath, parent)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	for _, root := range rootTables {
		if err := walk(root, []string{root}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) traverseUpstreamToMySQL(
	ctx context.Context,
	sourceTx db.SourceTx,
	streamRowsToMySQL func(table string, rows db.RowIterator) error,
	rootTables []string,
) error {
	var walk func(current string, visitedPath []string) error
	walk = func(current string, visitedPath []string) error {
		for _, edge := range e.g.OutEdges[current] {
			parent := edge.ToTable
			if parent == current || len(edge.ToColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(current, parent)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			if containsPath(visitedPath, parent) {
				continue
			}

			fkVals := e.getFKValuesForEdge(current, edge)
			if len(fkVals) == 0 {
				continue
			}

			parentCols := e.schema.Columns[parent]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, edge.ToColumns[0])
			condition := ""
			if assoc != nil {
				condition = assoc.Condition()
			}

			rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
			if err != nil {
				return fmt.Errorf("query upstream parent (%s): %w", parent, err)
			}

			prevCount := e.RowCounts[parent]
			if err := streamRowsToMySQL(parent, rows); err != nil {
				return err
			}

			if e.RowCounts[parent] > prevCount {
				if err := walk(parent, append(visitedPath, parent)); err != nil {
					return err
				}
			}
		}

		for _, ve := range e.g.VirtualEdges[current] {
			grouped := map[string][]string{}
			for _, pp := range e.polyData[current][ve.DiscriminatorCol] {
				grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
			}
			for discVal, fkVals := range grouped {
				tc, ok := ve.Mappings[discVal]
				if !ok {
					continue
				}
				parent := tc.Table
				assoc := e.findAssociation(current, parent)
				if assoc != nil && assoc.IsSkipped() {
					continue
				}
				if containsPath(visitedPath, parent) {
					continue
				}

				parentCols := e.schema.Columns[parent]
				colNames := colNameSlice(parentCols)
				colInfo := findColumnByName(parentCols, tc.Column)
				condition := ""
				if assoc != nil {
					condition = assoc.Condition()
				}

				rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, tc.Column, colInfo, fkVals, 0, condition)
				if err != nil {
					return fmt.Errorf("query upstream virtual parent (%s): %w", parent, err)
				}

				prevCount := e.RowCounts[parent]
				if err := streamRowsToMySQL(parent, rows); err != nil {
					return err
				}

				if e.RowCounts[parent] > prevCount {
					if err := walk(parent, append(visitedPath, parent)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	for _, root := range rootTables {
		if err := walk(root, []string{root}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) traverseUpstreamToSQLite(
	ctx context.Context,
	sourceTx db.SourceTx,
	streamRowsToSQLite func(table string, rows db.RowIterator) error,
	rootTables []string,
) error {
	var walk func(current string, visitedPath []string) error
	walk = func(current string, visitedPath []string) error {
		for _, edge := range e.g.OutEdges[current] {
			parent := edge.ToTable
			if parent == current || len(edge.ToColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(current, parent)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			if containsPath(visitedPath, parent) {
				continue
			}

			fkVals := e.getFKValuesForEdge(current, edge)
			if len(fkVals) == 0 {
				continue
			}

			parentCols := e.schema.Columns[parent]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, edge.ToColumns[0])
			condition := ""
			if assoc != nil {
				condition = assoc.Condition()
			}

			rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
			if err != nil {
				return fmt.Errorf("query upstream parent (%s): %w", parent, err)
			}

			prevCount := e.RowCounts[parent]
			if err := streamRowsToSQLite(parent, rows); err != nil {
				return err
			}

			if e.RowCounts[parent] > prevCount {
				if err := walk(parent, append(visitedPath, parent)); err != nil {
					return err
				}
			}
		}

		for _, ve := range e.g.VirtualEdges[current] {
			grouped := map[string][]string{}
			for _, pp := range e.polyData[current][ve.DiscriminatorCol] {
				grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
			}
			for discVal, fkVals := range grouped {
				tc, ok := ve.Mappings[discVal]
				if !ok {
					continue
				}
				parent := tc.Table
				assoc := e.findAssociation(current, parent)
				if assoc != nil && assoc.IsSkipped() {
					continue
				}
				if containsPath(visitedPath, parent) {
					continue
				}

				parentCols := e.schema.Columns[parent]
				colNames := colNameSlice(parentCols)
				colInfo := findColumnByName(parentCols, tc.Column)
				condition := ""
				if assoc != nil {
					condition = assoc.Condition()
				}

				rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, tc.Column, colInfo, fkVals, 0, condition)
				if err != nil {
					return fmt.Errorf("query upstream virtual parent (%s): %w", parent, err)
				}

				prevCount := e.RowCounts[parent]
				if err := streamRowsToSQLite(parent, rows); err != nil {
					return err
				}

				if e.RowCounts[parent] > prevCount {
					if err := walk(parent, append(visitedPath, parent)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	for _, root := range rootTables {
		if err := walk(root, []string{root}); err != nil {
			return err
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Direct target streaming mode: MySQL (Two-Phase Insert Resolution)
// -------------------------------------------------------------------------

// ExportAnchorsToTargetMySQL executes streaming hydration directly against a target MySQL instance
// using Two-Phase resolution for circular FK dependencies across multiple anchors.
func (e *Exporter) ExportAnchorsToTargetMySQL(
	ctx context.Context,
	targetDB *sql.DB,
	targetSchema string,
	anchors []Anchor,
	progressFn func(table string, n int),
) error {
	sourceTx, err := e.sourceDB.BeginSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("begin source snapshot: %w", err)
	}
	defer sourceTx.Rollback(ctx)

	targetConn, err := targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire target connection: %w", err)
	}
	defer targetConn.Close()

	targetTx, err := targetConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	targetResolver := NewTwoPhaseResolver(e.schema, e.g, e.sccs, db.EngineMySQL, targetSchema)

	streamRowsToMySQL := func(table string, rows db.RowIterator) error {
		if rows == nil {
			return nil
		}
		defer rows.Close()

		cols := e.schema.Columns[table]
		if len(cols) == 0 {
			return nil
		}
		colNames := colNameSlice(cols)
		pkCols := e.schema.PrimaryKeys[table]

		for rows.Next() {
			rawVals, err := rows.Values()
			if err != nil {
				return fmt.Errorf("row values %s: %w", table, err)
			}
			pk := buildPK(pkCols, colNames, rawVals)
			if e.markVisited(table, pk) {
				continue
			}

			e.applyMasking(table, cols, rawVals)
			e.coerceDanglingNulls(table, cols, rawVals)
			e.recordFKValues(table, cols, rawVals)
			e.capturePolyData(table, colNames, rawVals)

			stmt, updates := targetResolver.GeneratePhase1Insert(table, cols, rawVals)
			if _, err := targetTx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("target insert %s: %w", table, err)
			}
			targetResolver.RecordPendingUpdates(updates)

			e.RowCounts[table]++
			if progressFn != nil {
				progressFn(table, 1)
			}
		}
		return rows.Err()
	}

	rootTables := make([]string, 0, len(anchors))
	for _, a := range anchors {
		// 1. Stream self-referencing hierarchy if present
		if e.isSelfRef(a.Table) {
			edge := e.selfRefEdge(a.Table)
			if edge != nil && len(edge.FromColumns) == 1 && len(edge.ToColumns) == 1 {
				childCol, parentCol := edge.FromColumns[0], edge.ToColumns[0]
				cols := e.schema.Columns[a.Table]
				colNames := colNameSlice(cols)
				quotedCols := make([]string, len(colNames))
				for i, c := range colNames {
					quotedCols[i] = "t." + db.QuoteIdent(e.engine, c)
				}
				selectCols := strings.Join(quotedCols, ", ")
				seedWhere, seedLimit := buildSeedClauses(a.Where, a.Limit)

				cteSQL := fmt.Sprintf(`
WITH RECURSIVE hierarchy AS (
  SELECT %s FROM %s t %s %s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
)
SELECT DISTINCT * FROM hierarchy`,
					selectCols, db.QuoteTable(e.engine, e.cfg.Schema, a.Table), seedWhere, seedLimit,
					selectCols, db.QuoteTable(e.engine, e.cfg.Schema, a.Table), db.QuoteIdent(e.engine, parentCol), db.QuoteIdent(e.engine, childCol),
					selectCols, db.QuoteTable(e.engine, e.cfg.Schema, a.Table), db.QuoteIdent(e.engine, childCol), db.QuoteIdent(e.engine, parentCol),
				)

				rows, err := sourceTx.Query(ctx, cteSQL)
				if err != nil {
					return fmt.Errorf("recursive CTE %s: %w", a.Table, err)
				}
				if err := streamRowsToMySQL(a.Table, rows); err != nil {
					return err
				}
			}
		}

		// 2. Stream anchor seed rows
		cols := e.schema.Columns[a.Table]
		if len(cols) > 0 {
			colNames := colNameSlice(cols)
			query := e.buildQuery(e.cfg.Schema, a.Table, colNames, a.Where, a.Limit)
			rows, err := sourceTx.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("query anchor %s: %w", a.Table, err)
			}
			if err := streamRowsToMySQL(a.Table, rows); err != nil {
				return err
			}
		}
		rootTables = append(rootTables, a.Table)
	}

	if e.cfg.Upstream {
		if err := e.traverseUpstreamToMySQL(ctx, sourceTx, streamRowsToMySQL, rootTables); err != nil {
			return fmt.Errorf("stream upstream to mysql: %w", err)
		}
	} else {
		// 3. Upward BFS traversal: stream mandatory parents
		{
			queue := make([]string, 0, len(rootTables))
			enqueued := make(map[string]bool, len(rootTables))
			for _, t := range rootTables {
				if !enqueued[t] {
					enqueued[t] = true
					queue = append(queue, t)
				}
			}

			for len(queue) > 0 {
				table := queue[0]
				queue = queue[1:]

				for _, edge := range e.g.OutEdges[table] {
					parent := edge.ToTable
					if parent == table || len(edge.ToColumns) != 1 {
						continue
					}
					assoc := e.findAssociation(table, parent)
					if assoc != nil && assoc.IsSkipped() {
						continue
					}
					fkVals := e.getFKValuesForEdge(table, edge)
					if len(fkVals) == 0 {
						continue
					}

					parentCols := e.schema.Columns[parent]
					colNames := colNameSlice(parentCols)
					colInfo := findColumnByName(parentCols, edge.ToColumns[0])
					condition := ""
					if assoc != nil {
						condition = assoc.Condition()
					}

					rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
					if err != nil {
						return fmt.Errorf("query parents of %s: %w", parent, err)
					}
					if err := streamRowsToMySQL(parent, rows); err != nil {
						return err
					}

					if !enqueued[parent] {
						enqueued[parent] = true
						queue = append(queue, parent)
					}
				}

				// Virtual FK polymorphic parents
				for _, ve := range e.g.VirtualEdges[table] {
					grouped := map[string][]string{}
					for _, pp := range e.polyData[table][ve.DiscriminatorCol] {
						grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
					}
					for discVal, fkVals := range grouped {
						tc, ok := ve.Mappings[discVal]
						if !ok {
							continue
						}
						parentCols := e.schema.Columns[tc.Table]
						colNames := colNameSlice(parentCols)
						colInfo := findColumnByName(parentCols, tc.Column)

						rows, err := queryByIDs(ctx, sourceTx, e.engine, e.cfg.Schema, tc.Table, colNames, tc.Column, colInfo, fkVals, 0)
						if err != nil {
							return fmt.Errorf("virtual FK query (%s→%s): %w", table, tc.Table, err)
						}
						if err := streamRowsToMySQL(tc.Table, rows); err != nil {
							return err
						}

						if !enqueued[tc.Table] {
							enqueued[tc.Table] = true
							queue = append(queue, tc.Table)
						}
					}
				}
			}
		}

		// 4. Downward BFS traversal: stream child tables
		{
			type work struct {
				table string
				depth int
			}
			queue := make([]work, 0, len(rootTables))
			enqueued := make(map[string]bool, len(rootTables))
			for _, t := range rootTables {
				if !enqueued[t] {
					enqueued[t] = true
					queue = append(queue, work{table: t, depth: 0})
				}
			}

			for len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]

				if e.cfg.MaxDepth > 0 && item.depth >= e.cfg.MaxDepth {
					continue
				}

				for _, edge := range e.g.InEdges[item.table] {
					child := edge.FromTable
					if child == item.table || len(edge.FromColumns) != 1 {
						continue
					}
					assoc := e.findAssociation(child, item.table)
					if assoc != nil && assoc.IsSkipped() {
						continue
					}
					fkVals := e.visitedPKs(item.table)
					if len(fkVals) == 0 {
						continue
					}

					childCols := e.schema.Columns[child]
					colNames := colNameSlice(childCols)
					colInfo := findColumnByName(childCols, edge.FromColumns[0])

					sampling := e.samplingConfig(child, edge.FromColumns[0])
					rows, err := queryByIDsWithSampling(ctx, sourceTx, e.engine, e.cfg.Schema, child, colNames, edge.FromColumns[0], colInfo, fkVals, e.cfg.MaxRowsPerTable, sampling)
					if err != nil {
						return fmt.Errorf("query children %s: %w", child, err)
					}
					if err := streamRowsToMySQL(child, rows); err != nil {
						return err
					}

					if !enqueued[child] {
						enqueued[child] = true
						queue = append(queue, work{child, item.depth + 1})
					}
				}
			}
		}
	}

	// 5. Phase 2: Execute pending updates inside the target transaction
	updates := targetResolver.FlushPendingUpdates()
	for _, u := range updates {
		if _, err := targetTx.ExecContext(ctx, u); err != nil {
			return fmt.Errorf("phase 2 update on target: %w", err)
		}
	}

	return targetTx.Commit()
}

// ExportToTargetMySQL executes streaming hydration directly against a target MySQL instance.
func (e *Exporter) ExportToTargetMySQL(
	ctx context.Context,
	targetDB *sql.DB,
	targetSchema string,
	anchorTable string,
	anchorWhere string,
	anchorLimit int,
	progressFn func(table string, n int),
) error {
	return e.ExportAnchorsToTargetMySQL(ctx, targetDB, targetSchema, []Anchor{{Table: anchorTable, Where: anchorWhere, Limit: anchorLimit}}, progressFn)
}

// -------------------------------------------------------------------------
// Direct target streaming mode: SQLite (Bulk loading with PRAGMA defer_foreign_keys)
// -------------------------------------------------------------------------

// ExportAnchorsToTargetSQLite executes streaming hydration directly into an SQLite database file or memory instance across multiple anchors.
func (e *Exporter) ExportAnchorsToTargetSQLite(
	ctx context.Context,
	targetDB *sql.DB,
	anchors []Anchor,
	progressFn func(table string, n int),
) error {
	sourceTx, err := e.sourceDB.BeginSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("begin source snapshot: %w", err)
	}
	defer sourceTx.Rollback(ctx)

	conn, err := targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire target sqlite connection: %w", err)
	}
	defer conn.Close()

	// High-throughput bulk loading PRAGMAs:
	// PRAGMA foreign_keys = ON;
	// PRAGMA defer_foreign_keys = ON;
	// BEGIN TRANSACTION;
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;"); err != nil {
		return fmt.Errorf("enable sqlite foreign keys: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON;"); err != nil {
		return fmt.Errorf("enable sqlite defer_foreign_keys: %w", err)
	}

	targetTx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin target sqlite tx: %w", err)
	}
	defer targetTx.Rollback()

	streamRowsToSQLite := func(table string, rows db.RowIterator) error {
		if rows == nil {
			return nil
		}
		defer rows.Close()

		cols := e.schema.Columns[table]
		if len(cols) == 0 {
			return nil
		}
		colNames := colNameSlice(cols)
		pkCols := e.schema.PrimaryKeys[table]

		quotedCols := make([]string, len(colNames))
		for i, c := range colNames {
			quotedCols[i] = db.QuoteIdent(db.EngineSQLite, c)
		}
		quotedTable := db.QuoteTable(db.EngineSQLite, "", table)

		for rows.Next() {
			rawVals, err := rows.Values()
			if err != nil {
				return fmt.Errorf("row values %s: %w", table, err)
			}
			pk := buildPK(pkCols, colNames, rawVals)
			if e.markVisited(table, pk) {
				continue
			}

			e.applyMasking(table, cols, rawVals)
			e.coerceDanglingNulls(table, cols, rawVals)
			e.recordFKValues(table, cols, rawVals)
			e.capturePolyData(table, colNames, rawVals)

			valuesClause := FormatValues(db.EngineSQLite, cols, rawVals)
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
				quotedTable,
				strings.Join(quotedCols, ", "),
				valuesClause,
			)

			if _, err := targetTx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("target sqlite insert into %s: %w", table, err)
			}

			e.RowCounts[table]++
			if progressFn != nil {
				progressFn(table, 1)
			}
		}
		return rows.Err()
	}

	rootTables := make([]string, 0, len(anchors))
	for _, a := range anchors {
		// 1. Stream self-referencing hierarchy if present
		if e.isSelfRef(a.Table) {
			edge := e.selfRefEdge(a.Table)
			if edge != nil && len(edge.FromColumns) == 1 && len(edge.ToColumns) == 1 {
				childCol, parentCol := edge.FromColumns[0], edge.ToColumns[0]
				cols := e.schema.Columns[a.Table]
				colNames := colNameSlice(cols)
				quotedCols := make([]string, len(colNames))
				for i, c := range colNames {
					quotedCols[i] = "t." + db.QuoteIdent(e.engine, c)
				}
				selectCols := strings.Join(quotedCols, ", ")
				seedWhere, seedLimit := buildSeedClauses(a.Where, a.Limit)

				cteSQL := fmt.Sprintf(`
WITH RECURSIVE hierarchy AS (
  SELECT %s FROM %s t %s %s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
)
SELECT DISTINCT * FROM hierarchy`,
					selectCols, db.QuoteTable(e.engine, e.cfg.Schema, a.Table), seedWhere, seedLimit,
					selectCols, db.QuoteTable(e.engine, e.cfg.Schema, a.Table), db.QuoteIdent(e.engine, parentCol), db.QuoteIdent(e.engine, childCol),
					selectCols, db.QuoteTable(e.engine, e.cfg.Schema, a.Table), db.QuoteIdent(e.engine, childCol), db.QuoteIdent(e.engine, parentCol),
				)

				rows, err := sourceTx.Query(ctx, cteSQL)
				if err != nil {
					return fmt.Errorf("recursive CTE %s: %w", a.Table, err)
				}
				if err := streamRowsToSQLite(a.Table, rows); err != nil {
					return err
				}
			}
		}

		// 2. Stream anchor seed rows
		cols := e.schema.Columns[a.Table]
		if len(cols) > 0 {
			colNames := colNameSlice(cols)
			query := e.buildQuery(e.cfg.Schema, a.Table, colNames, a.Where, a.Limit)
			rows, err := sourceTx.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("query anchor %s: %w", a.Table, err)
			}
			if err := streamRowsToSQLite(a.Table, rows); err != nil {
				return err
			}
		}
		rootTables = append(rootTables, a.Table)
	}

	if e.cfg.Upstream {
		if err := e.traverseUpstreamToSQLite(ctx, sourceTx, streamRowsToSQLite, rootTables); err != nil {
			return fmt.Errorf("stream upstream to sqlite: %w", err)
		}
	} else {
		// 3. Upward BFS traversal: stream mandatory parents
		{
			queue := make([]string, 0, len(rootTables))
			enqueued := make(map[string]bool, len(rootTables))
			for _, t := range rootTables {
				if !enqueued[t] {
					enqueued[t] = true
					queue = append(queue, t)
				}
			}

			for len(queue) > 0 {
				table := queue[0]
				queue = queue[1:]

				for _, edge := range e.g.OutEdges[table] {
					parent := edge.ToTable
					if parent == table || len(edge.ToColumns) != 1 {
						continue
					}
					assoc := e.findAssociation(table, parent)
					if assoc != nil && assoc.IsSkipped() {
						continue
					}
					fkVals := e.getFKValuesForEdge(table, edge)
					if len(fkVals) == 0 {
						continue
					}

					parentCols := e.schema.Columns[parent]
					colNames := colNameSlice(parentCols)
					colInfo := findColumnByName(parentCols, edge.ToColumns[0])
					condition := ""
					if assoc != nil {
						condition = assoc.Condition()
					}

					rows, err := queryByIDsWithCondition(ctx, sourceTx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
					if err != nil {
						return fmt.Errorf("query parents of %s: %w", parent, err)
					}
					if err := streamRowsToSQLite(parent, rows); err != nil {
						return err
					}

					if !enqueued[parent] {
						enqueued[parent] = true
						queue = append(queue, parent)
					}
				}

				// Virtual FK polymorphic parents
				for _, ve := range e.g.VirtualEdges[table] {
					grouped := map[string][]string{}
					for _, pp := range e.polyData[table][ve.DiscriminatorCol] {
						grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
					}
					for discVal, fkVals := range grouped {
						tc, ok := ve.Mappings[discVal]
						if !ok {
							continue
						}
						parentCols := e.schema.Columns[tc.Table]
						colNames := colNameSlice(parentCols)
						colInfo := findColumnByName(parentCols, tc.Column)

						rows, err := queryByIDs(ctx, sourceTx, e.engine, e.cfg.Schema, tc.Table, colNames, tc.Column, colInfo, fkVals, 0)
						if err != nil {
							return fmt.Errorf("virtual FK query (%s→%s): %w", table, tc.Table, err)
						}
						if err := streamRowsToSQLite(tc.Table, rows); err != nil {
							return err
						}

						if !enqueued[tc.Table] {
							enqueued[tc.Table] = true
							queue = append(queue, tc.Table)
						}
					}
				}
			}
		}

		// 4. Downward BFS traversal: stream child tables
		{
			type work struct {
				table string
				depth int
			}
			queue := make([]work, 0, len(rootTables))
			enqueued := make(map[string]bool, len(rootTables))
			for _, t := range rootTables {
				if !enqueued[t] {
					enqueued[t] = true
					queue = append(queue, work{table: t, depth: 0})
				}
			}

			for len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]

				if e.cfg.MaxDepth > 0 && item.depth >= e.cfg.MaxDepth {
					continue
				}

				for _, edge := range e.g.InEdges[item.table] {
					child := edge.FromTable
					if child == item.table || len(edge.FromColumns) != 1 {
						continue
					}
					assoc := e.findAssociation(child, item.table)
					if assoc != nil && assoc.IsSkipped() {
						continue
					}
					fkVals := e.visitedPKs(item.table)
					if len(fkVals) == 0 {
						continue
					}

					childCols := e.schema.Columns[child]
					colNames := colNameSlice(childCols)
					colInfo := findColumnByName(childCols, edge.FromColumns[0])

					sampling := e.samplingConfig(child, edge.FromColumns[0])
					rows, err := queryByIDsWithSampling(ctx, sourceTx, e.engine, e.cfg.Schema, child, colNames, edge.FromColumns[0], colInfo, fkVals, e.cfg.MaxRowsPerTable, sampling)
					if err != nil {
						return fmt.Errorf("query children %s: %w", child, err)
					}
					if err := streamRowsToSQLite(child, rows); err != nil {
						return err
					}

					if !enqueued[child] {
						enqueued[child] = true
						queue = append(queue, work{child, item.depth + 1})
					}
				}
			}
		}
	}

	// Commit transaction
	if err := targetTx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite transaction: %w", err)
	}

	// Follow with PRAGMA foreign_key_check to ensure 0 integrity violations
	fkRows, err := conn.QueryContext(ctx, "PRAGMA foreign_key_check;")
	if err != nil {
		return fmt.Errorf("run sqlite foreign_key_check: %w", err)
	}
	defer fkRows.Close()

	var violations []string
	for fkRows.Next() {
		var tbl, rowid, parent, fkid string
		if err := fkRows.Scan(&tbl, &rowid, &parent, &fkid); err != nil {
			return fmt.Errorf("scan foreign_key_check violation: %w", err)
		}
		violations = append(violations, fmt.Sprintf("table %s (rowid %s) -> parent %s (fkid %s)", tbl, rowid, parent, fkid))
	}
	if err := fkRows.Err(); err != nil {
		return fmt.Errorf("sqlite foreign_key_check rows: %w", err)
	}

	if len(violations) > 0 {
		return fmt.Errorf("sqlite foreign key check failed (%d violations): %s", len(violations), strings.Join(violations, "; "))
	}

	return nil
}

// ExportToTargetSQLite executes streaming hydration directly into an SQLite database file or memory instance.
func (e *Exporter) ExportToTargetSQLite(
	ctx context.Context,
	targetDB *sql.DB,
	anchorTable string,
	anchorWhere string,
	anchorLimit int,
	progressFn func(table string, n int),
) error {
	return e.ExportAnchorsToTargetSQLite(ctx, targetDB, []Anchor{{Table: anchorTable, Where: anchorWhere, Limit: anchorLimit}}, progressFn)
}

// -------------------------------------------------------------------------
// Self-referencing hierarchy resolution (SQL text emission path)
// -------------------------------------------------------------------------

func (e *Exporter) isSelfRef(table string) bool {
	for _, edge := range e.g.OutEdges[table] {
		if edge.ToTable == table {
			return true
		}
	}
	return false
}

func (e *Exporter) selfRefEdge(table string) *graph.Edge {
	for i, edge := range e.g.OutEdges[table] {
		if edge.ToTable == table {
			return &e.g.OutEdges[table][i]
		}
	}
	return nil
}

func (e *Exporter) resolveSelfRef(ctx context.Context, tx db.SourceTx, w io.Writer, table string, anchorWhere string, limit int) error {
	edge := e.selfRefEdge(table)
	if edge == nil || len(edge.FromColumns) != 1 || len(edge.ToColumns) != 1 {
		return nil
	}
	childCol, parentCol := edge.FromColumns[0], edge.ToColumns[0]
	cols := e.schema.Columns[table]
	if len(cols) == 0 {
		return nil
	}
	colNames := colNameSlice(cols)
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = "t." + db.QuoteIdent(e.engine, c)
	}
	selectCols := strings.Join(quotedCols, ", ")
	seedWhere, seedLimit := buildSeedClauses(anchorWhere, limit)

	cteSQL := fmt.Sprintf(`
WITH RECURSIVE hierarchy AS (
  SELECT %s FROM %s t %s %s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
  UNION
  SELECT %s FROM %s t INNER JOIN hierarchy h ON t.%s = h.%s
)
SELECT DISTINCT * FROM hierarchy`,
		selectCols, db.QuoteTable(e.engine, e.cfg.Schema, table), seedWhere, seedLimit,
		selectCols, db.QuoteTable(e.engine, e.cfg.Schema, table), db.QuoteIdent(e.engine, parentCol), db.QuoteIdent(e.engine, childCol),
		selectCols, db.QuoteTable(e.engine, e.cfg.Schema, table), db.QuoteIdent(e.engine, childCol), db.QuoteIdent(e.engine, parentCol),
	)

	rows, err := tx.Query(ctx, cteSQL)
	if err != nil {
		return fmt.Errorf("recursive CTE for %s: %w", table, err)
	}
	defer rows.Close()

	pkCols := e.schema.PrimaryKeys[table]
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return err
		}
		pk := buildPK(pkCols, colNames, vals)
		if e.markVisited(table, pk) {
			continue
		}
		e.applyMasking(table, cols, vals)
		e.coerceDanglingNulls(table, cols, vals)
		e.recordFKValues(table, cols, vals)
		e.capturePolyData(table, colNames, vals)

		if e.engine == db.EngineMySQL {
			stmt, updates := e.resolver.GeneratePhase1Insert(table, cols, vals)
			e.resolver.RecordPendingUpdates(updates)
			fmt.Fprintln(w, stmt)
		} else {
			fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING;\n",
				db.QuoteTable(e.engine, e.cfg.Schema, table),
				strings.Join(quotedColNames(e.engine, colNames), ", "),
				FormatValues(e.engine, cols, vals),
			)
		}
		e.RowCounts[table]++
	}
	return rows.Err()
}

// -------------------------------------------------------------------------
// BFS traversal — SQL emission path
// -------------------------------------------------------------------------

func (e *Exporter) bfsParents(ctx context.Context, tx db.SourceTx, w io.Writer, rootTables []string) error {
	queue := make([]string, 0, len(rootTables))
	enqueued := make(map[string]bool, len(rootTables))
	for _, t := range rootTables {
		if !enqueued[t] {
			enqueued[t] = true
			queue = append(queue, t)
		}
	}

	for len(queue) > 0 {
		table := queue[0]
		queue = queue[1:]

		for _, edge := range e.g.OutEdges[table] {
			parent := edge.ToTable
			if parent == table || len(edge.ToColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(table, parent)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			fkVals := e.getFKValuesForEdge(table, edge)
			if len(fkVals) == 0 {
				continue
			}

			parentCols := e.schema.Columns[parent]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, edge.ToColumns[0])
			condition := ""
			if assoc != nil {
				condition = assoc.Condition()
			}

			rows, err := queryByIDsWithCondition(ctx, tx, e.engine, e.cfg.Schema, parent, colNames, edge.ToColumns[0], colInfo, fkVals, 0, condition)
			if err != nil {
				return fmt.Errorf("query parents (%s): %w", parent, err)
			}

			if _, err := e.emitFromRows(rows, w, parent); err != nil {
				return err
			}

			if !enqueued[parent] {
				enqueued[parent] = true
				queue = append(queue, parent)
			}
		}

		// Virtual FK (polymorphic) parents
		if err := e.resolveVirtualParents(ctx, tx, w, table, &queue, enqueued); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) bfsChildren(ctx context.Context, tx db.SourceTx, w io.Writer, rootTables []string, _ int) error {
	type work struct {
		table string
		depth int
	}
	queue := make([]work, 0, len(rootTables))
	enqueued := make(map[string]bool, len(rootTables))
	for _, t := range rootTables {
		if !enqueued[t] {
			enqueued[t] = true
			queue = append(queue, work{table: t, depth: 0})
		}
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if e.cfg.MaxDepth > 0 && item.depth >= e.cfg.MaxDepth {
			continue
		}

		for _, edge := range e.g.InEdges[item.table] {
			child := edge.FromTable
			if child == item.table || len(edge.FromColumns) != 1 {
				continue
			}
			assoc := e.findAssociation(child, item.table)
			if assoc != nil && assoc.IsSkipped() {
				continue
			}
			fkVals := e.visitedPKs(item.table)
			if len(fkVals) == 0 {
				continue
			}

			childCols := e.schema.Columns[child]
			colNames := colNameSlice(childCols)
			colInfo := findColumnByName(childCols, edge.FromColumns[0])

			sampling := e.samplingConfig(child, edge.FromColumns[0])
			rows, err := queryByIDsWithSampling(ctx, tx, e.engine, e.cfg.Schema, child, colNames, edge.FromColumns[0], colInfo, fkVals, e.cfg.MaxRowsPerTable, sampling)
			if err != nil {
				return fmt.Errorf("query children (%s): %w", child, err)
			}

			if _, err := e.emitFromRows(rows, w, child); err != nil {
				return err
			}

			if !enqueued[child] {
				enqueued[child] = true
				queue = append(queue, work{child, item.depth + 1})
			}
		}
	}
	return nil
}

func (e *Exporter) resolveVirtualParents(ctx context.Context, tx db.SourceTx, w io.Writer, table string, queue *[]string, enqueued map[string]bool) error {
	for _, ve := range e.g.VirtualEdges[table] {
		discGroups := e.polyData[table][ve.DiscriminatorCol]
		grouped := map[string][]string{}
		for _, pp := range discGroups {
			grouped[pp.discVal] = append(grouped[pp.discVal], pp.fkVal)
		}
		for discVal, fkVals := range grouped {
			tc, ok := ve.Mappings[discVal]
			if !ok {
				continue
			}

			parentCols := e.schema.Columns[tc.Table]
			colNames := colNameSlice(parentCols)
			colInfo := findColumnByName(parentCols, tc.Column)

			rows, err := queryByIDs(ctx, tx, e.engine, e.cfg.Schema, tc.Table, colNames, tc.Column, colInfo, fkVals, 0)
			if err != nil {
				return fmt.Errorf("virtual FK query (%s→%s): %w", table, tc.Table, err)
			}

			if _, err := e.emitFromRows(rows, w, tc.Table); err != nil {
				return err
			}

			if !enqueued[tc.Table] {
				enqueued[tc.Table] = true
				*queue = append(*queue, tc.Table)
			}
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Core fetch and emit helpers
// -------------------------------------------------------------------------

func (e *Exporter) markVisited(table string, pk string) bool {
	if _, ok := e.visited[table]; !ok {
		e.visited[table] = make(map[string]bool)
	}
	if e.visited[table][pk] {
		return true
	}
	e.visited[table][pk] = true
	if e.keysetQueue != nil {
		e.keysetQueue.Enqueue(table, pk)
	}
	return false
}

// KeysetQueue returns the keyset queue tracking all unique visited rows.
func (e *Exporter) KeysetQueue() *graph.KeysetQueue {
	return e.keysetQueue
}

// PaginateKeyset drains items in batches from the keyset queue using cursor seek pagination.
func (e *Exporter) PaginateKeyset(batchSize int, fn func([]graph.KeysetItem) error) error {
	if e.keysetQueue == nil {
		return nil
	}
	return e.keysetQueue.Paginate(batchSize, fn)
}

func (e *Exporter) samplingConfig(table string, fkCol string) StratifiedSampling {
	pkCol := fkCol
	if pks := e.schema.PrimaryKeys[table]; len(pks) > 0 {
		pkCol = pks[0]
	}
	seed := e.cfg.SamplingSeed
	if seed == "" {
		seed = "dbdrain-sampling"
	}
	return StratifiedSampling{
		ChildrenPerParent: e.cfg.ChildrenPerParent,
		Seed:              seed,
		PKCol:             pkCol,
	}
}

func (e *Exporter) visitedPKs(table string) []string {
	pks := make([]string, 0, len(e.visited[table]))
	for pk := range e.visited[table] {
		pks = append(pks, pk)
	}
	return pks
}

func (e *Exporter) findAssociation(source, target string) *config.Association {
	cfg := config.Config{Associations: e.cfg.Associations}
	return cfg.FindAssociation(source, target)
}

func containsPath(path []string, s string) bool {
	for _, item := range path {
		if item == s {
			return true
		}
	}
	return false
}

func (e *Exporter) recordFKValues(table string, cols []introspect.Column, vals []any) {
	if e.g == nil || len(e.g.OutEdges[table]) == 0 {
		return
	}
	colIdx := make(map[string]int, len(cols))
	for i, c := range cols {
		colIdx[c.Name] = i
	}
	for _, edge := range e.g.OutEdges[table] {
		for _, colName := range edge.FromColumns {
			idx, ok := colIdx[colName]
			if !ok || idx >= len(vals) || vals[idx] == nil {
				continue
			}
			strVal := fmt.Sprintf("%v", vals[idx])
			if strVal == "<nil>" || strVal == "" {
				continue
			}
			if e.fkData == nil {
				e.fkData = make(map[string]map[string]map[string]bool)
			}
			if e.fkData[table] == nil {
				e.fkData[table] = make(map[string]map[string]bool)
			}
			if e.fkData[table][colName] == nil {
				e.fkData[table][colName] = make(map[string]bool)
			}
			e.fkData[table][colName][strVal] = true
		}
	}
}

func (e *Exporter) getFKValuesForEdge(table string, edge graph.Edge) []string {
	if len(edge.FromColumns) > 0 {
		colName := edge.FromColumns[0]
		if e.fkData != nil && e.fkData[table] != nil && len(e.fkData[table][colName]) > 0 {
			out := make([]string, 0, len(e.fkData[table][colName]))
			for v := range e.fkData[table][colName] {
				out = append(out, v)
			}
			return out
		}
	}
	return e.visitedPKs(table)
}

// coerceDanglingNulls checks if any foreign key points to an excluded table (e.g. restriction: false).
// If the child column is nullable, its value is coerced to nil (NULL) during extraction.
func (e *Exporter) coerceDanglingNulls(table string, cols []introspect.Column, vals []any) {
	if e.g == nil {
		return
	}
	colIdx := make(map[string]int, len(cols))
	for i, c := range cols {
		colIdx[c.Name] = i
	}

	for _, edge := range e.g.OutEdges[table] {
		assoc := e.findAssociation(table, edge.ToTable)
		if assoc != nil && assoc.IsSkipped() {
			for _, colName := range edge.FromColumns {
				idx, ok := colIdx[colName]
				if !ok || idx >= len(vals) || vals[idx] == nil {
					continue
				}
				if cols[idx].IsNullable {
					vals[idx] = nil
				}
			}
		}
	}
}

func (e *Exporter) capturePolyData(table string, colNames []string, vals []any) {
	virtualEdges, ok := e.g.VirtualEdges[table]
	if !ok || len(virtualEdges) == 0 {
		return
	}
	colIdx := make(map[string]int, len(colNames))
	for i, c := range colNames {
		colIdx[c] = i
	}

	for _, ve := range virtualEdges {
		discIdx, hasDisc := colIdx[ve.DiscriminatorCol]
		fkIdx, hasFK := colIdx[ve.ChildColumn]
		if !hasDisc || !hasFK {
			continue
		}
		discVal := fmt.Sprintf("%v", vals[discIdx])
		fkVal := fmt.Sprintf("%v", vals[fkIdx])
		if discVal == "<nil>" || fkVal == "<nil>" {
			continue
		}

		if e.polyData[table] == nil {
			e.polyData[table] = make(map[string][]polyPair)
		}
		e.polyData[table][ve.DiscriminatorCol] = append(
			e.polyData[table][ve.DiscriminatorCol],
			polyPair{discVal: discVal, fkVal: fkVal},
		)
	}
}

func (e *Exporter) emitFromRows(rows db.RowIterator, w io.Writer, table string) ([]string, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	cols := e.schema.Columns[table]
	if len(cols) == 0 {
		return nil, nil
	}
	colNames := colNameSlice(cols)
	pkCols := e.schema.PrimaryKeys[table]
	var pks []string

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("row values %s: %w", table, err)
		}
		pk := buildPK(pkCols, colNames, vals)
		if e.markVisited(table, pk) {
			continue
		}
		pks = append(pks, pk)
		e.applyMasking(table, cols, vals)
		e.coerceDanglingNulls(table, cols, vals)
		e.recordFKValues(table, cols, vals)
		e.capturePolyData(table, colNames, vals)

		if e.engine == db.EngineMySQL {
			stmt, updates := e.resolver.GeneratePhase1Insert(table, cols, vals)
			e.resolver.RecordPendingUpdates(updates)
			fmt.Fprintln(w, stmt)
		} else {
			fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING;\n",
				db.QuoteTable(e.engine, e.cfg.Schema, table),
				strings.Join(quotedColNames(e.engine, colNames), ", "),
				FormatValues(e.engine, cols, vals),
			)
		}
		e.RowCounts[table]++
	}
	return pks, rows.Err()
}

func (e *Exporter) fetchAndEmit(ctx context.Context, tx db.SourceTx, w io.Writer, table string, whereClause string, limit int) ([]string, error) {
	cols := e.schema.Columns[table]
	if len(cols) == 0 {
		return nil, nil
	}
	colNames := colNameSlice(cols)
	query := e.buildQuery(e.cfg.Schema, table, colNames, whereClause, limit)

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", table, err)
	}
	return e.emitFromRows(rows, w, table)
}

// -------------------------------------------------------------------------
// Masking
// -------------------------------------------------------------------------

func (e *Exporter) applyMasking(table string, cols []introspect.Column, vals []any) {
	if e.masker == nil {
		return
	}
	ruleset := config.Config{Rules: e.cfg.Rules}

	for i, col := range cols {
		if vals[i] == nil {
			continue
		}
		strVal := fmt.Sprintf("%v", vals[i])

		if rule := ruleset.FindRule(table, col.Name); rule != nil {
			masked := e.masker.ApplyTransform(string(rule.Transform), col.Name, strVal)
			if masked != strVal {
				vals[i] = masked
			}
		} else {
			masked := e.masker.MaskColumn(col.Name, strVal)
			if masked != strVal {
				vals[i] = masked
			}
		}
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func buildSeedClauses(where string, limit int) (string, string) {
	w, l := "", ""
	if where != "" {
		w = "WHERE " + where
	}
	if limit > 0 {
		l = fmt.Sprintf("LIMIT %d", limit)
	}
	return w, l
}

func colNameSlice(cols []introspect.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

func (e *Exporter) buildQuery(schema, table string, colNames []string, whereClause string, limit int) string {
	return buildQueryForEngine(e.engine, schema, table, colNames, whereClause, limit)
}

func buildQuery(schema, table string, colNames []string, whereClause string, limit int) string {
	return buildQueryForEngine(db.EnginePostgres, schema, table, colNames, whereClause, limit)
}

func buildQueryForEngine(engine db.EngineType, schema, table string, colNames []string, whereClause string, limit int) string {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = db.QuoteIdent(engine, c)
	}
	q := fmt.Sprintf(`SELECT %s FROM %s`,
		strings.Join(quotedCols, ", "),
		db.QuoteTable(engine, schema, table),
	)
	if whereClause != "" {
		q += " WHERE " + whereClause
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return q
}

func formatStringList(ss []string) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = "'" + escapePostgresString(s) + "'"
	}
	return strings.Join(parts, ", ")
}

func quoteIdent(s string) string {
	return graph.QuoteIdent(s)
}

func quotedColNames(engine db.EngineType, names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = db.QuoteIdent(engine, n)
	}
	return out
}

func buildPK(pkCols []string, colNames []string, vals []any) string {
	if len(pkCols) == 0 {
		return fmt.Sprintf("%v", vals)
	}
	parts := make([]string, 0, len(pkCols))
	for _, pk := range pkCols {
		for j, col := range colNames {
			if col == pk {
				parts = append(parts, fmt.Sprintf("%v", vals[j]))
				break
			}
		}
	}
	return strings.Join(parts, ",")
}

func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
