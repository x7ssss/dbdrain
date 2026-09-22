package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/x7ssss/dbdrain/internal/config"
	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/drain"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/ui"
	"github.com/x7ssss/dbdrain/internal/verify"
)

const version = "v0.5.0"

var (
	source          string
	fromExpr        string
	output          string
	schemaName      string
	anonymize       bool
	salt            string
	maxRowsPerTable int
	maxDepth        int
	target          string
	configPath      string
	doVerify        bool
)

var rootCmd = &cobra.Command{
	Use:   "dbdrain",
	Short: "High-performance PostgreSQL & MySQL/MariaDB database slicer and PII anonymizer",
	Long: `dbdrain ` + version + ` extracts referentially intact subsets from a production
PostgreSQL or MySQL/MariaDB database, resolves circular FK dependencies (including
two-phase insert/update resolution for MySQL), handles polymorphic associations,
and deterministically masks PII.

Examples:
  # Stream slice to stdout and pipe directly into psql
  dbdrain --source "postgres://user:pass@localhost/prod" \
          --from "users WHERE id IN (1,2,3) LIMIT 50" | psql postgres://localhost/staging

  # Export MySQL slice to SQL file with two-phase circular resolution
  dbdrain --source "mysql://root:secret@localhost:3306/shop" \
          --from "orders WHERE created_at > '2026-01-01' LIMIT 100" \
          --anonymize-pii --output slice.sql

  # Stream directly into a target database with integrity check
  dbdrain --source "postgres://user:pass@localhost/prod" \
          --from "users WHERE id = 42 LIMIT 1" \
          --target "postgres://user:pass@localhost/staging" \
          --depth 2 --max-rows-per-table 500 --verify`,
	RunE: runDrain,
}

func init() {
	rootCmd.Flags().StringVar(&source, "source", "", "Source database connection string (PostgreSQL or MySQL/MariaDB) (required)")
	rootCmd.Flags().StringVar(&fromExpr, "from", "", `Anchor query, e.g. "users WHERE id IN (1,2,3) LIMIT 50" (required)`)
	rootCmd.Flags().StringVar(&output, "output", "-", "Output file path (default: stdout)")
	rootCmd.Flags().StringVar(&schemaName, "schema", "public", "Target schema/database name")
	rootCmd.Flags().BoolVar(&anonymize, "anonymize-pii", false, "Enable deterministic PII masking")
	rootCmd.Flags().StringVar(&salt, "salt", "dbdrain-secret-salt", "Salt for deterministic HMAC masking")
	rootCmd.Flags().IntVar(&maxRowsPerTable, "max-rows-per-table", 0, "Hard cap on rows pulled per child table (0 = unlimited)")
	rootCmd.Flags().IntVar(&maxDepth, "depth", 0, "Max FK traversal depth for child tables (0 = unlimited; parents always fully resolved)")
	rootCmd.Flags().StringVar(&target, "target", "", "Target database connection string for direct hydration (bypasses --output)")
	rootCmd.Flags().StringVar(&configPath, "config", "", "Path to dbdrain.yaml config file (auto-detected if omitted)")
	rootCmd.Flags().BoolVar(&doVerify, "verify", false, "Run post-hydration orphan integrity checks on --target (requires --target)")

	rootCmd.MarkFlagRequired("source")
	rootCmd.MarkFlagRequired("from")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// parseFromExpr parses the --from expression into table, where clause, and limit.
// Format: "<table> [WHERE <clause>] [LIMIT <n>]"
func parseFromExpr(expr string) (table string, whereClause string, limit int, err error) {
	expr = strings.TrimSpace(expr)

	limitRe := regexp.MustCompile(`(?i)\sLIMIT\s+(\d+)\s*$`)
	if m := limitRe.FindStringSubmatchIndex(expr); m != nil {
		limitStr := expr[m[2]:m[3]]
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return
		}
		expr = strings.TrimSpace(expr[:m[0]])
	}

	whereRe := regexp.MustCompile(`(?i)^(\S+)\s+WHERE\s+(.+)$`)
	if m := whereRe.FindStringSubmatch(expr); m != nil {
		table = m[1]
		whereClause = m[2]
		return
	}

	parts := strings.Fields(expr)
	if len(parts) == 0 {
		err = fmt.Errorf("--from expression is empty")
		return
	}
	table = parts[0]
	return
}

func runDrain(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	start := time.Now()

	// Validate flags.
	if doVerify && target == "" {
		return fmt.Errorf("--verify requires --target to be set")
	}

	anchorTable, anchorWhere, anchorLimit, err := parseFromExpr(fromExpr)
	if err != nil {
		return fmt.Errorf("invalid --from expression: %w", err)
	}

	// Load declarative config (silently skips if no dbdrain.yaml found).
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	isTTY := ui.IsTTY()
	var spinner *ui.Spinner
	spinnerMsg := func(msg string) {
		if spinner != nil {
			spinner.Stop()
		}
		if isTTY {
			spinner = ui.NewSpinner(msg)
			spinner.Start()
		}
	}
	stopSpinner := func() {
		if spinner != nil {
			spinner.Stop()
			spinner = nil
		}
	}

	sourceEngine, err := db.DetectEngine(source)
	if err != nil {
		return fmt.Errorf("detect source engine: %w", err)
	}

	spinnerMsg("Connecting to source database...")

	var sourceDB db.SourceDB
	var schema *introspect.Schema
	effectiveSchema := schemaName

	if sourceEngine == db.EngineMySQL {
		dsn, dbName, err := db.ParseMySQLDSN(source)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("parse mysql source: %w", err)
		}
		if !cmd.Flags().Changed("schema") && dbName != "" {
			effectiveSchema = dbName
		}

		sqlDB, err := sql.Open("mysql", dsn)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("connect to mysql source: %w", err)
		}
		defer sqlDB.Close()

		if err := sqlDB.PingContext(ctx); err != nil {
			stopSpinner()
			return fmt.Errorf("ping mysql source: %w", err)
		}

		sourceDB = db.NewMySQLSource(sqlDB)

		spinnerMsg("Introspecting MySQL schema...")
		schema, err = introspect.LoadMySQL(ctx, sqlDB, effectiveSchema)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("introspect mysql schema: %w", err)
		}
	} else {
		conn, err := pgx.Connect(ctx, source)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("connect to source: %w", err)
		}
		defer conn.Close(ctx)

		sourceDB = db.NewPostgresSource(conn)

		spinnerMsg("Introspecting PostgreSQL schema...")
		schema, err = introspect.LoadPostgres(ctx, conn, effectiveSchema)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("introspect schema: %w", err)
		}
	}

	// Convert config virtual FKs → graph.VirtualFKInput.
	var vfkInputs []graph.VirtualFKInput
	for _, vfk := range cfg.VirtualForeignKeys {
		vfkInputs = append(vfkInputs, graph.VirtualFKInput{
			ChildTable:       vfk.ChildTable,
			ChildColumn:      vfk.ChildColumn,
			DiscriminatorCol: vfk.ParentTableColumn,
			Mappings:         vfk.Mappings,
		})
	}

	g := graph.New(schema, vfkInputs)
	sccs := graph.TarjanSCC(g)

	drainCfg := drain.Config{
		Schema:          effectiveSchema,
		AnonymizePII:    anonymize,
		Salt:            salt,
		MaxRowsPerTable: maxRowsPerTable,
		MaxDepth:        maxDepth,
		Rules:           cfg.Rules,
	}

	exporter := drain.New(sourceDB, schema, g, sccs, drainCfg)

	// ---- Direct target hydration mode ----
	if target != "" {
		targetEngine, err := db.DetectEngine(target)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("detect target engine: %w", err)
		}

		var mu sync.Mutex
		rowsStreamed := make(map[string]int)

		progressFn := func(tbl string, n int) {
			mu.Lock()
			rowsStreamed[tbl] += n
			total := 0
			for _, v := range rowsStreamed {
				total += v
			}
			mu.Unlock()
			if isTTY && spinner != nil {
				spinner.UpdateMessage(fmt.Sprintf("Streaming '%s' → target (%d rows)...", anchorTable, total))
			}
		}

		var uiViolations []ui.Violation

		if targetEngine == db.EngineMySQL {
			spinnerMsg("Connecting to target MySQL database...")
			targetDSN, targetDBName, err := db.ParseMySQLDSN(target)
			if err != nil {
				stopSpinner()
				return fmt.Errorf("parse target mysql DSN: %w", err)
			}
			targetSchema := effectiveSchema
			if targetDBName != "" {
				targetSchema = targetDBName
			}

			targetDB, err := sql.Open("mysql", targetDSN)
			if err != nil {
				stopSpinner()
				return fmt.Errorf("connect to target mysql: %w", err)
			}
			defer targetDB.Close()

			if err := targetDB.PingContext(ctx); err != nil {
				stopSpinner()
				return fmt.Errorf("ping target mysql: %w", err)
			}

			spinnerMsg(fmt.Sprintf("Streaming '%s' → target MySQL...", anchorTable))
			if err := exporter.ExportToTargetMySQL(ctx, targetDB, targetSchema, anchorTable, anchorWhere, anchorLimit, progressFn); err != nil {
				stopSpinner()
				return fmt.Errorf("stream to target mysql: %w", err)
			}

			// Post-hydration integrity verification.
			if doVerify {
				spinnerMsg("Running integrity checks on MySQL target...")
				violations, verifyErr := verify.RunMySQL(ctx, targetDB, targetSchema, schema, exporter.RowCounts)
				if verifyErr != nil {
					stopSpinner()
					return fmt.Errorf("integrity check: %w", verifyErr)
				}
				for _, v := range violations {
					uiViolations = append(uiViolations, ui.Violation{
						ChildTable:   v.ChildTable,
						ChildColumn:  v.ChildColumn,
						ParentTable:  v.ParentTable,
						ParentColumn: v.ParentColumn,
						OrphanCount:  v.OrphanCount,
					})
				}
			}
		} else {
			spinnerMsg("Connecting to target PostgreSQL database...")
			targetConn, err := pgx.Connect(ctx, target)
			if err != nil {
				stopSpinner()
				return fmt.Errorf("connect to target: %w", err)
			}
			defer targetConn.Close(ctx)

			spinnerMsg(fmt.Sprintf("Streaming '%s' → target...", anchorTable))
			if err := exporter.ExportToTarget(ctx, targetConn, anchorTable, anchorWhere, anchorLimit, progressFn); err != nil {
				stopSpinner()
				return fmt.Errorf("stream to target: %w", err)
			}

			// Post-hydration integrity verification.
			if doVerify {
				spinnerMsg("Running integrity checks...")
				violations, verifyErr := verify.Run(ctx, targetConn, effectiveSchema, schema, exporter.RowCounts)
				if verifyErr != nil {
					stopSpinner()
					return fmt.Errorf("integrity check: %w", verifyErr)
				}
				for _, v := range violations {
					uiViolations = append(uiViolations, ui.Violation{
						ChildTable:   v.ChildTable,
						ChildColumn:  v.ChildColumn,
						ParentTable:  v.ParentTable,
						ParentColumn: v.ParentColumn,
						OrphanCount:  v.OrphanCount,
					})
				}
			}
		}

		stopSpinner()

		if isTTY {
			hasCycles := false
			for _, scc := range sccs {
				if scc.HasCycle {
					hasCycles = true
					break
				}
			}
			sum := &ui.Summary{
				Rows:       exporter.RowCounts,
				Duration:   time.Since(start),
				HasCycles:  hasCycles,
				Target:     target,
				Violations: uiViolations,
				VerifyRan:  doVerify,
			}
			sum.Print(os.Stderr)
		} else if doVerify && len(uiViolations) > 0 {
			for _, v := range uiViolations {
				fmt.Fprintf(os.Stderr, "ORPHAN: %s.%s → %s.%s : %d rows\n",
					v.ChildTable, v.ChildColumn, v.ParentTable, v.ParentColumn, v.OrphanCount)
			}
		}

		if len(uiViolations) > 0 {
			os.Exit(1)
		}
		return nil
	}

	// ---- SQL emission mode ----
	var w io.Writer = os.Stdout
	if output != "" && output != "-" {
		f, err := os.Create(output)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("open output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	spinnerMsg(fmt.Sprintf("Exporting slice from '%s'...", anchorTable))

	if err := exporter.Export(ctx, w, anchorTable, anchorWhere, anchorLimit); err != nil {
		stopSpinner()
		return fmt.Errorf("export: %w", err)
	}

	stopSpinner()

	if isTTY {
		hasCycles := false
		for _, scc := range sccs {
			if scc.HasCycle {
				hasCycles = true
				break
			}
		}
		sum := &ui.Summary{
			Rows:      exporter.RowCounts,
			Duration:  time.Since(start),
			HasCycles: hasCycles,
		}
		sum.Print(os.Stderr)
	}

	return nil
}
