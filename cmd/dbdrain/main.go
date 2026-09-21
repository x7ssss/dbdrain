package main

import (
	"context"
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
	"github.com/x7ssss/dbdrain/internal/drain"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/ui"
)

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
)

var rootCmd = &cobra.Command{
	Use:   "dbdrain",
	Short: "High-performance PostgreSQL database slicer and PII anonymizer",
	Long: `dbdrain extracts referentially intact subsets from a production PostgreSQL
database, resolves circular FK dependencies, and deterministically masks PII.

Examples:
  # Stream slice to stdout and pipe directly into psql
  dbdrain --source "postgres://user:pass@localhost/prod" \
          --from "users WHERE id IN (1,2,3) LIMIT 50" | psql postgres://localhost/staging

  # Save to file with PII masking
  dbdrain --source "postgres://user:pass@localhost/prod" \
          --from "orders WHERE created_at > NOW() - INTERVAL '7 days' LIMIT 100" \
          --anonymize-pii --output slice.sql

  # Stream directly into a target database
  dbdrain --source "postgres://user:pass@localhost/prod" \
          --from "users WHERE id = 42 LIMIT 1" \
          --target "postgres://user:pass@localhost/staging" \
          --depth 2 --max-rows-per-table 500`,
	RunE: runDrain,
}

func init() {
	rootCmd.Flags().StringVar(&source, "source", "", "Source PostgreSQL connection string (required)")
	rootCmd.Flags().StringVar(&fromExpr, "from", "", `Anchor query, e.g. "users WHERE id IN (1,2,3) LIMIT 50" (required)`)
	rootCmd.Flags().StringVar(&output, "output", "-", "Output file path (default: stdout)")
	rootCmd.Flags().StringVar(&schemaName, "schema", "public", "Target schema name")
	rootCmd.Flags().BoolVar(&anonymize, "anonymize-pii", false, "Enable deterministic PII masking")
	rootCmd.Flags().StringVar(&salt, "salt", "dbdrain-secret-salt", "Salt for deterministic HMAC masking")
	rootCmd.Flags().IntVar(&maxRowsPerTable, "max-rows-per-table", 0, "Hard cap on rows pulled per child table (0 = unlimited)")
	rootCmd.Flags().IntVar(&maxDepth, "depth", 0, "Max FK traversal depth for child tables (0 = unlimited; parents always fully resolved)")
	rootCmd.Flags().StringVar(&target, "target", "", "Target PostgreSQL connection string for direct hydration (bypasses --output)")

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

	// Extract LIMIT
	limitRe := regexp.MustCompile(`(?i)\sLIMIT\s+(\d+)\s*$`)
	if m := limitRe.FindStringSubmatchIndex(expr); m != nil {
		limitStr := expr[m[2]:m[3]]
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return
		}
		expr = strings.TrimSpace(expr[:m[0]])
	}

	// Extract WHERE
	whereRe := regexp.MustCompile(`(?i)^(\S+)\s+WHERE\s+(.+)$`)
	if m := whereRe.FindStringSubmatch(expr); m != nil {
		table = m[1]
		whereClause = m[2]
		return
	}

	// Just a table name
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

	// Parse --from expression
	anchorTable, anchorWhere, anchorLimit, err := parseFromExpr(fromExpr)
	if err != nil {
		return fmt.Errorf("invalid --from expression: %w", err)
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

	spinnerMsg("Connecting to source database...")

	// Connect to source
	conn, err := pgx.Connect(ctx, source)
	if err != nil {
		stopSpinner()
		return fmt.Errorf("connect to source: %w", err)
	}
	defer conn.Close(ctx)

	spinnerMsg("Introspecting schema...")

	// Introspect schema
	schema, err := introspect.Load(ctx, conn, schemaName)
	if err != nil {
		stopSpinner()
		return fmt.Errorf("introspect schema: %w", err)
	}

	// Build dependency graph + detect cycles
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	exporter := drain.New(conn, schema, g, sccs, drain.Config{
		Schema:          schemaName,
		AnonymizePII:    anonymize,
		Salt:            salt,
		MaxRowsPerTable: maxRowsPerTable,
		MaxDepth:        maxDepth,
	})

	// --- Direct target hydration mode ---
	if target != "" {
		spinnerMsg("Connecting to target database...")
		targetConn, err := pgx.Connect(ctx, target)
		if err != nil {
			stopSpinner()
			return fmt.Errorf("connect to target: %w", err)
		}
		defer targetConn.Close(ctx)

		var mu sync.Mutex
		rowsStreamed := make(map[string]int)

		spinnerMsg(fmt.Sprintf("Streaming '%s' → target...", anchorTable))

		progressFn := func(table string, n int) {
			mu.Lock()
			rowsStreamed[table] += n
			mu.Unlock()
			if isTTY {
				mu.Lock()
				total := 0
				for _, v := range rowsStreamed {
					total += v
				}
				mu.Unlock()
				spinner.UpdateMessage(fmt.Sprintf("Streaming '%s' → target (%d rows)...", anchorTable, total))
			}
		}

		if err := exporter.ExportToTarget(ctx, targetConn, anchorTable, anchorWhere, anchorLimit, progressFn); err != nil {
			stopSpinner()
			return fmt.Errorf("stream to target: %w", err)
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
				Target:    target,
			}
			sum.Print(os.Stderr)
		}
		return nil
	}

	// --- SQL emission mode ---
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

	// Print summary on TTY
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
