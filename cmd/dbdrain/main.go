package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/x7ssss/dbdrain/internal/drain"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/ui"
)

var (
	source      string
	fromExpr    string
	output      string
	schemaName  string
	anonymize   bool
	salt        string
)

var rootCmd = &cobra.Command{
	Use:   "dbdrain",
	Short: "High-performance PostgreSQL database slicer and PII anonymizer",
	Long: `dbdrain extracts referentially intact subsets from a production PostgreSQL
database, resolves circular FK dependencies, and deterministically masks PII.

Example:
  dbdrain --source "postgres://user:pass@localhost/prod" \
          --from "users WHERE id IN (1,2,3) LIMIT 50" \
          --anonymize-pii --output slice.sql`,
	RunE: runDrain,
}

func init() {
	rootCmd.Flags().StringVar(&source, "source", "", "Source PostgreSQL connection string (required)")
	rootCmd.Flags().StringVar(&fromExpr, "from", "", `Anchor query, e.g. "users WHERE id IN (1,2,3) LIMIT 50" (required)`)
	rootCmd.Flags().StringVar(&output, "output", "-", "Output file path (default: stdout)")
	rootCmd.Flags().StringVar(&schemaName, "schema", "public", "Target schema name")
	rootCmd.Flags().BoolVar(&anonymize, "anonymize-pii", false, "Enable deterministic PII masking")
	rootCmd.Flags().StringVar(&salt, "salt", "dbdrain-secret-salt", "Salt for deterministic HMAC masking")

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
	// Normalize whitespace
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

	// Determine output writer
	var w io.Writer = os.Stdout
	if output != "" && output != "-" {
		f, err := os.Create(output)
		if err != nil {
			return fmt.Errorf("open output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	isTTY := ui.IsTTY()

	var spinner *ui.Spinner
	if isTTY {
		spinner = ui.NewSpinner("Connecting to database...")
		spinner.Start()
	}

	// Connect to PostgreSQL
	conn, err := pgx.Connect(ctx, source)
	if err != nil {
		if spinner != nil {
			spinner.Stop()
		}
		return fmt.Errorf("connect to database: %w", err)
	}
	defer conn.Close(ctx)

	if spinner != nil {
		spinner.Stop()
		spinner = ui.NewSpinner("Introspecting schema...")
		spinner.Start()
	}

	// Introspect schema
	schema, err := introspect.Load(ctx, conn, schemaName)
	if err != nil {
		if spinner != nil {
			spinner.Stop()
		}
		return fmt.Errorf("introspect schema: %w", err)
	}

	// Build dependency graph
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	if spinner != nil {
		spinner.Stop()
		spinner = ui.NewSpinner(fmt.Sprintf("Exporting slice from '%s'...", anchorTable))
		spinner.Start()
	}

	// Export
	exporter := drain.New(conn, schema, g, sccs, drain.Config{
		Schema:       schemaName,
		AnonymizePII: anonymize,
		Salt:         salt,
	})

	if err := exporter.Export(ctx, w, anchorTable, anchorWhere, anchorLimit); err != nil {
		if spinner != nil {
			spinner.Stop()
		}
		return fmt.Errorf("export: %w", err)
	}

	if spinner != nil {
		spinner.Stop()
	}

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
			Rows:      make(map[string]int),
			Duration:  time.Since(start),
			HasCycles: hasCycles,
		}
		sum.Print(os.Stderr)
	}

	return nil
}
