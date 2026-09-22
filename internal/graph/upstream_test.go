package graph

import (
	"reflect"
	"sort"
	"testing"

	"github.com/x7ssss/dbdrain/internal/introspect"
)

func TestUpstreamClosureMinimalAndPruned(t *testing.T) {
	// Schema:
	// customers (id)
	//   <- invoices (id, customer_id)
	//        <- charges (id, invoice_id)
	//        <- invoice_lines (id, invoice_id) [sibling of charges]
	//   <- subscriptions (id, customer_id) [sibling of invoices]
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"customers":     {{Name: "id"}},
			"invoices":      {{Name: "id"}, {Name: "customer_id"}},
			"charges":       {{Name: "id"}, {Name: "invoice_id"}},
			"invoice_lines": {{Name: "id"}, {Name: "invoice_id"}},
			"subscriptions": {{Name: "id"}, {Name: "customer_id"}},
		},
		ForeignKeys: []introspect.ForeignKey{
			{FromTable: "invoices", FromColumns: []string{"customer_id"}, ToTable: "customers", ToColumns: []string{"id"}},
			{FromTable: "charges", FromColumns: []string{"invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
			{FromTable: "invoice_lines", FromColumns: []string{"invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
			{FromTable: "subscriptions", FromColumns: []string{"customer_id"}, ToTable: "customers", ToColumns: []string{"id"}},
		},
	}

	g := New(schema)

	// Upstream closure from leaf "charges"
	closure := g.UpstreamClosure([]string{"charges"}, nil)

	sort.Strings(closure)
	expected := []string{"charges", "customers", "invoices"}
	sort.Strings(expected)

	if !reflect.DeepEqual(closure, expected) {
		t.Fatalf("expected upstream closure %v, got %v", expected, closure)
	}

	// Sibling check: invoice_lines and subscriptions MUST NOT be present
	for _, tbl := range closure {
		if tbl == "invoice_lines" || tbl == "subscriptions" {
			t.Fatalf("sibling table %q leaked into upstream closure!", tbl)
		}
	}
}

func TestUpstreamClosureMultiPathConvergence(t *testing.T) {
	// Multi-path convergence:
	// charges -> invoices -> customers
	// charges -> payments -> customers
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"customers": {{Name: "id"}},
			"invoices":  {{Name: "id"}, {Name: "customer_id"}},
			"payments":  {{Name: "id"}, {Name: "customer_id"}},
			"charges":   {{Name: "id"}, {Name: "invoice_id"}, {Name: "payment_id"}},
		},
		ForeignKeys: []introspect.ForeignKey{
			{FromTable: "charges", FromColumns: []string{"invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
			{FromTable: "charges", FromColumns: []string{"payment_id"}, ToTable: "payments", ToColumns: []string{"id"}},
			{FromTable: "invoices", FromColumns: []string{"customer_id"}, ToTable: "customers", ToColumns: []string{"id"}},
			{FromTable: "payments", FromColumns: []string{"customer_id"}, ToTable: "customers", ToColumns: []string{"id"}},
		},
	}

	g := New(schema)
	closure := g.UpstreamClosure([]string{"charges"}, nil)

	sort.Strings(closure)
	expected := []string{"charges", "customers", "invoices", "payments"}
	sort.Strings(expected)

	if !reflect.DeepEqual(closure, expected) {
		t.Fatalf("expected converged upstream closure %v, got %v", expected, closure)
	}
}

func TestUpstreamClosurePathCycleDetection(t *testing.T) {
	// Circular dependency:
	// charges -> invoices -> accounts -> invoices (cycle!)
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"charges":  {{Name: "id"}, {Name: "invoice_id"}},
			"invoices": {{Name: "id"}, {Name: "account_id"}},
			"accounts": {{Name: "id"}, {Name: "latest_invoice_id"}},
		},
		ForeignKeys: []introspect.ForeignKey{
			{FromTable: "charges", FromColumns: []string{"invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
			{FromTable: "invoices", FromColumns: []string{"account_id"}, ToTable: "accounts", ToColumns: []string{"id"}},
			{FromTable: "accounts", FromColumns: []string{"latest_invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
		},
	}

	g := New(schema)
	// Must terminate cleanly without infinite recursion
	closure := g.UpstreamClosure([]string{"charges"}, nil)

	sort.Strings(closure)
	expected := []string{"accounts", "charges", "invoices"}
	sort.Strings(expected)

	if !reflect.DeepEqual(closure, expected) {
		t.Fatalf("expected cycle closure %v, got %v", expected, closure)
	}
}

func TestPartitionedTableLogicalRootInDAG(t *testing.T) {
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"users":              {{Name: "id"}},
			"orders_partitioned": {{Name: "id"}, {Name: "user_id"}, {Name: "created_at"}},
			"orders_2026_01":     {{Name: "id"}, {Name: "user_id"}, {Name: "created_at"}},
			"orders_2026_02":     {{Name: "id"}, {Name: "user_id"}, {Name: "created_at"}},
		},
		PartitionedTables: map[string]*introspect.PartitionedTable{
			"orders_partitioned": {
				RootTable: "orders_partitioned",
				Strategy:  "RANGE",
				KeyDef:    "RANGE (created_at)",
				Partitions: []introspect.PartitionBound{
					{PartitionName: "orders_2026_01", Bound: "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"},
					{PartitionName: "orders_2026_02", Bound: "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"},
				},
			},
		},
		ChildToRootPartition: map[string]string{
			"orders_2026_01": "orders_partitioned",
			"orders_2026_02": "orders_partitioned",
		},
		ForeignKeys: []introspect.ForeignKey{
			{FromTable: "orders_2026_01", FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
			{FromTable: "orders_2026_02", FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
		},
	}

	g := New(schema)

	// DAG must represent orders_partitioned as a single logical root node,
	// child partitions must NOT exist as independent nodes.
	if _, ok := g.Nodes["orders_partitioned"]; !ok {
		t.Errorf("expected logical root 'orders_partitioned' in DAG nodes")
	}
	if _, ok := g.Nodes["orders_2026_01"]; ok {
		t.Errorf("child partition 'orders_2026_01' should not be an independent DAG node")
	}
	if _, ok := g.Nodes["orders_2026_02"]; ok {
		t.Errorf("child partition 'orders_2026_02' should not be an independent DAG node")
	}

	// OutEdges for orders_partitioned should have exactly one edge to users (deduplicated)
	outEdges := g.OutEdges["orders_partitioned"]
	if len(outEdges) != 1 {
		t.Fatalf("expected 1 deduplicated OutEdge from orders_partitioned, got %d", len(outEdges))
	}
	if outEdges[0].ToTable != "users" {
		t.Errorf("expected OutEdge to point to users, got %s", outEdges[0].ToTable)
	}
}
