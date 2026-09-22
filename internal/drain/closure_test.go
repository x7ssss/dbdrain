package drain

import (
	"strings"
	"testing"

	"github.com/x7ssss/dbdrain/internal/config"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

func TestMultiAnchorVisitedDeduplication(t *testing.T) {
	schema := makeSchema(map[string][]string{
		"tenants": {"id", "name"},
		"users":   {"id", "tenant_id", "name"},
		"orders":  {"id", "user_id", "total"},
	}, []introspect.ForeignKey{
		{FromTable: "users", FromColumns: []string{"tenant_id"}, ToTable: "tenants", ToColumns: []string{"id"}},
		{FromTable: "orders", FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
	})
	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	exp := New(nil, schema, g, sccs, Config{})

	// Simulate visiting entities from Anchor 1: users
	if exp.markVisited("users", "u1") {
		t.Errorf("expected first visit of u1 to return false")
	}
	if exp.markVisited("users", "u2") {
		t.Errorf("expected first visit of u2 to return false")
	}

	// Simulate visiting entities from Anchor 2: tenants (which reaches users again)
	if !exp.markVisited("users", "u1") {
		t.Errorf("expected duplicate visit of u1 to return true (deduplicated)")
	}
	if exp.markVisited("tenants", "t1") {
		t.Errorf("expected first visit of t1 to return false")
	}

	// Keyset queue should contain 3 unique items: users:u1, users:u2, tenants:t1
	kq := exp.KeysetQueue()
	if kq.Len() != 3 {
		t.Fatalf("expected 3 items in keyset queue, got %d", kq.Len())
	}

	var items []graph.KeysetItem
	err := exp.PaginateKeyset(2, func(page []graph.KeysetItem) error {
		items = append(items, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("PaginateKeyset failed: %v", err)
	}

	if len(items) != 3 {
		t.Fatalf("expected 3 paginated items, got %d", len(items))
	}
}

func TestStratifiedSamplingConfig(t *testing.T) {
	schema := makeSchema(map[string][]string{
		"users":  {"id", "name"},
		"orders": {"order_id", "user_id", "amount"},
	}, []introspect.ForeignKey{
		{FromTable: "orders", FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
	})
	schema.PrimaryKeys["orders"] = []string{"order_id"}

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	cfg := Config{
		ChildrenPerParent: 3,
		SamplingSeed:      "custom-test-seed",
	}
	exp := New(nil, schema, g, sccs, cfg)

	sc := exp.samplingConfig("orders", "user_id")
	if sc.ChildrenPerParent != 3 {
		t.Errorf("expected ChildrenPerParent=3, got %d", sc.ChildrenPerParent)
	}
	if sc.Seed != "custom-test-seed" {
		t.Errorf("expected Seed='custom-test-seed', got %q", sc.Seed)
	}
	if sc.PKCol != "order_id" {
		t.Errorf("expected PKCol='order_id', got %q", sc.PKCol)
	}
}

func TestStratifiedChildQueryReferentialInvariant(t *testing.T) {
	cols := []string{"id", "tenant_id", "status"}

	// When ChildrenPerParent is 1
	q1 := graph.BuildStratifiedChildQuery("public", "users", cols, "tenant_id", "id", "bigint", 1, 0)
	// Referential invariant verification:
	// rn = 1 ensures that every parent with >= 1 child is guaranteed to retain at least 1 child
	if !strings.Contains(q1, "rn = 1 OR (rn <= 1 AND total_children > 1)") {
		t.Errorf("query does not maintain referential invariant for N=1: %s", q1)
	}

	// When ChildrenPerParent is 5
	q5 := graph.BuildStratifiedChildQuery("public", "users", cols, "tenant_id", "id", "bigint", 5, 100)
	if !strings.Contains(q5, "rn = 1 OR (rn <= 5 AND total_children > 1)") {
		t.Errorf("query does not maintain referential invariant for N=5: %s", q5)
	}

	// Deterministic MD5 hash order verification
	if !strings.Contains(q5, "ORDER BY MD5(CAST(c.\"id\" AS text) || $2)") {
		t.Errorf("query missing deterministic hash ordering: %s", q5)
	}
}

func TestReverseTraversalMinimalUpstreamClosure(t *testing.T) {
	// Schema:
	// customers <- invoices <- charges (incident leaf)
	// Siblings/children that MUST be pruned:
	// customer_notes -> customers (sibling of invoices)
	// invoice_items -> invoices (sibling of charges)
	// audit_trail -> charges (child of charges)
	schema := makeSchema(map[string][]string{
		"customers":      {"id", "name"},
		"invoices":       {"id", "customer_id", "total"},
		"charges":        {"id", "invoice_id", "amount"},
		"customer_notes": {"id", "customer_id", "note"},
		"invoice_items":  {"id", "invoice_id", "item"},
		"audit_trail":    {"id", "charge_id", "action"},
	}, []introspect.ForeignKey{
		{FromTable: "invoices", FromColumns: []string{"customer_id"}, ToTable: "customers", ToColumns: []string{"id"}},
		{FromTable: "charges", FromColumns: []string{"invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
		{FromTable: "customer_notes", FromColumns: []string{"customer_id"}, ToTable: "customers", ToColumns: []string{"id"}},
		{FromTable: "invoice_items", FromColumns: []string{"invoice_id"}, ToTable: "invoices", ToColumns: []string{"id"}},
		{FromTable: "audit_trail", FromColumns: []string{"charge_id"}, ToTable: "charges", ToColumns: []string{"id"}},
	})

	g := graph.New(schema)
	closure := g.UpstreamClosure([]string{"charges"}, nil)

	closureMap := make(map[string]bool)
	for _, tbl := range closure {
		closureMap[tbl] = true
	}

	// Must include anchor charges and ancestors invoices, customers
	if !closureMap["charges"] {
		t.Errorf("expected closure to contain 'charges'")
	}
	if !closureMap["invoices"] {
		t.Errorf("expected closure to contain ancestor 'invoices'")
	}
	if !closureMap["customers"] {
		t.Errorf("expected closure to contain ancestor 'customers'")
	}

	// Must strictly PRUNE all siblings and downward children (zero sibling rows/tables)
	siblingsAndChildren := []string{"customer_notes", "invoice_items", "audit_trail"}
	for _, s := range siblingsAndChildren {
		if closureMap[s] {
			t.Errorf("reverse traversal failed to prune sibling/child table %q: closure=%v", s, closure)
		}
	}
}

func TestConditionalFKRestrictionsAndDanglingNulls(t *testing.T) {
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"charges": {
				{Name: "id", DataType: "text", Ordinal: 1, IsNullable: false},
				{Name: "amount", DataType: "numeric", Ordinal: 2, IsNullable: false},
				{Name: "audit_id", DataType: "text", Ordinal: 3, IsNullable: true},
				{Name: "not_null_audit_id", DataType: "text", Ordinal: 4, IsNullable: false},
			},
			"audit_trail": {
				{Name: "id", DataType: "text", Ordinal: 1, IsNullable: false},
			},
			"analytics_events": {
				{Name: "id", DataType: "text", Ordinal: 1, IsNullable: false},
			},
		},
		PrimaryKeys: map[string][]string{
			"charges":          {"id"},
			"audit_trail":      {"id"},
			"analytics_events": {"id"},
		},
		ForeignKeys: []introspect.ForeignKey{
			{FromTable: "charges", FromColumns: []string{"audit_id"}, ToTable: "audit_trail", ToColumns: []string{"id"}},
			{FromTable: "charges", FromColumns: []string{"not_null_audit_id"}, ToTable: "audit_trail", ToColumns: []string{"id"}},
		},
	}

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	cfg := Config{
		Associations: []config.Association{
			{
				Source:      "charges",
				Target:      "audit_trail",
				Restriction: config.RestrictionValue{Skip: true},
			},
			{
				Source:      "charges",
				Target:      "analytics_events",
				Restriction: config.RestrictionValue{Condition: "event_type = 'billing'"},
			},
		},
	}

	exp := New(nil, schema, g, sccs, cfg)

	// Verify predicate finding
	assoc := exp.findAssociation("charges", "analytics_events")
	if assoc == nil || assoc.Condition() != "event_type = 'billing'" {
		t.Fatalf("expected predicate \"event_type = 'billing'\", got %+v", assoc)
	}

	// Verify skipped association
	skippedAssoc := exp.findAssociation("charges", "audit_trail")
	if skippedAssoc == nil || !skippedAssoc.IsSkipped() {
		t.Fatalf("expected audit_trail to be skipped")
	}

	// Test dangling NULL coercion
	cols := schema.Columns["charges"]
	vals := []any{"ch_100", 250.50, "aud_999", "aud_fixed"}

	exp.coerceDanglingNulls("charges", cols, vals)

	// Nullable audit_id (index 2) must be coerced to nil
	if vals[2] != nil {
		t.Errorf("expected nullable FK audit_id to be coerced to nil, got %v", vals[2])
	}
	// Non-nullable audit_id (index 3) must NOT be coerced to nil
	if vals[3] != "aud_fixed" {
		t.Errorf("expected non-nullable FK not_null_audit_id to remain %q, got %v", "aud_fixed", vals[3])
	}
	// Non-FK columns must remain unchanged
	if vals[0] != "ch_100" || vals[1] != 250.50 {
		t.Errorf("unexpected changes to non-FK columns: %v", vals)
	}
}
