package drain

import (
	"strings"
	"testing"

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
