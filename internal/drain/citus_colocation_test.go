package drain

import (
	"strings"
	"testing"

	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

func TestCitusEdgeResolution(t *testing.T) {
	schema := &introspect.Schema{
		HasCitus: true,
		CitusTables: map[string]*introspect.CitusTable{
			"users": {
				TableName:          "users",
				TableType:          introspect.CitusTableDistributed,
				DistributionColumn: "tenant_id",
				DistributionMethod: "hash",
				ColocationID:       1,
			},
			"orders": {
				TableName:          "orders",
				TableType:          introspect.CitusTableDistributed,
				DistributionColumn: "tenant_id",
				DistributionMethod: "hash",
				ColocationID:       1,
			},
			"countries": {
				TableName:          "countries",
				TableType:          introspect.CitusTableReference,
				DistributionMethod: "reference",
			},
		},
	}

	exp := New(nil, schema, nil, nil, Config{})

	// 1. Single column FK
	edgeSingle := graph.Edge{
		FromTable:   "orders",
		FromColumns: []string{"country_code"},
		ToTable:     "countries",
		ToColumns:   []string{"code"},
	}
	fCol, tCol, ok := exp.resolveParentEdge(edgeSingle)
	if !ok || fCol != "country_code" || tCol != "code" {
		t.Errorf("expected single FK resolution: got (%s, %s, %v)", fCol, tCol, ok)
	}

	// 2. Citus compound FK between colocated distributed tables: (tenant_id, user_id) -> (tenant_id, id)
	edgeCitus := graph.Edge{
		FromTable:   "orders",
		FromColumns: []string{"tenant_id", "user_id"},
		ToTable:     "users",
		ToColumns:   []string{"tenant_id", "id"},
	}
	fCol, tCol, ok = exp.resolveParentEdge(edgeCitus)
	if !ok || fCol != "user_id" || tCol != "id" {
		t.Errorf("expected Citus compound parent FK resolution: got (%s, %s, %v)", fCol, tCol, ok)
	}

	// 3. Citus child edge resolution: traversing from users (parent) down to orders (child)
	fCol, tCol, ok = exp.resolveChildEdge(edgeCitus)
	if !ok || fCol != "user_id" || tCol != "id" {
		t.Errorf("expected Citus compound child FK resolution: got (%s, %s, %v)", fCol, tCol, ok)
	}
}

func TestRecordTenantBoundaryAndCondition(t *testing.T) {
	schema := &introspect.Schema{
		HasCitus: true,
		Columns: map[string][]introspect.Column{
			"users": {
				{Name: "id", DataType: "bigint"},
				{Name: "tenant_id", DataType: "bigint"},
				{Name: "email", DataType: "text"},
			},
			"orders": {
				{Name: "id", DataType: "bigint"},
				{Name: "tenant_id", DataType: "bigint"},
				{Name: "user_id", DataType: "bigint"},
			},
			"audit_logs": {
				{Name: "id", DataType: "bigint"},
				{Name: "message", DataType: "text"},
			},
		},
		CitusTables: map[string]*introspect.CitusTable{
			"users": {
				TableName:          "users",
				TableType:          introspect.CitusTableDistributed,
				DistributionColumn: "tenant_id",
				ColocationID:       10,
			},
			"orders": {
				TableName:          "orders",
				TableType:          introspect.CitusTableDistributed,
				DistributionColumn: "tenant_id",
				ColocationID:       10,
			},
			"audit_logs": {
				TableName: "audit_logs",
				TableType: introspect.CitusTableLocal,
			},
		},
	}

	exp := New(nil, schema, nil, nil, Config{})

	// Initially, no tenant boundary recorded
	cond := exp.getColocationCondition("orders")
	if cond != "" {
		t.Errorf("expected empty condition before row recording, got %q", cond)
	}

	// Record a user row for tenant 42
	userCols := schema.Columns["users"]
	exp.recordFKValues("users", userCols, []any{int64(1), int64(42), "user1@tenant42.com"})

	// Verify colocation boundary captured
	if !exp.colocationTenants[10]["42"] {
		t.Errorf("expected tenant 42 to be recorded in colocation group 10")
	}

	// Orders should now have the joint tenant boundary condition
	cond = exp.getColocationCondition("orders")
	expected := `"tenant_id" IN ('42')`
	if cond != expected {
		t.Errorf("expected %q, got %q", expected, cond)
	}

	// Local table has no colocation group
	localCond := exp.getColocationCondition("audit_logs")
	if localCond != "" {
		t.Errorf("expected empty condition for local table, got %q", localCond)
	}

	// Record another row for tenant 99
	exp.recordFKValues("users", userCols, []any{int64(2), int64(99), "user2@tenant99.com"})
	cond = exp.getColocationCondition("orders")
	if !strings.Contains(cond, "'42'") || !strings.Contains(cond, "'99'") {
		t.Errorf("expected condition to contain both tenants 42 and 99, got %q", cond)
	}
}

func TestCombineConditions(t *testing.T) {
	c1 := `"status" = 'active'`
	c2 := `"tenant_id" IN ('42')`

	combined := combineConditions(c1, c2)
	if combined != `("status" = 'active') AND ("tenant_id" IN ('42'))` {
		t.Errorf("unexpected combined condition: %q", combined)
	}

	if combineConditions("", c2) != c2 {
		t.Errorf("expected c2 when c1 is empty")
	}
	if combineConditions(c1, "") != c1 {
		t.Errorf("expected c1 when c2 is empty")
	}
}
