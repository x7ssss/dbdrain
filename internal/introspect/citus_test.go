package introspect

import (
	"strings"
	"testing"
)

func TestCitusMetadataCategorization(t *testing.T) {
	s := &Schema{
		HasCitus: true,
		Columns: map[string][]Column{
			"users": {
				{Name: "id", DataType: "bigint", IsNullable: false},
				{Name: "tenant_id", DataType: "bigint", IsNullable: false},
			},
			"countries": {
				{Name: "code", DataType: "text", IsNullable: false},
				{Name: "name", DataType: "text", IsNullable: false},
			},
			"audit_logs": {
				{Name: "id", DataType: "bigint", IsNullable: false},
				{Name: "action", DataType: "text", IsNullable: false},
			},
		},
		CitusTables: map[string]*CitusTable{
			"users": {
				TableName:          "users",
				TableType:          CitusTableDistributed,
				DistributionColumn: "tenant_id",
				DistributionMethod: "hash",
				ColocationID:       1,
			},
			"countries": {
				TableName:          "countries",
				TableType:          CitusTableReference,
				DistributionMethod: "reference",
			},
			"audit_logs": {
				TableName: "audit_logs",
				TableType: CitusTableLocal,
			},
		},
	}

	if !s.IsCitusDistributed("users") {
		t.Errorf("expected users to be Citus distributed")
	}
	if s.IsCitusReference("users") {
		t.Errorf("expected users not to be reference table")
	}

	if !s.IsCitusReference("countries") {
		t.Errorf("expected countries to be reference table")
	}
	if s.IsCitusDistributed("countries") {
		t.Errorf("expected countries not to be distributed")
	}

	if s.IsCitusDistributed("audit_logs") || s.IsCitusReference("audit_logs") {
		t.Errorf("expected audit_logs to be neither distributed nor reference (Local)")
	}
}

func TestCitusPhysicalShardDetection(t *testing.T) {
	s := &Schema{
		HasCitus: true,
		CitusTables: map[string]*CitusTable{
			"users": {
				TableName:          "users",
				TableType:          CitusTableDistributed,
				DistributionColumn: "tenant_id",
				DistributionMethod: "hash",
			},
		},
		CitusPhysicalShards: map[string]bool{
			"users_102008": true,
		},
	}

	// Exact entry in CitusPhysicalShards map
	if !s.IsPhysicalShard("users_102008") {
		t.Errorf("expected users_102008 to be detected as physical shard")
	}

	// Heuristic pattern: <distTable>_<digits>
	if !s.IsPhysicalShard("users_102009") {
		t.Errorf("expected users_102009 to be detected as physical shard via heuristic")
	}

	// Logical coordinator relation must never be treated as a shard
	if s.IsPhysicalShard("users") {
		t.Errorf("logical table users must not be a physical shard")
	}

	// Non-digit suffix table must not be treated as a shard
	if s.IsPhysicalShard("users_archive") {
		t.Errorf("users_archive must not be detected as a physical shard")
	}
}

func TestCitusTopologyValidation(t *testing.T) {
	s := &Schema{
		HasCitus: true,
		CitusTables: map[string]*CitusTable{
			"tenants": {
				TableName:          "tenants",
				TableType:          CitusTableDistributed,
				DistributionColumn: "id",
				ColocationID:       1,
			},
			"users": {
				TableName:          "users",
				TableType:          CitusTableDistributed,
				DistributionColumn: "tenant_id",
				ColocationID:       1,
			},
			"events": {
				TableName:          "events",
				TableType:          CitusTableDistributed,
				DistributionColumn: "tenant_id",
				ColocationID:       2, // Different colocation group!
			},
			"orders": {
				TableName:          "orders",
				TableType:          CitusTableDistributed,
				DistributionColumn: "tenant_id",
				ColocationID:       1,
			},
		},
	}

	// 1. Valid FK: colocated and includes distribution column
	s.ForeignKeys = []ForeignKey{
		{
			ConstraintName: "users_tenant_fk",
			FromTable:      "users",
			FromColumns:    []string{"tenant_id"},
			ToTable:        "tenants",
			ToColumns:      []string{"id"},
		},
	}
	warnings := s.ValidateCitusTopology()
	if len(warnings) != 0 {
		t.Errorf("expected 0 warnings for valid colocated FK, got %v", warnings)
	}

	// 2. Missing distribution column in FK
	s.ForeignKeys = []ForeignKey{
		{
			ConstraintName: "orders_user_fk",
			FromTable:      "orders",
			FromColumns:    []string{"user_id"}, // Missing tenant_id!
			ToTable:        "users",
			ToColumns:      []string{"id"},
		},
	}
	warnings = s.ValidateCitusTopology()
	if len(warnings) == 0 {
		t.Errorf("expected warning for FK missing distribution column")
	} else {
		found := false
		for _, w := range warnings {
			if strings.Contains(w, "does not include distribution column") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected distribution column warning, got %v", warnings)
		}
	}

	// 3. Different colocation groups
	s.ForeignKeys = []ForeignKey{
		{
			ConstraintName: "events_user_fk",
			FromTable:      "events",
			FromColumns:    []string{"tenant_id", "user_id"},
			ToTable:        "users",
			ToColumns:      []string{"tenant_id", "id"},
		},
	}
	warnings = s.ValidateCitusTopology()
	foundColocation := false
	for _, w := range warnings {
		if strings.Contains(w, "spans different colocation groups") {
			foundColocation = true
			break
		}
	}
	if !foundColocation {
		t.Errorf("expected colocation group warning, got %v", warnings)
	}
}

func TestGenerateCitusTableDDL(t *testing.T) {
	s := &Schema{
		HasCitus: true,
		Columns: map[string][]Column{
			"users": {
				{Name: "id", DataType: "bigint", IsNullable: false},
				{Name: "tenant_id", DataType: "bigint", IsNullable: false},
			},
			"countries": {
				{Name: "code", DataType: "text", IsNullable: false},
			},
			"audit_logs": {
				{Name: "id", DataType: "bigint", IsNullable: false},
			},
		},
		PrimaryKeys: map[string][]string{
			"users":     {"id", "tenant_id"},
			"countries": {"code"},
		},
		CitusTables: map[string]*CitusTable{
			"users": {
				TableName:          "users",
				TableType:          CitusTableDistributed,
				DistributionColumn: "tenant_id",
			},
			"countries": {
				TableName: "countries",
				TableType: CitusTableReference,
			},
			"audit_logs": {
				TableName: "audit_logs",
				TableType: CitusTableLocal,
			},
		},
	}

	// Test GenerateCitusTableDDL directly
	distDDL := GenerateCitusTableDDL(s, "users")
	if distDDL != "SELECT create_distributed_table('users', 'tenant_id');" {
		t.Errorf("unexpected distributed DDL: %q", distDDL)
	}

	refDDL := GenerateCitusTableDDL(s, "countries")
	if refDDL != "SELECT create_reference_table('countries');" {
		t.Errorf("unexpected reference DDL: %q", refDDL)
	}

	localDDL := GenerateCitusTableDDL(s, "audit_logs")
	if localDDL != "" {
		t.Errorf("expected empty DDL for local table, got %q", localDDL)
	}

	// Test GeneratePostgresTableDDL integration
	fullDDL := GeneratePostgresTableDDL(s, "users")
	if !strings.Contains(fullDDL, "CREATE TABLE IF NOT EXISTS \"users\"") {
		t.Errorf("expected CREATE TABLE in DDL, got:\n%s", fullDDL)
	}
	if !strings.Contains(fullDDL, "SELECT create_distributed_table('users', 'tenant_id');") {
		t.Errorf("expected create_distributed_table appended to DDL, got:\n%s", fullDDL)
	}
}
