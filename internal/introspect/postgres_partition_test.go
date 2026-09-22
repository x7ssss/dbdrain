package introspect

import (
	"strings"
	"testing"
)

func TestPartitionSchemaAndDDL(t *testing.T) {
	schema := &Schema{
		Columns: map[string][]Column{
			"orders_partitioned": {
				{Name: "id", DataType: "integer", IsNullable: false, Ordinal: 1},
				{Name: "total", DataType: "numeric(10,2)", IsNullable: false, Ordinal: 2},
				{Name: "created_at", DataType: "date", IsNullable: false, Ordinal: 3},
			},
			"orders_2026_01": {
				{Name: "id", DataType: "integer", IsNullable: false, Ordinal: 1},
				{Name: "total", DataType: "numeric(10,2)", IsNullable: false, Ordinal: 2},
				{Name: "created_at", DataType: "date", IsNullable: false, Ordinal: 3},
			},
		},
		PrimaryKeys: map[string][]string{
			"orders_partitioned": {"id", "created_at"},
		},
		PartitionedTables: map[string]*PartitionedTable{
			"orders_partitioned": {
				RootTable: "orders_partitioned",
				Strategy:  "RANGE",
				KeyDef:    "RANGE (created_at)",
				Partitions: []PartitionBound{
					{
						PartitionName: "orders_2026_01",
						Bound:         "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
					},
					{
						PartitionName: "orders_2026_02",
						Bound:         "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
					},
				},
			},
		},
		ChildToRootPartition: map[string]string{
			"orders_2026_01": "orders_partitioned",
			"orders_2026_02": "orders_partitioned",
		},
	}

	if !schema.IsPartitionedRoot("orders_partitioned") {
		t.Errorf("expected orders_partitioned to be a partitioned root")
	}
	if schema.IsChildPartition("orders_partitioned") {
		t.Errorf("expected orders_partitioned NOT to be a child partition")
	}
	if !schema.IsChildPartition("orders_2026_01") {
		t.Errorf("expected orders_2026_01 to be a child partition")
	}
	if schema.RootPartition("orders_2026_01") != "orders_partitioned" {
		t.Errorf("expected RootPartition to return orders_partitioned, got %q", schema.RootPartition("orders_2026_01"))
	}

	ddl := GeneratePostgresTableDDL(schema, "orders_partitioned")
	if !strings.Contains(ddl, `CREATE TABLE IF NOT EXISTS "orders_partitioned"`) {
		t.Errorf("missing root table DDL: %s", ddl)
	}
	if !strings.Contains(ddl, `PARTITION BY RANGE (created_at)`) {
		t.Errorf("missing partition strategy clause: %s", ddl)
	}
	if !strings.Contains(ddl, `CREATE TABLE IF NOT EXISTS "orders_2026_01" PARTITION OF "orders_partitioned" FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')`) {
		t.Errorf("missing child partition 1 DDL: %s", ddl)
	}
	if !strings.Contains(ddl, `CREATE TABLE IF NOT EXISTS "orders_2026_02" PARTITION OF "orders_partitioned" FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')`) {
		t.Errorf("missing child partition 2 DDL: %s", ddl)
	}
}
