package introspect

import (
	"fmt"
	"testing"
)

// mockScanner implements RowScanner for testing parser functions.
type mockScanner struct {
	rows   [][]any
	cursor int
	err    error
}

func newMockScanner(rows [][]any) *mockScanner {
	return &mockScanner{rows: rows, cursor: -1}
}

func (m *mockScanner) Next() bool {
	m.cursor++
	return m.cursor < len(m.rows)
}

func (m *mockScanner) Scan(dest ...any) error {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return fmt.Errorf("out of range")
	}
	row := m.rows[m.cursor]
	if len(dest) != len(row) {
		return fmt.Errorf("destination count %d does not match row count %d", len(dest), len(row))
	}
	for i, v := range row {
		switch d := dest[i].(type) {
		case *string:
			*d = fmt.Sprintf("%v", v)
		case *int:
			*d = v.(int)
		case *bool:
			*d = v.(bool)
		default:
			return fmt.Errorf("unsupported type %T", dest[i])
		}
	}
	return nil
}

func (m *mockScanner) Err() error {
	return m.err
}

func (m *mockScanner) Close() error {
	return nil
}

func TestParseTableEngines(t *testing.T) {
	mock := newMockScanner([][]any{
		{"users", "InnoDB"},
		{"orders", "InnoDB"},
		{"legacy_audit", "MyISAM"},
		{"ephemeral_cache", "MEMORY"},
	})

	engines, warnings, err := ParseTableEngines(mock)
	if err != nil {
		t.Fatalf("ParseTableEngines failed: %v", err)
	}

	if engines["users"] != "InnoDB" {
		t.Errorf("expected users to be InnoDB, got %q", engines["users"])
	}
	if engines["legacy_audit"] != "MyISAM" {
		t.Errorf("expected legacy_audit to be MyISAM, got %q", engines["legacy_audit"])
	}

	if len(warnings) != 2 {
		t.Fatalf("expected 2 warnings for MyISAM and MEMORY, got %d: %v", len(warnings), warnings)
	}
}

func TestParseColumns(t *testing.T) {
	mock := newMockScanner([][]any{
		{"users", "id", "int", "NO", 1},
		{"users", "email", "varchar", "NO", 2},
		{"users", "manager_id", "int", "YES", 3},
	})

	cols, err := ParseColumns(mock)
	if err != nil {
		t.Fatalf("ParseColumns failed: %v", err)
	}

	userCols, ok := cols["users"]
	if !ok || len(userCols) != 3 {
		t.Fatalf("expected 3 columns for users, got %d", len(userCols))
	}

	if userCols[0].Name != "id" || userCols[0].IsNullable {
		t.Errorf("id: expected non-nullable, got %+v", userCols[0])
	}
	if userCols[2].Name != "manager_id" || !userCols[2].IsNullable {
		t.Errorf("manager_id: expected nullable, got %+v", userCols[2])
	}
}

func TestParsePrimaryKeys(t *testing.T) {
	mock := newMockScanner([][]any{
		{"users", "id"},
		{"order_items", "order_id"},
		{"order_items", "item_seq"},
	})

	pks, err := ParsePrimaryKeys(mock)
	if err != nil {
		t.Fatalf("ParsePrimaryKeys failed: %v", err)
	}

	if len(pks["users"]) != 1 || pks["users"][0] != "id" {
		t.Errorf("users pk mismatch: %+v", pks["users"])
	}

	expectedOrderItems := []string{"order_id", "item_seq"}
	if len(pks["order_items"]) != 2 {
		t.Fatalf("expected 2 columns in order_items PK, got %d", len(pks["order_items"]))
	}
	for i, col := range expectedOrderItems {
		if pks["order_items"][i] != col {
			t.Errorf("expected pk col %d to be %q, got %q", i, col, pks["order_items"][i])
		}
	}
}

// TestCompoundForeignKeyOrderingPreservation verifies that compound foreign key columns
// are strictly ordered by ORDINAL_POSITION even when database rows are returned out-of-order.
func TestCompoundForeignKeyOrderingPreservation(t *testing.T) {
	// Intentionally feed rows out of order (ordinal 3, then 1, then 2)
	mock := newMockScanner([][]any{
		{"fk_compound_tenant_user_role", "memberships", "role_id", "roles", "id", 3},
		{"fk_compound_tenant_user_role", "memberships", "tenant_id", "roles", "tenant_id", 1},
		{"fk_compound_tenant_user_role", "memberships", "user_id", "roles", "user_id", 2},
	})

	fks, err := ParseForeignKeys(mock)
	if err != nil {
		t.Fatalf("ParseForeignKeys failed: %v", err)
	}

	if len(fks) != 1 {
		t.Fatalf("expected 1 compound foreign key, got %d", len(fks))
	}

	fk := fks[0]
	if fk.FromTable != "memberships" || fk.ToTable != "roles" {
		t.Errorf("unexpected table mapping: %s -> %s", fk.FromTable, fk.ToTable)
	}

	expectedFromCols := []string{"tenant_id", "user_id", "role_id"}
	expectedToCols := []string{"tenant_id", "user_id", "id"}

	if len(fk.FromColumns) != 3 || len(fk.ToColumns) != 3 {
		t.Fatalf("expected 3 compound cols, got from=%v, to=%v", fk.FromColumns, fk.ToColumns)
	}

	for i := range expectedFromCols {
		if fk.FromColumns[i] != expectedFromCols[i] {
			t.Errorf("FromColumns[%d]: expected %q, got %q", i, expectedFromCols[i], fk.FromColumns[i])
		}
		if fk.ToColumns[i] != expectedToCols[i] {
			t.Errorf("ToColumns[%d]: expected %q, got %q", i, expectedToCols[i], fk.ToColumns[i])
		}
	}
}
