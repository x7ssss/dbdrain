package drain

import (
	"testing"
	"time"

	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/graph"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// TestTwoPhaseSelfReferentialCycle verifies that a self-referencing cyclic FK (e.g. users.manager_id)
// is coerced to NULL during Phase 1 INSERT and restored via Phase 2 UPDATE.
func TestTwoPhaseSelfReferentialCycle(t *testing.T) {
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"users": {
				{Name: "id", DataType: "int", IsNullable: false, Ordinal: 1},
				{Name: "name", DataType: "varchar", IsNullable: false, Ordinal: 2},
				{Name: "manager_id", DataType: "int", IsNullable: true, Ordinal: 3},
			},
		},
		PrimaryKeys: map[string][]string{
			"users": {"id"},
		},
		ForeignKeys: []introspect.ForeignKey{
			{
				ConstraintName: "fk_users_manager",
				FromTable:      "users",
				FromColumns:    []string{"manager_id"},
				ToTable:        "users",
				ToColumns:      []string{"id"},
			},
		},
	}

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	resolver := NewTwoPhaseResolver(schema, g, sccs, db.EngineMySQL, "")

	if !resolver.IsCyclicNullableFK("users", "manager_id") {
		t.Fatal("expected users.manager_id to be recognized as a cyclic nullable FK")
	}

	// Row: id=1, name="Alice", manager_id=2
	cols := schema.Columns["users"]
	rowVals := []any{1, "Alice", 2}

	insertSQL, updates := resolver.GeneratePhase1Insert("users", cols, rowVals)

	// Verify Phase 1: manager_id coerced to NULL and uses backticks
	expectedInsert := "INSERT INTO `users` (`id`, `name`, `manager_id`) VALUES (1, 'Alice', NULL);"
	if insertSQL != expectedInsert {
		t.Errorf("Phase 1 mismatch:\nwant: %s\ngot:  %s", expectedInsert, insertSQL)
	}

	if len(updates) != 1 {
		t.Fatalf("expected 1 Phase 2 update, got %d", len(updates))
	}

	// Verify Phase 2: UPDATE statement
	updateSQL := resolver.GeneratePhase2Update(updates[0])
	expectedUpdate := "UPDATE `users` SET `manager_id` = 2 WHERE `id` = 1;"
	if updateSQL != expectedUpdate {
		t.Errorf("Phase 2 mismatch:\nwant: %s\ngot:  %s", expectedUpdate, updateSQL)
	}
}

// TestTwoPhaseMutualCycle verifies that mutually dependent tables (e.g. users <-> teams)
// have their cyclic nullable FKs coerced to NULL in Phase 1 and updated in Phase 2.
func TestTwoPhaseMutualCycle(t *testing.T) {
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"users": {
				{Name: "id", DataType: "int", IsNullable: false, Ordinal: 1},
				{Name: "name", DataType: "varchar", IsNullable: false, Ordinal: 2},
				{Name: "team_id", DataType: "int", IsNullable: true, Ordinal: 3},
			},
			"teams": {
				{Name: "id", DataType: "int", IsNullable: false, Ordinal: 1},
				{Name: "title", DataType: "varchar", IsNullable: false, Ordinal: 2},
				{Name: "lead_id", DataType: "int", IsNullable: true, Ordinal: 3},
			},
		},
		PrimaryKeys: map[string][]string{
			"users": {"id"},
			"teams": {"id"},
		},
		ForeignKeys: []introspect.ForeignKey{
			{
				ConstraintName: "fk_users_team",
				FromTable:      "users",
				FromColumns:    []string{"team_id"},
				ToTable:        "teams",
				ToColumns:      []string{"id"},
			},
			{
				ConstraintName: "fk_teams_lead",
				FromTable:      "teams",
				FromColumns:    []string{"lead_id"},
				ToTable:        "users",
				ToColumns:      []string{"id"},
			},
		},
	}

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)

	resolver := NewTwoPhaseResolver(schema, g, sccs, db.EngineMySQL, "production")

	if !resolver.IsCyclicNullableFK("users", "team_id") {
		t.Error("expected users.team_id to be cyclic nullable")
	}
	if !resolver.IsCyclicNullableFK("teams", "lead_id") {
		t.Error("expected teams.lead_id to be cyclic nullable")
	}

	// 1. Insert user
	userCols := schema.Columns["users"]
	userInsert, userUpdates := resolver.GeneratePhase1Insert("users", userCols, []any{10, "Alice", 1})
	expectedUserInsert := "INSERT INTO `production`.`users` (`id`, `name`, `team_id`) VALUES (10, 'Alice', NULL);"
	if userInsert != expectedUserInsert {
		t.Errorf("User insert mismatch:\nwant: %s\ngot:  %s", expectedUserInsert, userInsert)
	}

	// 2. Insert team
	teamCols := schema.Columns["teams"]
	teamInsert, teamUpdates := resolver.GeneratePhase1Insert("teams", teamCols, []any{1, "Core Team", 10})
	expectedTeamInsert := "INSERT INTO `production`.`teams` (`id`, `title`, `lead_id`) VALUES (1, 'Core Team', NULL);"
	if teamInsert != expectedTeamInsert {
		t.Errorf("Team insert mismatch:\nwant: %s\ngot:  %s", expectedTeamInsert, teamInsert)
	}

	resolver.RecordPendingUpdates(userUpdates)
	resolver.RecordPendingUpdates(teamUpdates)

	flushedUpdates := resolver.FlushPendingUpdates()
	if len(flushedUpdates) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(flushedUpdates))
	}

	expectedU1 := "UPDATE `production`.`users` SET `team_id` = 1 WHERE `id` = 10;"
	expectedU2 := "UPDATE `production`.`teams` SET `lead_id` = 10 WHERE `id` = 1;"

	if flushedUpdates[0] != expectedU1 {
		t.Errorf("Update 1 mismatch:\nwant: %s\ngot:  %s", expectedU1, flushedUpdates[0])
	}
	if flushedUpdates[1] != expectedU2 {
		t.Errorf("Update 2 mismatch:\nwant: %s\ngot:  %s", expectedU2, flushedUpdates[1])
	}
}

// TestTwoPhaseCompoundPrimaryKey verifies that compound primary keys are correctly formatted
// in the WHERE clause of Phase 2 updates with AND conjunction.
func TestTwoPhaseCompoundPrimaryKey(t *testing.T) {
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"nodes": {
				{Name: "tenant_id", DataType: "int", IsNullable: false, Ordinal: 1},
				{Name: "node_id", DataType: "int", IsNullable: false, Ordinal: 2},
				{Name: "parent_node_id", DataType: "int", IsNullable: true, Ordinal: 3},
			},
		},
		PrimaryKeys: map[string][]string{
			"nodes": {"tenant_id", "node_id"},
		},
		ForeignKeys: []introspect.ForeignKey{
			{
				ConstraintName: "fk_nodes_parent",
				FromTable:      "nodes",
				FromColumns:    []string{"parent_node_id"},
				ToTable:        "nodes",
				ToColumns:      []string{"node_id"},
			},
		},
	}

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	resolver := NewTwoPhaseResolver(schema, g, sccs, db.EngineMySQL, "")

	cols := schema.Columns["nodes"]
	_, updates := resolver.GeneratePhase1Insert("nodes", cols, []any{100, 5, 2})

	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}

	updateSQL := resolver.GeneratePhase2Update(updates[0])
	expected := "UPDATE `nodes` SET `parent_node_id` = 2 WHERE `tenant_id` = 100 AND `node_id` = 5;"
	if updateSQL != expected {
		t.Errorf("Compound PK update mismatch:\nwant: %s\ngot:  %s", expected, updateSQL)
	}
}

// TestTwoPhaseAlreadyNullValue verifies that a row with an already NULL cyclic FK does not
// generate an unnecessary Phase 2 UPDATE.
func TestTwoPhaseAlreadyNullValue(t *testing.T) {
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"users": {
				{Name: "id", DataType: "int", IsNullable: false, Ordinal: 1},
				{Name: "manager_id", DataType: "int", IsNullable: true, Ordinal: 2},
			},
		},
		PrimaryKeys: map[string][]string{
			"users": {"id"},
		},
		ForeignKeys: []introspect.ForeignKey{
			{
				ConstraintName: "fk_users_self",
				FromTable:      "users",
				FromColumns:    []string{"manager_id"},
				ToTable:        "users",
				ToColumns:      []string{"id"},
			},
		},
	}

	g := graph.New(schema)
	sccs := graph.TarjanSCC(g)
	resolver := NewTwoPhaseResolver(schema, g, sccs, db.EngineMySQL, "")

	cols := schema.Columns["users"]
	insertSQL, updates := resolver.GeneratePhase1Insert("users", cols, []any{1, nil})

	expectedInsert := "INSERT INTO `users` (`id`, `manager_id`) VALUES (1, NULL);"
	if insertSQL != expectedInsert {
		t.Errorf("expected %s, got %s", expectedInsert, insertSQL)
	}

	if len(updates) != 0 {
		t.Errorf("expected 0 updates for already-NULL value, got %d", len(updates))
	}
}

// TestMySQLDataFormatting verifies that booleans, dates, binary, and JSON types
// format to standard MySQL syntax.
func TestMySQLDataFormatting(t *testing.T) {
	cols := []introspect.Column{
		{Name: "is_active", DataType: "boolean"},
		{Name: "created_at", DataType: "datetime"},
		{Name: "payload", DataType: "json"},
		{Name: "bin_hash", DataType: "binary"},
	}

	ts := time.Date(2026, 9, 22, 12, 30, 45, 0, time.UTC)
	vals := []any{
		true,
		ts,
		[]byte(`{"status":"ok","count":42}`),
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},
	}

	formatted := FormatValues(db.EngineMySQL, cols, vals)

	// Booleans in MySQL: 1
	// Datetime: '2026-09-22 12:30:45'
	// JSON: '{"status":"ok","count":42}'
	// Binary: X'DEADBEEF'
	expected := `1, '2026-09-22 12:30:45', '{\"status\":\"ok\",\"count\":42}', X'DEADBEEF'`
	if formatted != expected {
		t.Errorf("MySQL formatting mismatch:\nwant: %s\ngot:  %s", expected, formatted)
	}
}
