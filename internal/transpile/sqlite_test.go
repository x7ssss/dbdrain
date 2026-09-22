package transpile

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/verify"
)

func TestMapToSQLiteType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"uuid", "TEXT"},
		{"json", "TEXT"},
		{"jsonb", "TEXT"},
		{"enum", "TEXT"},
		{"text[]", "TEXT"},
		{"array", "TEXT"},
		{"inet", "TEXT"},
		{"cidr", "TEXT"},
		{"timestamp with time zone", "TEXT"},
		{"timestamptz", "TEXT"},
		{"datetime", "TEXT"},
		{"date", "TEXT"},
		{"time", "TEXT"},
		{"boolean", "INTEGER"},
		{"bool", "INTEGER"},
		{"numeric", "NUMERIC"},
		{"decimal(10,2)", "NUMERIC"},
		{"serial", "INTEGER"},
		{"bigserial", "INTEGER"},
		{"int", "INTEGER"},
		{"integer", "INTEGER"},
		{"bigint", "INTEGER"},
		{"smallint", "INTEGER"},
		{"tinyint(1)", "INTEGER"},
		{"float", "REAL"},
		{"double precision", "REAL"},
		{"real", "REAL"},
		{"bytea", "BLOB"},
		{"blob", "BLOB"},
		{"binary(16)", "BLOB"},
		{"varchar(255)", "TEXT"},
		{"text", "TEXT"},
		{"char(2)", "TEXT"},
	}

	for _, tt := range tests {
		got := MapToSQLiteType(tt.input)
		if got != tt.want {
			t.Errorf("MapToSQLiteType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestTranspileColumnDefinition(t *testing.T) {
	// Single integer PK (serial) -> INTEGER PRIMARY KEY AUTOINCREMENT
	colSerial := introspect.Column{Name: "id", DataType: "serial", IsNullable: false}
	gotSerial := TranspileColumnDefinition(colSerial, true)
	wantSerial := `"id" INTEGER PRIMARY KEY AUTOINCREMENT`
	if gotSerial != wantSerial {
		t.Errorf("TranspileColumnDefinition(serial PK) = %q, want %q", gotSerial, wantSerial)
	}

	// Single non-integer PK (UUID) -> TEXT PRIMARY KEY (omit AUTOINCREMENT)
	colUUID := introspect.Column{Name: "id", DataType: "uuid", IsNullable: false}
	gotUUID := TranspileColumnDefinition(colUUID, true)
	wantUUID := `"id" TEXT PRIMARY KEY`
	if gotUUID != wantUUID {
		t.Errorf("TranspileColumnDefinition(UUID PK) = %q, want %q", gotUUID, wantUUID)
	}

	// Regular non-null column
	colEmail := introspect.Column{Name: "email", DataType: "varchar(255)", IsNullable: false}
	gotEmail := TranspileColumnDefinition(colEmail, false)
	wantEmail := `"email" TEXT NOT NULL`
	if gotEmail != wantEmail {
		t.Errorf("TranspileColumnDefinition(email) = %q, want %q", gotEmail, wantEmail)
	}

	// Nullable column
	colBio := introspect.Column{Name: "bio", DataType: "text", IsNullable: true}
	gotBio := TranspileColumnDefinition(colBio, false)
	wantBio := `"bio" TEXT`
	if gotBio != wantBio {
		t.Errorf("TranspileColumnDefinition(bio) = %q, want %q", gotBio, wantBio)
	}
}

func TestGenerateTableDDL(t *testing.T) {
	// Test compound primary keys and compound foreign keys preservation
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"orders": {
				{Name: "tenant_id", DataType: "int", IsNullable: false},
				{Name: "order_id", DataType: "int", IsNullable: false},
				{Name: "total", DataType: "numeric(10,2)", IsNullable: false},
				{Name: "customer_tenant_id", DataType: "int", IsNullable: false},
				{Name: "customer_id", DataType: "int", IsNullable: false},
			},
		},
		PrimaryKeys: map[string][]string{
			"orders": {"tenant_id", "order_id"},
		},
		ForeignKeys: []introspect.ForeignKey{
			{
				ConstraintName: "fk_orders_customer",
				FromTable:      "orders",
				FromColumns:    []string{"customer_tenant_id", "customer_id"},
				ToTable:        "customers",
				ToColumns:      []string{"tenant_id", "id"},
			},
		},
	}

	ddl := GenerateTableDDL(schema, "orders")

	// Ensure compound primary key is preserved
	if !strings.Contains(ddl, `PRIMARY KEY ("tenant_id", "order_id")`) {
		t.Errorf("DDL missing compound PRIMARY KEY: %s", ddl)
	}

	// Ensure compound foreign key is preserved
	expectedFK := `FOREIGN KEY ("customer_tenant_id", "customer_id") REFERENCES "customers" ("tenant_id", "id")`
	if !strings.Contains(ddl, expectedFK) {
		t.Errorf("DDL missing compound FOREIGN KEY: %s", ddl)
	}

	// Ensure numeric column transpiled to NUMERIC
	if !strings.Contains(ddl, `"total" NUMERIC NOT NULL`) {
		t.Errorf("DDL missing numeric column: %s", ddl)
	}
}

func TestSQLiteCyclicForeignKeyHydration(t *testing.T) {
	ctx := context.Background()

	// In-memory SQLite database
	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite in-memory db: %v", err)
	}
	defer sqliteDB.Close()

	// Initialize performance PRAGMAs
	if err := db.InitSQLiteDB(ctx, sqliteDB); err != nil {
		t.Fatalf("init sqlite db: %v", err)
	}

	// Define cyclic schema: users <-> teams
	schema := &introspect.Schema{
		Columns: map[string][]introspect.Column{
			"users": {
				{Name: "id", DataType: "serial", IsNullable: false},
				{Name: "name", DataType: "text", IsNullable: false},
				{Name: "team_id", DataType: "int", IsNullable: true},
			},
			"teams": {
				{Name: "id", DataType: "serial", IsNullable: false},
				{Name: "name", DataType: "text", IsNullable: false},
				{Name: "leader_id", DataType: "int", IsNullable: true},
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
				ConstraintName: "fk_teams_leader",
				FromTable:      "teams",
				FromColumns:    []string{"leader_id"},
				ToTable:        "users",
				ToColumns:      []string{"id"},
			},
		},
	}

	// 1. Create transpiled schema
	ddl := GenerateSchemaDDL(schema, []string{"users", "teams"})
	if _, err := sqliteDB.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("exec transpiled ddl: %v\nDDL:\n%s", err, ddl)
	}

	// 2. Hydrate cyclic foreign keys using PRAGMA defer_foreign_keys = ON
	conn, err := sqliteDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;"); err != nil {
		t.Fatalf("enable foreign_keys: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON;"); err != nil {
		t.Fatalf("enable defer_foreign_keys: %v", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	// Insert user pointing to team 10 (which does not exist yet)
	if _, err := tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "team_id") VALUES (1, 'Alice', 10);`); err != nil {
		t.Fatalf("insert user: %v", err)
	}

	// Insert team pointing back to user 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO "teams" ("id", "name", "leader_id") VALUES (10, 'Core', 1);`); err != nil {
		t.Fatalf("insert team: %v", err)
	}

	// Commit deferred transaction - should succeed since all references exist at commit time
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deferred cyclic transaction failed: %v", err)
	}

	// 3. Verify PRAGMA foreign_key_check returns 0 violations
	violations, err := verify.RunSQLite(ctx, sqliteDB)
	if err != nil {
		t.Fatalf("verify.RunSQLite failed: %v", err)
	}

	if len(violations) != 0 {
		t.Fatalf("expected 0 foreign key violations, got %d: %+v", len(violations), violations)
	}
}
