package db

import (
	"testing"
)

func TestDetectEngine(t *testing.T) {
	tests := []struct {
		uri      string
		expected EngineType
		wantErr  bool
	}{
		{"postgres://user:pass@localhost:5432/db", EnginePostgres, false},
		{"postgresql://user:pass@localhost:5432/db", EnginePostgres, false},
		{"mysql://root:pass@localhost:3306/db", EngineMySQL, false},
		{"mariadb://root:pass@localhost:3306/db", EngineMySQL, false},
		{"root:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4", EngineMySQL, false},
		{"root:pass@unix(/tmp/mysql.sock)/db", EngineMySQL, false},
		{"sqlite://file.db", "", true},
		{"invalid://uri", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		got, err := DetectEngine(tt.uri)
		if (err != nil) != tt.wantErr {
			t.Errorf("DetectEngine(%q) error = %v, wantErr %v", tt.uri, err, tt.wantErr)
			continue
		}
		if got != tt.expected {
			t.Errorf("DetectEngine(%q) = %v, want %v", tt.uri, got, tt.expected)
		}
	}
}

func TestParseMySQLDSN(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantDSN    string
		wantDBName string
	}{
		{
			name:       "mysql url with port and db",
			uri:        "mysql://root:secret@127.0.0.1:3306/my_database",
			wantDSN:    "root:secret@tcp(127.0.0.1:3306)/my_database?multiStatements=true&parseTime=true",
			wantDBName: "my_database",
		},
		{
			name:       "mariadb url without port",
			uri:        "mariadb://app_user:pass@dbhost/production",
			wantDSN:    "app_user:pass@tcp(dbhost:3306)/production?multiStatements=true&parseTime=true",
			wantDBName: "production",
		},
		{
			name:       "standard mysql driver DSN format",
			uri:        "user:password@tcp(localhost:3306)/custom_db?charset=utf8mb4",
			wantDSN:    "user:password@tcp(localhost:3306)/custom_db?charset=utf8mb4",
			wantDBName: "custom_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, dbName, err := ParseMySQLDSN(tt.uri)
			if err != nil {
				t.Fatalf("ParseMySQLDSN failed: %v", err)
			}
			if dbName != tt.wantDBName {
				t.Errorf("dbName = %q, want %q", dbName, tt.wantDBName)
			}
			if dsn != tt.wantDSN {
				t.Errorf("dsn = %q, want %q", dsn, tt.wantDSN)
			}
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	if got := QuoteIdent(EnginePostgres, "users"); got != `"users"` {
		t.Errorf("QuoteIdent(Postgres, users) = %q, want %q", got, `"users"`)
	}
	if got := QuoteIdent(EnginePostgres, `user"name`); got != `"user""name"` {
		t.Errorf("QuoteIdent(Postgres, user\"name) = %q, want %q", got, `"user""name"`)
	}

	if got := QuoteIdent(EngineMySQL, "users"); got != "`users`" {
		t.Errorf("QuoteIdent(MySQL, users) = %q, want %q", got, "`users`")
	}
	if got := QuoteIdent(EngineMySQL, "user`name"); got != "`user``name`" {
		t.Errorf("QuoteIdent(MySQL, user`name) = %q, want %q", got, "`user``name`")
	}
}

func TestQuoteTable(t *testing.T) {
	if got := QuoteTable(EnginePostgres, "public", "users"); got != `"public"."users"` {
		t.Errorf("QuoteTable(Postgres, public, users) = %q, want %q", got, `"public"."users"`)
	}
	if got := QuoteTable(EnginePostgres, "", "users"); got != `"users"` {
		t.Errorf("QuoteTable(Postgres, '', users) = %q, want %q", got, `"users"`)
	}

	if got := QuoteTable(EngineMySQL, "mydb", "users"); got != "`mydb`.`users`" {
		t.Errorf("QuoteTable(MySQL, mydb, users) = %q, want %q", got, "`mydb`.`users`")
	}
	if got := QuoteTable(EngineMySQL, "", "users"); got != "`users`" {
		t.Errorf("QuoteTable(MySQL, '', users) = %q, want %q", got, "`users`")
	}
}
