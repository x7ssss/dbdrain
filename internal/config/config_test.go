package config

import (
	"os"
	"path/filepath"
	"testing"
)

// ---- WildcardMatch tests ----

func TestWildcardMatchExact(t *testing.T) {
	if !WildcardMatch("users", "users") {
		t.Error("exact match should succeed")
	}
	if WildcardMatch("users", "orders") {
		t.Error("different strings should not match")
	}
}

func TestWildcardMatchStar(t *testing.T) {
	if !WildcardMatch("*", "anything") {
		t.Error("* should match anything")
	}
	if !WildcardMatch("*", "") {
		t.Error("* should match empty string")
	}
}

func TestWildcardMatchSuffix(t *testing.T) {
	if !WildcardMatch("*_token", "access_token") {
		t.Error("*_token should match access_token")
	}
	if !WildcardMatch("*_token", "refresh_token") {
		t.Error("*_token should match refresh_token")
	}
	if WildcardMatch("*_token", "token_store") {
		t.Error("*_token should NOT match token_store")
	}
	if WildcardMatch("*_token", "token") {
		t.Error("*_token should NOT match 'token' (no prefix)")
	}
}

func TestWildcardMatchPrefix(t *testing.T) {
	if !WildcardMatch("user_*", "user_email") {
		t.Error("user_* should match user_email")
	}
	if WildcardMatch("user_*", "admin_email") {
		t.Error("user_* should NOT match admin_email")
	}
}

func TestWildcardMatchContains(t *testing.T) {
	if !WildcardMatch("*email*", "user_email_address") {
		t.Error("*email* should match user_email_address")
	}
	if !WildcardMatch("*email*", "email") {
		t.Error("*email* should match 'email'")
	}
}

func TestWildcardMatchMultiStar(t *testing.T) {
	if !WildcardMatch("*_*_id", "order_item_id") {
		t.Error("*_*_id should match order_item_id")
	}
	if !WildcardMatch("user*name*", "username_full") {
		t.Error("user*name* should match username_full")
	}
}

// ---- FindRule tests ----

func TestFindRuleExactMatch(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Table: "users", Column: "email", Transform: TransformFakeEmail},
		},
	}
	r := cfg.FindRule("users", "email")
	if r == nil {
		t.Fatal("expected a rule, got nil")
	}
	if r.Transform != TransformFakeEmail {
		t.Errorf("expected fake_email, got %s", r.Transform)
	}
}

func TestFindRuleWildcardTable(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Table: "*", Column: "*_token", Transform: TransformUUIDRemap},
		},
	}
	r := cfg.FindRule("sessions", "access_token")
	if r == nil {
		t.Fatal("expected wildcard table rule to match")
	}
	if r.Transform != TransformUUIDRemap {
		t.Errorf("expected uuid_remap, got %s", r.Transform)
	}
}

func TestFindRuleWildcardColumn(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Table: "users", Column: "*", Transform: TransformRedact},
		},
	}
	r := cfg.FindRule("users", "any_column")
	if r == nil {
		t.Fatal("expected wildcard column rule to match")
	}
	if r.Transform != TransformRedact {
		t.Errorf("expected redact, got %s", r.Transform)
	}
}

func TestFindRuleNoMatch(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Table: "users", Column: "email", Transform: TransformFakeEmail},
		},
	}
	r := cfg.FindRule("orders", "amount")
	if r != nil {
		t.Errorf("expected nil rule for non-matching table/column, got %+v", r)
	}
}

// Exact match should win over wildcard when both could match.
func TestFindRulePriorityExactOverWildcard(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Table: "*", Column: "email", Transform: TransformRedact},
			{Table: "users", Column: "email", Transform: TransformFakeEmail},
		},
	}
	r := cfg.FindRule("users", "email")
	if r == nil {
		t.Fatal("expected a rule")
	}
	if r.Transform != TransformFakeEmail {
		t.Errorf("exact table+column should win; expected fake_email, got %s", r.Transform)
	}
}

func TestFindRuleNilConfig(t *testing.T) {
	var cfg *Config
	if cfg.FindRule("users", "email") != nil {
		t.Error("nil config should return nil rule")
	}
}

// ---- Load tests ----

func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load("/nonexistent/path/dbdrain.yaml")
	// Explicit path that doesn't exist → should error
	if err == nil {
		t.Error("expected error for explicit missing file, got nil")
	}
	_ = cfg
}

func TestLoadDefaultMissing(t *testing.T) {
	// Empty path → probes for dbdrain.yaml in CWD; should return empty config silently.
	// We run in a temp dir that has no dbdrain.yaml.
	orig, _ := os.Getwd()
	tmp := t.TempDir()
	os.Chdir(tmp)
	defer os.Chdir(orig)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("probing for missing default config should not error: %v", err)
	}
	if len(cfg.Rules) != 0 || len(cfg.VirtualForeignKeys) != 0 {
		t.Error("empty config should have no rules or vfks")
	}
}

func TestLoadValidYAML(t *testing.T) {
	yaml := `
rules:
  - table: users
    column: email
    transform: fake_email
  - table: "*"
    column: "*_token"
    transform: uuid_remap
virtual_foreign_keys:
  - child_table: comments
    child_column: commentable_id
    parent_table_column: commentable_type
    mappings:
      post: posts.id
      video: videos.id
`
	tmp := filepath.Join(t.TempDir(), "dbdrain.yaml")
	if err := os.WriteFile(tmp, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(tmp)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(cfg.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(cfg.Rules))
	}
	if cfg.Rules[0].Table != "users" || cfg.Rules[0].Transform != TransformFakeEmail {
		t.Errorf("unexpected rule[0]: %+v", cfg.Rules[0])
	}
	if cfg.Rules[1].Column != "*_token" || cfg.Rules[1].Transform != TransformUUIDRemap {
		t.Errorf("unexpected rule[1]: %+v", cfg.Rules[1])
	}
	if len(cfg.VirtualForeignKeys) != 1 {
		t.Fatalf("expected 1 virtual FK, got %d", len(cfg.VirtualForeignKeys))
	}
	vfk := cfg.VirtualForeignKeys[0]
	if vfk.ChildTable != "comments" || vfk.ChildColumn != "commentable_id" {
		t.Errorf("unexpected VFK: %+v", vfk)
	}
	if vfk.Mappings["post"] != "posts.id" || vfk.Mappings["video"] != "videos.id" {
		t.Errorf("unexpected VFK mappings: %+v", vfk.Mappings)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(tmp, []byte("rules: [invalid yaml: :::"), 0644)
	_, err := Load(tmp)
	if err == nil {
		t.Error("expected parse error for invalid YAML")
	}
}

// ---- ParseTableColumn tests ----

func TestParseTableColumn(t *testing.T) {
	tc, err := ParseTableColumn("posts.id")
	if err != nil {
		t.Fatal(err)
	}
	if tc.Table != "posts" || tc.Column != "id" {
		t.Errorf("unexpected: %+v", tc)
	}
}

func TestParseTableColumnInvalid(t *testing.T) {
	_, err := ParseTableColumn("noDot")
	if err == nil {
		t.Error("expected error for missing dot")
	}
	_, err = ParseTableColumn(".col")
	if err == nil {
		t.Error("expected error for empty table")
	}
}
