package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Transform names supported in dbdrain.yaml rules.
const (
	TransformFakeEmail Transform = "fake_email"  // HMAC email → anon_<hash>@drain.local
	TransformRedact    Transform = "redact"       // fixed placeholder [REDACTED]
	TransformUUIDRemap Transform = "uuid_remap"  // deterministic UUID from HMAC
	TransformHMACPhone Transform = "hmac_phone"  // HMAC phone → +1555<hash>
	TransformHMACName  Transform = "hmac_name"   // HMAC name → User_<hash>
	TransformPasswd    Transform = "redact_password" // bcrypt placeholder
	TransformToken     Transform = "hmac_token"  // HMAC token → tok_<hash>
)

// Transform is the masking transform identifier.
type Transform string

// Rule defines a deterministic masking rule for a specific table/column pattern.
// Both Table and Column support wildcard "*" and glob patterns like "*_token".
type Rule struct {
	Table     string    `yaml:"table"`
	Column    string    `yaml:"column"`
	Transform Transform `yaml:"transform"`
}

// TableColumn references a specific column in a specific table.
type TableColumn struct {
	Table  string
	Column string
}

// ParseTableColumn parses a "table.column" string.
func ParseTableColumn(s string) (TableColumn, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return TableColumn{}, fmt.Errorf("expected format 'table.column', got %q", s)
	}
	return TableColumn{Table: parts[0], Column: parts[1]}, nil
}

// VirtualForeignKey defines a polymorphic relationship that isn't represented as a
// real database FK constraint — e.g. Rails-style polymorphic associations.
type VirtualForeignKey struct {
	ChildTable        string             `yaml:"child_table"`
	ChildColumn       string             `yaml:"child_column"`       // FK value column (commentable_id)
	ParentTableColumn string             `yaml:"parent_table_column"` // discriminator column (commentable_type)
	Mappings          map[string]string  `yaml:"mappings"`            // "post" -> "posts.id"
}

// RestrictionValue represents the restriction field of an Association.
// It can be a boolean (e.g. `false` to skip FK traversal entirely)
// or a SQL predicate string (e.g. `"event_type = 'billing'"`).
type RestrictionValue struct {
	Skip      bool   // true when restriction: false
	Condition string // SQL predicate when restriction is a string
}

func (r *RestrictionValue) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	var b bool
	if err := value.Decode(&b); err == nil {
		r.Skip = !b // restriction: false -> Skip is true
		r.Condition = ""
		return nil
	}
	var s string
	if err := value.Decode(&s); err == nil {
		r.Skip = false
		r.Condition = s
		return nil
	}
	return fmt.Errorf("restriction must be boolean or string, got %v", value.Tag)
}

func (r RestrictionValue) MarshalYAML() (any, error) {
	if r.Skip {
		return false, nil
	}
	if r.Condition != "" {
		return r.Condition, nil
	}
	return nil, nil
}

// Association defines relationship restrictions or conditional SQL predicates on FK traversals.
type Association struct {
	Source      string           `yaml:"source"`
	Target      string           `yaml:"target"`
	Restriction RestrictionValue `yaml:"restriction"`
}

// IsSkipped returns true if this association is explicitly restricted/skipped.
func (a *Association) IsSkipped() bool {
	if a == nil {
		return false
	}
	return a.Restriction.Skip
}

// Condition returns the conditional SQL predicate string, if any.
func (a *Association) Condition() string {
	if a == nil {
		return ""
	}
	return a.Restriction.Condition
}

// Config is the top-level structure for dbdrain.yaml.
type Config struct {
	Rules              []Rule              `yaml:"rules"`
	VirtualForeignKeys []VirtualForeignKey `yaml:"virtual_foreign_keys"`
	Associations       []Association       `yaml:"associations"`
}

// Load reads the YAML config from path. Returns an empty Config if the file does not exist.
// An explicit empty path ("") causes the loader to probe for "dbdrain.yaml" in the working directory.
func Load(path string) (*Config, error) {
	probe := path == ""
	if probe {
		path = "dbdrain.yaml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && probe {
			return &Config{}, nil // silently skip missing default config
		}
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	return &cfg, nil
}

// FindRule returns the best-matching Rule for (table, column), or nil if none match.
// Priority (highest first):
//  1. exact table + exact column
//  2. exact table + wildcard/glob column
//  3. wildcard/glob table + exact column
//  4. wildcard/glob table + wildcard/glob column
func (c *Config) FindRule(table, column string) *Rule {
	if c == nil {
		return nil
	}
	best := -1
	var found *Rule
	for i := range c.Rules {
		r := &c.Rules[i]
		score := matchScore(r.Table, table, r.Column, column)
		if score > best {
			best = score
			found = r
		}
	}
	if best < 0 {
		return nil
	}
	return found
}

// FindAssociation returns the Association matching (source, target), or nil if none match.
func (c *Config) FindAssociation(source, target string) *Association {
	if c == nil {
		return nil
	}
	for i := range c.Associations {
		a := &c.Associations[i]
		if (a.Source == source || WildcardMatch(a.Source, source)) &&
			(a.Target == target || WildcardMatch(a.Target, target)) {
			return a
		}
	}
	return nil
}

// matchScore rates how specifically a (ruleTable, ruleCol) pair matches (table, col).
// Returns -1 for no match.
func matchScore(ruleTable, table, ruleCol, col string) int {
	if !WildcardMatch(ruleTable, table) || !WildcardMatch(ruleCol, col) {
		return -1
	}
	score := 0
	if ruleTable == table { // exact table match
		score += 4
	} else if ruleTable != "*" {
		score += 2 // glob table match
	}
	if ruleCol == col { // exact column match
		score += 2
	} else if ruleCol != "*" {
		score += 1 // glob column match
	}
	return score
}

// WildcardMatch returns true if pattern matches value.
// Supports:
//   - "*"          → matches everything
//   - "exact"      → exact equality
//   - "*_suffix"   → HasSuffix
//   - "prefix_*"   → HasPrefix
//   - "*infix*"    → Contains
//   - multi-*      → full DP glob match
func WildcardMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == value
	}
	// Single-star optimisations.
	if strings.Count(pattern, "*") == 1 {
		idx := strings.Index(pattern, "*")
		prefix := pattern[:idx]
		suffix := pattern[idx+1:]
		return strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix) &&
			len(value) >= len(prefix)+len(suffix)
	}
	// Multi-star: DP glob.
	return dpGlob(pattern, value)
}

// dpGlob implements full glob matching with multiple '*' wildcards.
func dpGlob(pattern, s string) bool {
	p := []rune(pattern)
	r := []rune(s)
	dp := make([][]bool, len(p)+1)
	for i := range dp {
		dp[i] = make([]bool, len(r)+1)
	}
	dp[0][0] = true
	for i := 1; i <= len(p); i++ {
		if p[i-1] == '*' {
			dp[i][0] = dp[i-1][0]
		}
	}
	for i := 1; i <= len(p); i++ {
		for j := 1; j <= len(r); j++ {
			if p[i-1] == '*' {
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			} else if p[i-1] == r[j-1] {
				dp[i][j] = dp[i-1][j-1]
			}
		}
	}
	return dp[len(p)][len(r)]
}
