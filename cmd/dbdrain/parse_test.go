package main

import (
	"testing"
)

func TestParseFromExpr(t *testing.T) {
	tests := []struct {
		input       string
		wantTable   string
		wantWhere   string
		wantLimit   int
		expectError bool
	}{
		{
			input:       "users LIMIT 20",
			wantTable:   "users",
			wantWhere:   "",
			wantLimit:   20,
			expectError: false,
		},
		{
			input:       "tenants WHERE tier = 'enterprise'",
			wantTable:   "tenants",
			wantWhere:   "tier = 'enterprise'",
			wantLimit:   0,
			expectError: false,
		},
		{
			input:       "orders WHERE status IN ('active', 'pending') LIMIT 50",
			wantTable:   "orders",
			wantWhere:   "status IN ('active', 'pending')",
			wantLimit:   50,
			expectError: false,
		},
		{
			input:       "audit_logs",
			wantTable:   "audit_logs",
			wantWhere:   "",
			wantLimit:   0,
			expectError: false,
		},
		{
			input:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		table, where, limit, err := parseFromExpr(tt.input)
		if tt.expectError {
			if err == nil {
				t.Errorf("parseFromExpr(%q): expected error, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseFromExpr(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if table != tt.wantTable || where != tt.wantWhere || limit != tt.wantLimit {
			t.Errorf("parseFromExpr(%q) = (%q, %q, %d), want (%q, %q, %d)",
				tt.input, table, where, limit, tt.wantTable, tt.wantWhere, tt.wantLimit)
		}
	}
}

func TestParseFromExprs(t *testing.T) {
	inputs := []string{
		"users LIMIT 20",
		"tenants WHERE tier = 'enterprise'",
		"orders WHERE id IN (1, 2, 3) LIMIT 10",
	}

	anchors, err := parseFromExprs(inputs)
	if err != nil {
		t.Fatalf("parseFromExprs unexpected error: %v", err)
	}

	if len(anchors) != 3 {
		t.Fatalf("expected 3 anchors, got %d", len(anchors))
	}

	if anchors[0].Table != "users" || anchors[0].Limit != 20 {
		t.Errorf("anchor[0] mismatch: %+v", anchors[0])
	}
	if anchors[1].Table != "tenants" || anchors[1].Where != "tier = 'enterprise'" {
		t.Errorf("anchor[1] mismatch: %+v", anchors[1])
	}
	if anchors[2].Table != "orders" || anchors[2].Where != "id IN (1, 2, 3)" || anchors[2].Limit != 10 {
		t.Errorf("anchor[2] mismatch: %+v", anchors[2])
	}

	// Empty slice error check
	if _, err := parseFromExprs(nil); err == nil {
		t.Errorf("expected error on empty inputs, got nil")
	}
}
