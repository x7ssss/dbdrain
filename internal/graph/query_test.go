package graph

import (
	"reflect"
	"strings"
	"testing"
)

func TestPGArrayType(t *testing.T) {
	tests := []struct {
		dataType string
		expected string
	}{
		{"bigint", "bigint"},
		{"int8", "bigint"},
		{"integer", "bigint"},
		{"int", "bigint"},
		{"int4", "bigint"},
		{"smallint", "bigint"},
		{"serial", "bigint"},
		{"bigserial", "bigint"},
		{"uuid", "uuid"},
		{"text", "text"},
		{"varchar", "text"},
		{"character varying", "text"},
		{"unknown_type", "text"},
	}

	for _, tt := range tests {
		got := PGArrayType(tt.dataType)
		if got != tt.expected {
			t.Errorf("PGArrayType(%q) = %q, want %q", tt.dataType, got, tt.expected)
		}
	}
}

func TestBuildANYQuery(t *testing.T) {
	q := BuildANYQuery("public", "orders", []string{"id", "user_id", "total"}, "user_id", "bigint", 0)
	expected := `SELECT "id", "user_id", "total" FROM "public"."orders" WHERE "user_id" = ANY($1::bigint[])`
	if q != expected {
		t.Errorf("BuildANYQuery got %q, want %q", q, expected)
	}

	qLimit := BuildANYQuery("public", "comments", []string{"id", "post_id"}, "post_id", "uuid", 50)
	if !strings.Contains(qLimit, `LIMIT 50`) {
		t.Errorf("BuildANYQuery with limit should contain LIMIT 50, got %q", qLimit)
	}
	if !strings.Contains(qLimit, `= ANY($1::uuid[])`) {
		t.Errorf("BuildANYQuery should cast to uuid[], got %q", qLimit)
	}
}

func TestFormatANYParam(t *testing.T) {
	// Bigint with valid numbers
	ids := []string{"101", "102", "103"}
	param := FormatANYParam(ids, "bigint")
	intSlice, ok := param.([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T", param)
	}
	expectedInts := []int64{101, 102, 103}
	if !reflect.DeepEqual(intSlice, expectedInts) {
		t.Errorf("got %v, want %v", intSlice, expectedInts)
	}

	// Bigint with invalid numbers should fallback to []string
	invalidIDs := []string{"101", "not_a_number"}
	paramFallback := FormatANYParam(invalidIDs, "bigint")
	strSlice, ok := paramFallback.([]string)
	if !ok {
		t.Fatalf("expected []string fallback, got %T", paramFallback)
	}
	if !reflect.DeepEqual(strSlice, invalidIDs) {
		t.Errorf("got %v, want %v", strSlice, invalidIDs)
	}

	// UUID array should remain []string
	uuidIDs := []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"}
	uuidParam := FormatANYParam(uuidIDs, "uuid")
	uuidSlice, ok := uuidParam.([]string)
	if !ok {
		t.Fatalf("expected []string for uuid, got %T", uuidParam)
	}
	if !reflect.DeepEqual(uuidSlice, uuidIDs) {
		t.Errorf("got %v, want %v", uuidSlice, uuidIDs)
	}
}

func TestBuildTempTableJoinQuery(t *testing.T) {
	q := BuildTempTableJoinQuery("public", "order_items", "_dbdrain_k_order_items", "order_id", []string{"id", "order_id", "price"}, 100)
	if !strings.Contains(q, `INNER JOIN "_dbdrain_k_order_items" k ON k.key = t."order_id"::text`) {
		t.Errorf("expected temp table join clause, got %q", q)
	}
	if !strings.Contains(q, `LIMIT 100`) {
		t.Errorf("expected LIMIT 100, got %q", q)
	}
}
