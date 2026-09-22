package graph

import (
	"fmt"
	"strings"
	"testing"
)

func TestKeysetQueuePagination(t *testing.T) {
	q := NewKeysetQueue()

	// Enqueue 25 items across 3 tables in arbitrary order
	tables := []string{"users", "orders", "audit_logs"}
	for i := 1; i <= 25; i++ {
		tbl := tables[i%len(tables)]
		pk := fmt.Sprintf("%03d", i)
		q.Enqueue(tbl, pk)
	}

	// Re-enqueue existing item to verify deduplication
	if q.Enqueue("users", "003") {
		t.Fatalf("expected duplicate item to be rejected")
	}

	if q.Len() != 25 {
		t.Fatalf("expected 25 items, got %d", q.Len())
	}

	// Paginate with batchSize = 7
	batchSize := 7
	var allRetrieved []KeysetItem
	var cursor *KeysetCursor
	hasMore := true
	pageCount := 0

	for hasMore {
		var page []KeysetItem
		page, cursor, hasMore = q.SeekPage(cursor, batchSize)
		pageCount++
		allRetrieved = append(allRetrieved, page...)
	}

	if len(allRetrieved) != 25 {
		t.Fatalf("expected 25 items retrieved, got %d", len(allRetrieved))
	}

	// Verify items are strictly sorted by (Table, PK)
	for i := 1; i < len(allRetrieved); i++ {
		prev := allRetrieved[i-1]
		curr := allRetrieved[i]
		if CompareKeyset(prev.Table, prev.PK, curr.Table, curr.PK) >= 0 {
			t.Fatalf("items out of order: prev=%+v, curr=%+v", prev, curr)
		}
	}

	// Test Paginate helper
	var paginated []KeysetItem
	err := q.Paginate(5, func(items []KeysetItem) error {
		paginated = append(paginated, items...)
		return nil
	})
	if err != nil {
		t.Fatalf("Paginate error: %v", err)
	}
	if len(paginated) != 25 {
		t.Fatalf("expected 25 items via Paginate, got %d", len(paginated))
	}
}

func TestBuildKeysetSeekQuery(t *testing.T) {
	cols := []string{"id", "user_id", "total"}

	// 1. PostgreSQL first page (no cursor)
	pgFirst := BuildKeysetSeekQuery("postgres", "public", "orders", cols, "user_id", "id", 100, false)
	if strings.Contains(pgFirst, "WHERE") {
		t.Fatalf("first page query should not contain WHERE: %s", pgFirst)
	}
	if !strings.Contains(pgFirst, `ORDER BY "user_id", "id" LIMIT 100`) {
		t.Fatalf("first page query missing ORDER BY/LIMIT: %s", pgFirst)
	}

	// 2. PostgreSQL subsequent page (with cursor)
	pgNext := BuildKeysetSeekQuery("postgres", "public", "orders", cols, "user_id", "id", 100, true)
	if !strings.Contains(pgNext, `WHERE ("user_id", "id") > ($1, $2)`) {
		t.Fatalf("subsequent page query missing composite keyset WHERE: %s", pgNext)
	}
	if !strings.Contains(pgNext, `ORDER BY "user_id", "id" LIMIT 100`) {
		t.Fatalf("subsequent page query missing ORDER BY/LIMIT: %s", pgNext)
	}

	// 3. MySQL first page
	myFirst := BuildKeysetSeekQuery("mysql", "shop", "orders", cols, "user_id", "id", 50, false)
	if strings.Contains(myFirst, "WHERE") {
		t.Fatalf("first page mysql query should not contain WHERE: %s", myFirst)
	}
	if !strings.Contains(myFirst, "ORDER BY `user_id`, `id` LIMIT 50") {
		t.Fatalf("first page mysql query missing ORDER BY/LIMIT: %s", myFirst)
	}

	// 4. MySQL subsequent page
	myNext := BuildKeysetSeekQuery("mysql", "shop", "orders", cols, "user_id", "id", 50, true)
	if !strings.Contains(myNext, "WHERE (`user_id`, `id`) > (?, ?)") {
		t.Fatalf("subsequent page mysql query missing composite keyset WHERE: %s", myNext)
	}
}

func TestBuildStratifiedChildQuery(t *testing.T) {
	cols := []string{"id", "user_id", "status", "created_at"}

	// PostgreSQL
	pgQuery := BuildStratifiedChildQuery("public", "orders", cols, "user_id", "id", "bigint", 3, 0)
	if !strings.Contains(pgQuery, "PARTITION BY c.\"user_id\"") {
		t.Fatalf("missing PARTITION BY: %s", pgQuery)
	}
	if !strings.Contains(pgQuery, "ORDER BY MD5(CAST(c.\"id\" AS text) || $2)") {
		t.Fatalf("missing deterministic MD5 hash ordering: %s", pgQuery)
	}
	if !strings.Contains(pgQuery, "rn = 1 OR (rn <= 3 AND total_children > 1)") {
		t.Fatalf("missing stratified sampling filter: %s", pgQuery)
	}

	// MySQL
	myQuery := BuildStratifiedChildQueryMySQL("shop", "orders", cols, "user_id", "id", "?, ?, ?", 5, 1000)
	if !strings.Contains(myQuery, "PARTITION BY c.`user_id`") {
		t.Fatalf("missing mysql PARTITION BY: %s", myQuery)
	}
	if !strings.Contains(myQuery, "ORDER BY MD5(CONCAT(CAST(c.`id` AS CHAR), ?))") {
		t.Fatalf("missing mysql deterministic MD5 ordering: %s", myQuery)
	}
	if !strings.Contains(myQuery, "rn = 1 OR (rn <= 5 AND total_children > 1)") {
		t.Fatalf("missing mysql stratified sampling filter: %s", myQuery)
	}
	if !strings.Contains(myQuery, "LIMIT 1000") {
		t.Fatalf("missing LIMIT: %s", myQuery)
	}
}
