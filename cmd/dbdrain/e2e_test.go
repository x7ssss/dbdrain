package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	_ "modernc.org/sqlite"
	anon "github.com/x7ssss/dbdrain/internal/anonymize"
	"github.com/x7ssss/dbdrain/internal/introspect"
	"github.com/x7ssss/dbdrain/internal/verify"
)

func findAvailablePort(preferred uint32) uint32 {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", preferred))
	if err == nil {
		_ = l.Close()
		return preferred
	}
	l, err = net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		defer l.Close()
		return uint32(l.Addr().(*net.TCPAddr).Port)
	}
	return preferred
}

func TestE2E(t *testing.T) {
	ctx := context.Background()

	// 1. Start embedded Postgres on an ephemeral port (e.g. 5433).
	port := findAvailablePort(5433)
	cfg := embeddedpostgres.DefaultConfig().
		Port(port).
		Database("source_db").
		Username("postgres").
		Password("postgres").
		StartTimeout(45 * time.Second)

	pg := embeddedpostgres.NewDatabase(cfg)
	if err := pg.Start(); err != nil {
		t.Fatalf("failed to start embedded postgres on port %d: %v", port, err)
	}
	defer func() {
		if err := pg.Stop(); err != nil {
			t.Logf("warning: stop embedded postgres: %v", err)
		}
	}()

	sourceURI := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/source_db?sslmode=disable", port)

	// Connect to source database
	sourceConn, err := pgx.Connect(ctx, sourceURI)
	if err != nil {
		t.Fatalf("connect to source database: %v", err)
	}
	defer sourceConn.Close(ctx)

	// 2. Create a realistic source schema:
	//    - users (id, name, email, created_at)
	//    - orders (id, user_id, total, status)
	//    - order_items (id, order_id, product_name, price)
	schemaSQL := `
	DROP TABLE IF EXISTS order_items CASCADE;
	DROP TABLE IF EXISTS orders CASCADE;
	DROP TABLE IF EXISTS users CASCADE;

	CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL UNIQUE,
		created_at TIMESTAMP NOT NULL DEFAULT NOW()
	);

	CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		total NUMERIC(10, 2) NOT NULL,
		status VARCHAR(50) NOT NULL
	);

	CREATE TABLE order_items (
		id INT PRIMARY KEY,
		order_id INT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
		product_name VARCHAR(255) NOT NULL,
		price NUMERIC(10, 2) NOT NULL
	);
	`
	if _, err := sourceConn.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("create source schema: %v", err)
	}

	// Insert 50 users
	for i := 1; i <= 50; i++ {
		name := fmt.Sprintf("User Original %02d", i)
		email := fmt.Sprintf("user%02d@example.com", i)
		_, err := sourceConn.Exec(ctx,
			"INSERT INTO users (id, name, email, created_at) VALUES ($1, $2, $3, $4)",
			i, name, email, time.Now().UTC(),
		)
		if err != nil {
			t.Fatalf("insert user %d: %v", i, err)
		}
	}

	// Insert 200 orders distributed across the 50 users (4 orders per user)
	for i := 1; i <= 200; i++ {
		userID := ((i - 1) % 50) + 1
		total := float64(i) * 15.50
		status := "completed"
		if i%3 == 0 {
			status = "pending"
		} else if i%3 == 1 {
			status = "shipped"
		}
		_, err := sourceConn.Exec(ctx,
			"INSERT INTO orders (id, user_id, total, status) VALUES ($1, $2, $3, $4)",
			i, userID, total, status,
		)
		if err != nil {
			t.Fatalf("insert order %d: %v", i, err)
		}
	}

	// Insert 400 order_items (2 items per order)
	itemID := 1
	for orderID := 1; orderID <= 200; orderID++ {
		_, err := sourceConn.Exec(ctx,
			"INSERT INTO order_items (id, order_id, product_name, price) VALUES ($1, $2, $3, $4)",
			itemID, orderID, fmt.Sprintf("Product %d-A", orderID), 24.99,
		)
		if err != nil {
			t.Fatalf("insert order_item %d: %v", itemID, err)
		}
		itemID++

		_, err = sourceConn.Exec(ctx,
			"INSERT INTO order_items (id, order_id, product_name, price) VALUES ($1, $2, $3, $4)",
			itemID, orderID, fmt.Sprintf("Product %d-B", orderID), 49.99,
		)
		if err != nil {
			t.Fatalf("insert order_item %d: %v", itemID, err)
		}
		itemID++
	}

	// Create a separate target database in the same embedded Postgres instance for direct verification
	if _, err := sourceConn.Exec(ctx, "CREATE DATABASE target_db"); err != nil {
		t.Fatalf("create target_db: %v", err)
	}

	targetURI := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/target_db?sslmode=disable", port)
	targetConn, err := pgx.Connect(ctx, targetURI)
	if err != nil {
		t.Fatalf("connect to target database: %v", err)
	}
	defer targetConn.Close(ctx)

	// Create identical schema in target_db with foreign keys strictly enforced
	if _, err := targetConn.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("create target schema: %v", err)
	}

	// 3. Run dbdrain against source: --from "users LIMIT 5" --anonymize-pii
	testSalt := "test-e2e-salt-2026"
	outputFile := filepath.Join(t.TempDir(), "extracted_slice.sql")

	// Reset CLI flag variables to avoid interference from any previous runs
	source = sourceURI
	fromExprs = []string{"users LIMIT 5"}
	output = outputFile
	schemaName = "public"
	anonymize = true
	salt = testSalt
	maxRowsPerTable = 0
	maxDepth = 0
	target = ""
	configPath = ""
	doVerify = false
	rateLimit = 0
	concurrency = 4
	safeMode = false
	maxLag = 30 * time.Second
	childrenPerParent = 0
	samplingSeed = "dbdrain-sampling"

	rootCmd.SetArgs([]string{
		"--source", sourceURI,
		"--from", "users LIMIT 5",
		"--anonymize-pii",
		"--salt", testSalt,
		"--output", outputFile,
	})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("dbdrain CLI extraction failed: %v", err)
	}

	// 4. Verify extracted output
	contentBytes, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("failed to read output SQL file: %v", err)
	}
	sqlDump := string(contentBytes)

	// Collect lines per table
	var userLines, orderLines, itemLines []string
	for _, line := range strings.Split(sqlDump, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, `INSERT INTO "public"."users"`) {
			userLines = append(userLines, line)
		} else if strings.HasPrefix(line, `INSERT INTO "public"."orders"`) {
			orderLines = append(orderLines, line)
		} else if strings.HasPrefix(line, `INSERT INTO "public"."order_items"`) {
			itemLines = append(itemLines, line)
		}
	}

	// (a) Verify Exactly 5 users and their child orders/items are extracted.
	if len(userLines) != 5 {
		t.Fatalf("expected exactly 5 users extracted, got %d. SQL Dump:\n%s", len(userLines), sqlDump)
	}

	valuesRe := regexp.MustCompile(`VALUES\s*\((.+)\)\s*ON CONFLICT`)
	extractedUserIDs := make(map[int]bool)
	extractedEmails := make(map[string]bool)
	extractedNames := make(map[string]bool)

	for _, line := range userLines {
		m := valuesRe.FindStringSubmatch(line)
		if len(m) < 2 {
			t.Fatalf("failed to parse values from user line: %s", line)
		}
		// VALUES: id, 'name', 'email', 'created_at'
		parts := strings.Split(m[1], ", ")
		if len(parts) < 3 {
			t.Fatalf("unexpected user line format: %s", line)
		}
		uid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			t.Fatalf("parse uid from %s: %v", parts[0], err)
		}
		name := strings.Trim(parts[1], "'")
		email := strings.Trim(parts[2], "'")

		extractedUserIDs[uid] = true
		extractedNames[name] = true
		extractedEmails[email] = true
	}

	if len(extractedUserIDs) != 5 {
		t.Fatalf("expected 5 unique user IDs, got %d: %v", len(extractedUserIDs), extractedUserIDs)
	}

	// Parse extracted orders
	if len(orderLines) == 0 {
		t.Fatalf("expected child orders to be extracted, got 0")
	}

	extractedOrderIDs := make(map[int]bool)
	for _, line := range orderLines {
		m := valuesRe.FindStringSubmatch(line)
		if len(m) < 2 {
			t.Fatalf("failed to parse values from order line: %s", line)
		}
		// VALUES: id, user_id, total, 'status'
		parts := strings.Split(m[1], ", ")
		if len(parts) < 2 {
			t.Fatalf("unexpected order line format: %s", line)
		}
		oid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			t.Fatalf("parse oid from %s: %v", parts[0], err)
		}
		uid, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			t.Fatalf("parse uid from %s: %v", parts[1], err)
		}
		extractedOrderIDs[oid] = true

		// Verify referential integrity: order must belong to one of the 5 extracted users
		if !extractedUserIDs[uid] {
			t.Errorf("orphaned order %d detected: user_id %d is not in extracted users %v", oid, uid, extractedUserIDs)
		}
	}

	// Each user has 4 orders, so 5 users * 4 orders = 20 orders
	expectedOrders := 5 * 4
	if len(orderLines) != expectedOrders {
		t.Errorf("expected %d child orders extracted, got %d", expectedOrders, len(orderLines))
	}

	// Parse extracted order_items
	if len(itemLines) == 0 {
		t.Fatalf("expected child order_items to be extracted, got 0")
	}

	for _, line := range itemLines {
		m := valuesRe.FindStringSubmatch(line)
		if len(m) < 2 {
			t.Fatalf("failed to parse values from item line: %s", line)
		}
		// VALUES: id, order_id, 'product_name', price
		parts := strings.Split(m[1], ", ")
		if len(parts) < 2 {
			t.Fatalf("unexpected item line format: %s", line)
		}
		iid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			t.Fatalf("parse iid from %s: %v", parts[0], err)
		}
		oid, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			t.Fatalf("parse oid from %s: %v", parts[1], err)
		}

		// Verify referential integrity: item must belong to one of the extracted orders
		if !extractedOrderIDs[oid] {
			t.Errorf("orphaned order_item %d detected: order_id %d is not in extracted orders", iid, oid)
		}
	}

	// Each order has 2 items, so 20 orders * 2 items = 40 items
	expectedItems := 20 * 2
	if len(itemLines) != expectedItems {
		t.Errorf("expected %d child order_items extracted, got %d", expectedItems, len(itemLines))
	}

	// (b) Verify Sensitive fields (email, name) are deterministically masked
	if strings.Contains(sqlDump, "@example.com") {
		t.Errorf("found raw PII: output SQL dump contains unmasked '@example.com'")
	}
	if strings.Contains(sqlDump, "User Original") {
		t.Errorf("found raw PII: output SQL dump contains unmasked 'User Original'")
	}

	// Check masked format
	emailMaskPattern := regexp.MustCompile(`^anon_[0-9a-f]{10}@drain\.local$`)
	for email := range extractedEmails {
		if !emailMaskPattern.MatchString(email) {
			t.Errorf("email %q does not match expected masked pattern anon_<hash>@drain.local", email)
		}
	}

	nameMaskPattern := regexp.MustCompile(`^User_[0-9a-f]{6}$`)
	for name := range extractedNames {
		if !nameMaskPattern.MatchString(name) {
			t.Errorf("name %q does not match expected masked pattern User_<hash>", name)
		}
	}

	// Verify determinism: hashing with the same salt produces the exact same masked string
	masker := anon.New(testSalt)
	for uid := range extractedUserIDs {
		expectedEmail := masker.MaskColumn("email", fmt.Sprintf("user%02d@example.com", uid))
		if !extractedEmails[expectedEmail] {
			t.Errorf("deterministic email mismatch for user %d: expected %s to be in extracted emails", uid, expectedEmail)
		}
		expectedName := masker.MaskColumn("name", fmt.Sprintf("User Original %02d", uid))
		if !extractedNames[expectedName] {
			t.Errorf("deterministic name mismatch for user %d: expected %s to be in extracted names", uid, expectedName)
		}
	}

	// (c) Verify Referential Integrity by executing the extracted SQL directly into target_db
	// Target has foreign key constraints enabled. If there are any missing parents, Postgres will error.
	if _, err := targetConn.Exec(ctx, sqlDump); err != nil {
		t.Fatalf("replaying extracted SQL into target database failed with constraint error: %v", err)
	}

	// Introspect target database schema to run verification query engine
	schema, err := introspect.LoadPostgres(ctx, targetConn, "public")
	if err != nil {
		t.Fatalf("introspect target schema: %v", err)
	}

	drainedCounts := map[string]int{
		"users":       len(userLines),
		"orders":      len(orderLines),
		"order_items": len(itemLines),
	}

	violations, err := verify.Run(ctx, targetConn, "public", schema, drainedCounts)
	if err != nil {
		t.Fatalf("integrity verification check failed: %v", err)
	}
	if len(violations) > 0 {
		t.Errorf("expected 0 orphan violations in target DB, got %d: %+v", len(violations), violations)
	}

	// Verify target row counts match extracted counts
	var userCount, orderCount, itemCount int
	_ = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&userCount)
	_ = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM orders").Scan(&orderCount)
	_ = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM order_items").Scan(&itemCount)

	if userCount != 5 {
		t.Errorf("target database user count = %d, want 5", userCount)
	}
	if orderCount != expectedOrders {
		t.Errorf("target database order count = %d, want %d", orderCount, expectedOrders)
	}
	if itemCount != expectedItems {
		t.Errorf("target database item count = %d, want %d", itemCount, expectedItems)
	}

	t.Logf("✔ E2E Extraction & Integrity Verified: 5 users, %d orders, %d items. 0 orphaned FKs. Deterministic PII masking verified.", orderCount, itemCount)

	// (d) Verify Direct SQLite target hydration with auto-transpiled schema & --verify
	sqliteFile := filepath.Join(t.TempDir(), "dev.db")

	// Reset CLI flag variables
	source = sourceURI
	fromExprs = []string{"users LIMIT 5"}
	output = "-"
	schemaName = "public"
	anonymize = true
	salt = testSalt
	maxRowsPerTable = 0
	maxDepth = 0
	target = sqliteFile
	configPath = ""
	doVerify = true
	rateLimit = 0
	concurrency = 4
	safeMode = false
	maxLag = 30 * time.Second
	childrenPerParent = 0
	samplingSeed = "dbdrain-sampling"

	rootCmd.SetArgs([]string{
		"--source", sourceURI,
		"--from", "users LIMIT 5",
		"--anonymize-pii",
		"--salt", testSalt,
		"--target", sqliteFile,
		"--verify",
	})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("dbdrain direct SQLite hydration failed: %v", err)
	}

	// Verify SQLite database directly
	sdb, err := sql.Open("sqlite", sqliteFile)
	if err != nil {
		t.Fatalf("open hydrated sqlite database: %v", err)
	}
	defer sdb.Close()

	var sUserCount, sOrderCount, sItemCount int
	_ = sdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM "users"`).Scan(&sUserCount)
	_ = sdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM "orders"`).Scan(&sOrderCount)
	_ = sdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM "order_items"`).Scan(&sItemCount)

	if sUserCount != 5 {
		t.Errorf("SQLite target user count = %d, want 5", sUserCount)
	}
	if sOrderCount != expectedOrders {
		t.Errorf("SQLite target order count = %d, want %d", sOrderCount, expectedOrders)
	}
	if sItemCount != expectedItems {
		t.Errorf("SQLite target item count = %d, want %d", sItemCount, expectedItems)
	}

	// Verify PRAGMA foreign_key_check on hydrated SQLite database
	violationsSQLite, err := verify.RunSQLite(ctx, sdb)
	if err != nil {
		t.Fatalf("verify.RunSQLite failed: %v", err)
	}
	if len(violationsSQLite) > 0 {
		t.Errorf("expected 0 FK violations in SQLite target, got %d: %+v", len(violationsSQLite), violationsSQLite)
	}

	t.Logf("✔ Direct SQLite Hydration & Foreign Key Check Verified: %d users, %d orders, %d items. 0 violations.", sUserCount, sOrderCount, sItemCount)

	// 4. Test safe-mode with rate limiting, concurrency, and health poller against embedded Postgres
	safeOutputFile := filepath.Join(t.TempDir(), "safe_slice.sql")
	source = sourceURI
	fromExprs = []string{"users LIMIT 3"}
	output = safeOutputFile
	schemaName = "public"
	anonymize = false
	salt = testSalt
	maxRowsPerTable = 0
	maxDepth = 0
	target = ""
	configPath = ""
	doVerify = false
	rateLimit = 500
	concurrency = 2
	safeMode = true
	maxLag = 10 * time.Second
	childrenPerParent = 0
	samplingSeed = "dbdrain-sampling"

	rootCmd.SetArgs([]string{
		"--source", sourceURI,
		"--from", "users LIMIT 3",
		"--output", safeOutputFile,
		"--safe-mode",
		"--rate-limit", "500",
		"--concurrency", "2",
		"--max-lag", "10s",
	})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("dbdrain CLI with --safe-mode failed: %v", err)
	}

	safeContent, err := os.ReadFile(safeOutputFile)
	if err != nil {
		t.Fatalf("read safe output file: %v", err)
	}
	if len(safeContent) == 0 {
		t.Fatalf("expected non-empty output with --safe-mode")
	}
	t.Logf("✔ Safe-mode extraction completed successfully with rate-limiting and background health poller")

	// 5. Test multi-anchor extraction with stratified child sampling and direct SQLite hydration
	multiAnchorSQLiteFile := filepath.Join(t.TempDir(), "multi_anchor.db")
	source = sourceURI
	fromExprs = []string{"users WHERE id = 1", "users WHERE id = 2"}
	output = "-"
	schemaName = "public"
	anonymize = false
	salt = testSalt
	maxRowsPerTable = 0
	maxDepth = 0
	target = multiAnchorSQLiteFile
	configPath = ""
	doVerify = true
	rateLimit = 0
	concurrency = 4
	safeMode = false
	maxLag = 30 * time.Second
	childrenPerParent = 1
	samplingSeed = "test-sampling-seed"

	rootCmd.SetArgs([]string{
		"--source", sourceURI,
		"--from", "users WHERE id = 1",
		"--from", "users WHERE id = 2",
		"--target", multiAnchorSQLiteFile,
		"--children-per-parent", "1",
		"--seed", "test-sampling-seed",
		"--verify",
	})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("dbdrain multi-anchor with stratified sampling failed: %v", err)
	}

	mDB, err := sql.Open("sqlite", multiAnchorSQLiteFile)
	if err != nil {
		t.Fatalf("open multi-anchor sqlite db: %v", err)
	}
	defer mDB.Close()

	var mUserCount, mOrderCount, mItemCount int
	_ = mDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM "users"`).Scan(&mUserCount)
	_ = mDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM "orders"`).Scan(&mOrderCount)
	_ = mDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM "order_items"`).Scan(&mItemCount)

	if mUserCount != 2 {
		t.Errorf("multi-anchor user count = %d, want 2", mUserCount)
	}
	// With childrenPerParent = 1, exactly 1 order per user -> 2 orders total!
	if mOrderCount != 2 {
		t.Errorf("stratified child orders count = %d, want 2", mOrderCount)
	}
	// With childrenPerParent = 1, exactly 1 item per order -> 2 items total!
	if mItemCount != 2 {
		t.Errorf("stratified child items count = %d, want 2", mItemCount)
	}

	violationsMulti, err := verify.RunSQLite(ctx, mDB)
	if err != nil {
		t.Fatalf("verify multi-anchor sqlite failed: %v", err)
	}
	if len(violationsMulti) > 0 {
		t.Errorf("expected 0 FK violations in multi-anchor target, got %d: %+v", len(violationsMulti), violationsMulti)
	}

	t.Logf("✔ Multi-Anchor DAG Closure & Stratified Child Sampling Verified: %d users, %d orders, %d items. 0 violations.", mUserCount, mOrderCount, mItemCount)
}
