<div align="center">

# 🚰 dbdrain

**Zero-downtime PostgreSQL, MySQL/MariaDB & SQLite database subsetting, polymorphic FK resolution & deterministic PII masking — in a single static binary.**

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://go.dev)
[![npm](https://img.shields.io/npm/v/dbdrain?logo=npm&color=CB3837)](https://www.npmjs.com/package/dbdrain)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/x7ssss/dbdrain)](https://github.com/x7ssss/dbdrain/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/x7ssss/dbdrain)](https://goreportcard.com/report/github.com/x7ssss/dbdrain)

</div>

---

## Why dbdrain?

| Tool | Problem |
|------|---------|
| **Snaplet** | Sunsetted in 2024 |
| **Jailer** | Requires Java + manual YAML relationship maps |
| **Neosync** | Heavy Docker setup, fails on circular foreign keys |
| **pg_dump / mysqldump** | Full database only — no subsetting |

**dbdrain** fills this void: a single Go binary, no runtime dependencies, no Docker, no YAML for basic usage.

- 🪶 **Native SQLite Target Hydration** — Transpile production PostgreSQL and MySQL schemas on-the-fly into SQLite DDL and hydrate local development databases (e.g. `--target ./dev.db`) with `PRAGMA defer_foreign_keys = ON`
- 🎯 **Multi-Anchor DAG Closures** — Repeatable `--from` flag extracts unified slices across heterogeneous roots with global entity deduplication preventing Cartesian explosion
- 🌲 **Reverse Subsetting & Upstream Ancestry Pruning** — `--upstream` treats anchor as an isolated leaf incident, computing strict upward-only recursive DAG closure while pruning all sibling and downstream branches
- 🌐 **Citus Distributed Table Awareness** — Discovers coordinator distribution metadata (`citus_tables`), categorizes tables (`Distributed`, `Reference`, `Local`), enforces colocation keys, prunes worker shards, and emits Citus DDL
- 🗄️ **Declarative PostgreSQL Partitioning** — Transparently introspects and reconstructs partition trees (`PARTITION BY RANGE/LIST/HASH`), routing streams through the logical root relation
- 🛡️ **Row-Level Security (RLS) Awareness** — `--bypass-rls` executes `SET row_security = off;` on Postgres connections; `--verify` distinctly categorizes `Valid`, `Policy-Excluded`, and `Corrupted/Orphan`
- ✂️ **Stratified Child Sampling** — `--children-per-parent N` uniformly samples child entities across parent nodes via SQL window ranking functions while preserving referential integrity
- ⏩ **Keyset Seek Pagination** — O(1) cursor seek pagination (`WHERE (table, pk) > ($1, $2)`) eliminates `OFFSET` scan degradation during batch traversal
- ⚡ **Multi-Engine Support** — Native support for PostgreSQL 12+, MySQL 8.0+ / MariaDB 10.5+, and SQLite 3
- 🔒 **Non-Locking Consistent Snapshots** — Isolation via `REPEATABLE READ READ ONLY` (Postgres) and `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT` (MySQL)
- 🔄 **Two-Phase & Deferred Circular FK Resolution** — Resolves cyclic dependencies in MySQL via two-phase inserts/updates, and in SQLite via `PRAGMA defer_foreign_keys = ON`
- ⚡ **Zero-Copy Streaming Engine** — Direct `pgx.CopyFromSource` over live cursors with `sync.Pool` buffer recycling for flat O(1) memory usage during millions of rows extractions
- 🧠 **Smart Query Batching** — `= ANY($1::type[])` parameterized binding avoids GEQO degradation up to 10k keys; automatically promotes to `UNLOGGED` scratch tables + binary `COPY` + indexed `JOIN` for >10k keys
- 📦 **Zero-Config npm Distribution** — Run instantly with `npx dbdrain` via platform-native `optionalDependencies` binaries
- 🔗 **Referentially Intact Subsets** — Recursively follows every FK chain upward and downward from seed rows
- 🌿 **Self-Referencing Hierarchies** — `users.manager_id → users.id`, `categories.parent_id → categories.id` resolved via recursive CTEs
- 🧩 **Polymorphic Associations** — Virtual FKs for Rails-style `commentable_id` / `commentable_type` patterns
- 🎭 **Deterministic PII Masking** — HMAC-SHA256 ensures the same input always produces the same output (no UNIQUE violations)
- 📋 **Declarative Config** — `dbdrain.yaml` for custom masking rules and virtual FK declarations
- 🚀 **Direct Target Streaming** — Bypass file I/O and hydrate a staging or local DB in one command
- ✅ **Integrity Verification** — Post-hydration orphan detection across PostgreSQL, MySQL, and SQLite (`PRAGMA foreign_key_check`)
- 🛡️ **Explosion Safeguards** — `--depth` and `--max-rows-per-table` prevent pulling entire production datasets

---

## Engine Compatibility Table

| Capability | PostgreSQL (12+) | MySQL (8.0+) & MariaDB (10.5+) | SQLite (3.x) |
|------------|------------------|--------------------------------|--------------|
| **Connection Scheme** | `postgres://`, `postgresql://` | `mysql://`, `mariadb://`, standard DSN | `sqlite://`, or `.db` / `.sqlite` file path |
| **Snapshot Isolation** | `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;` | `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;`<br>`START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT;` | Serialized transaction (`BEGIN TRANSACTION;`) with WAL mode |
| **Circular FK Resolution** | `SET CONSTRAINTS ALL DEFERRED;` | **Two-Phase Insert Resolution** (Phase 1 NULL FK insert + Phase 2 UPDATE) | `PRAGMA foreign_keys = ON;`<br>`PRAGMA defer_foreign_keys = ON;` |
| **Identifier Quoting** | Double quotes (`"users"."id"`) | Backticks (`` `users`.`id` ``) | Double quotes (`"users"."id"`) |
| **Parent/Child Batching** | `= ANY($1::type[])` (≤10k) / `UNLOGGED` temp table (>10k) | Chunked `IN (...)` queries in batches of 2,000 | Direct high-throughput batch hydration |
| **Target Hydration** | Zero-copy binary `COPY FROM` stream | Transactional bulk inserts & two-phase updates | High-throughput bulk loading PRAGMAs + deferred FK transaction |
| **Integrity Verification** | Custom LEFT JOIN orphan queries (`--verify`) | Custom LEFT JOIN orphan queries (`--verify`) | Automated `PRAGMA foreign_key_check;` (`--verify`) |

---

## Citus Distributed Table Introspection & Colocation (v1.0.0)

Citus extends PostgreSQL into a distributed database using coordinator-worker architecture, shard distribution, and table colocation. `dbdrain` natively understands Citus cluster topologies, transparently subsetting multi-tenant databases without manual configuration.

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Citus Coordinator Logical Discovery                  │
│                                                                        │
│   Logical Table: "users" (Distributed by "tenant_id", Colocation 1)    │
│   Logical Table: "orders" (Distributed by "tenant_id", Colocation 1)   │
│   Logical Table: "countries" (Reference Table, Replicated Everywhere)  │
│                                                                        │
│   Physical Shards: users_102008, orders_102009 (PRUNED & FILTERED)     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                         Joint Tenant Boundary
                         ("tenant_id" IN ('42'))
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 Referentially Intact Tenant Extraction                 │
│                                                                        │
│   users WHERE tenant_id = 42 ───► orders WHERE tenant_id = 42         │
│   (Zero leakage to tenant 43 even if primary keys collide across IDs)  │
└────────────────────────────────────────────────────────────────────────┘
```

### Key Citus Capabilities:

1. **Coordinator Metadata Discovery**:
   - Detects whether the source runs Citus by querying `pg_extension WHERE extname = 'citus'` or `citus_tables`.
   - Introspects distribution metadata from `citus_tables` and `pg_dist_partition`:
     * **Distribution Column**: Discovers the partitioning key (e.g. `tenant_id`, `company_id`).
     * **Distribution Method**: Hash (`h`), Reference (`n`), Range (`r`), or Append (`a`).
     * **Colocation Group**: Discovers `colocation_id` to map colocated shard families.
   - Categorizes every table as `Distributed`, `Reference`, or `Local`.

2. **Physical Shard Pruning**:
   - `dbdrain` automatically queries `pg_dist_shard` to discover all physical worker shard relations (e.g. `users_102008`, `orders_102009`).
   - Physical shards are systematically pruned from column cataloging, primary keys, foreign keys, and graph node creation.
   - Extractions **exclusively** execute through the coordinator's logical relations, allowing Citus to orchestrate distributed query planning.

3. **Colocation & Foreign Key Topology**:
   - Validates that foreign keys between distributed tables include the distribution column and share the same colocation group.
   - Emits descriptive warnings if an invalid foreign key spans disparate colocation groups.

4. **Joint Tenant Boundary Preservation**:
   - In multi-tenant systems, colocated tables share the distribution key. When an extraction begins with an anchor query on a tenant (e.g. `--from "users WHERE tenant_id = '42'"`), `dbdrain` captures the active tenant key set for that colocation group.
   - As traversal moves to downstream children or upstream parents (e.g. `orders`, `invoices`, `memberships`), `dbdrain` injects the joint tenant boundary condition (`"tenant_id" IN ('42')`).
   - This ensures absolute tenant isolation and prevents cross-tenant row contamination even when child primary keys collide across tenants.

5. **Target Citus DDL Emission**:
   - When generating PostgreSQL target DDL or streaming to a Citus target, `dbdrain` emits the appropriate distribution calls:
     ```sql
     -- Emitted after CREATE TABLE before data streaming:
     SELECT create_distributed_table('users', 'tenant_id');
     SELECT create_reference_table('countries');
     ```

---

## SQLite Target Transpilation & Hydration

When `--target` points to an SQLite file (e.g. `--target ./dev.db` or `--target sqlite://local.db`):
1. **Direct File & Schema Creation**: If the SQLite file does not exist, `dbdrain` automatically transpiles the source catalog into SQLite-compatible DDL, creates the tables, and enables performance optimizations.
2. **High-Throughput Bulk Loading PRAGMAs**:
   ```sql
   PRAGMA journal_mode = WAL;
   PRAGMA synchronous = OFF;
   PRAGMA temp_store = MEMORY;
   PRAGMA cache_size = -64000;
   ```
3. **Deferred Constraint Hydration**:
   ```sql
   PRAGMA foreign_keys = ON;
   PRAGMA defer_foreign_keys = ON;
   BEGIN TRANSACTION;
   ... [Batched Inserts] ...
   COMMIT;
   ```
4. **Integrity Audit**: Automatically runs `PRAGMA foreign_key_check;` upon completion to verify zero broken foreign keys.

### Type Transpilation Matrix

| Source Column Type (PostgreSQL / MySQL) | SQLite Affine Type | Transpilation Behavior |
|------------------------------------------|--------------------|------------------------|
| `SERIAL`, `AUTO_INCREMENT`, Integer PK | `INTEGER PRIMARY KEY AUTOINCREMENT` | Auto-incrementing primary key |
| Non-Integer PK (e.g. `UUID`, `VARCHAR`) | `TEXT PRIMARY KEY` | Primary key without AUTOINCREMENT |
| `INT`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT` | `INTEGER` | 64-bit signed integer |
| `VARCHAR`, `TEXT`, `CHAR`, `CLOB` | `TEXT` | UTF-8 string |
| `UUID` | `TEXT` | 36-character canonical UUID string |
| `JSON`, `JSONB` | `TEXT` | JSON document string |
| `TIMESTAMPTZ`, `DATETIME`, `TIMESTAMP`, `DATE` | `TEXT` | ISO 8601 UTC string representation |
| `BOOLEAN`, `BOOL` | `INTEGER` | `1` (true) or `0` (false) |
| `NUMERIC`, `DECIMAL` | `NUMERIC` | Exact decimal value |
| `FLOAT`, `DOUBLE`, `REAL`, `DOUBLE PRECISION` | `REAL` | 64-bit IEEE floating point |
| `BYTEA`, `BLOB`, `BINARY`, `VARBINARY` | `BLOB` | Binary hex literal (`X'...'`) |
| `ENUM`, `ARRAY` (`text[]`), `INET`, `CIDR` | `TEXT` | Serialized text representation |

---

## Two-Phase Circular FK Resolution (MySQL / MariaDB)

PostgreSQL natively allows deferred foreign keys via `SET CONSTRAINTS ALL DEFERRED`. However, MySQL and MariaDB **do not support deferred constraints**. A common workaround is setting `FOREIGN_KEY_CHECKS = 0`, but this has serious downsides:
1. It requires elevated administrative privileges (`SUPER` or `SYSTEM_VARIABLES_ADMIN`).
2. It completely disables constraint validation, potentially hiding data corruption.
3. In managed environments (AWS RDS, Aurora, Cloud SQL), changing global or session foreign key checks may be restricted or cause replication anomalies.

### How dbdrain Resolves MySQL Cycles Without Disabling FK Checks:

Using **Tarjan's Strongly Connected Components (SCC)** algorithm, `dbdrain` detects circular dependency loops (e.g. `users.team_id → teams.id` and `teams.lead_id → users.id`, or self-referencing `users.manager_id → users.id`).

```
┌─────────────────────────────────────────────────────────────┐
│                       Tarjan SCC Cycle                      │
│                                                             │
│       users (team_id [nullable]) ───► teams                 │
│         ▲                               │                   │
│         └─────────── lead_id ───────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

1. **Phase 1 (Safe Insert)**:
   For every row in a cycle, any nullable foreign key pointing to a mutually dependent table within the SCC is temporarily coerced to `NULL`:
   ```sql
   INSERT INTO `users` (`id`, `name`, `team_id`) VALUES (10, 'Alice', NULL);
   INSERT INTO `teams` (`id`, `title`, `lead_id`) VALUES (1, 'Core Team', 10);
   ```
2. **Phase 2 (Foreign Key Restoration)**:
   Once all dependent rows in the SCC are safely inserted, `dbdrain` applies targeted `UPDATE` statements to reinstate the foreign key references:
   ```sql
   UPDATE `users` SET `team_id` = 1 WHERE `id` = 10;
   ```

---

## Advanced Subsetting Algorithms (v0.8.0)

Version 0.8.0 introduces high-precision graph subsetting algorithms designed for complex schemas with skewed distributions and heterogeneous extraction requirements:

### 1. Multi-Anchor DAG Closure
Instead of restricting extractions to a single root table, `dbdrain` allows multiple repeatable `--from` flags to extract cohesive slices seeded across different entity domains in a single pass:

```bash
dbdrain --source "postgres://user:pass@prod-host/mydb" \
        --from "users WHERE id IN (101, 102) LIMIT 20" \
        --from "tenants WHERE tier = 'enterprise'" \
        --target "postgres://user:pass@staging-host/mydb"
```

- **Unified BFS Queue**: All seed rows across all defined anchors are resolved and enqueued into a unified BFS traversal queue.
- **Global `(table_name, primary_key)` Deduplication**: Visited row tracking is shared globally across roots. Entities reachable from multiple roots are visited and emitted exactly once, preventing duplicate key violations and eliminating Cartesian explosion.

### 2. Stratified Child Sampling (`--children-per-parent`)
Blunt table-level limits (`--max-rows-per-table`) suffer from **fan-out skew**: a handful of high-activity parent entities (e.g. power users with 50,000 orders) exhaust the entire row quota, leaving all other parents with 0 child rows.

`--children-per-parent N` enforces a uniform child quota per parent entity down the DAG using database window ranking functions:

```bash
# Pull enterprise tenants, but limit each tenant to at most 5 users, and each user to at most 3 orders
dbdrain --source "postgres://user:pass@prod-host/mydb" \
        --from "tenants WHERE tier = 'enterprise'" \
        --children-per-parent 5 \
        --seed "my-deterministic-seed" \
        --target "./dev.db"
```

#### Deterministic Window Queries:
- **PostgreSQL 12+**:
  ```sql
  WITH ranked AS (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY c."tenant_id" ORDER BY MD5(CAST(c."id" AS text) || $2)) AS rn,
           COUNT(*) OVER (PARTITION BY c."tenant_id") AS total_children
    FROM "public"."users" c
    WHERE c."tenant_id" = ANY($1::bigint[])
  )
  SELECT id, tenant_id, name, email FROM ranked
  WHERE rn = 1 OR (rn <= 5 AND total_children > 1);
  ```
- **MySQL 8.0+ / MariaDB 10.5+**:
  ```sql
  WITH ranked AS (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY c.`tenant_id` ORDER BY MD5(CONCAT(CAST(c.`id` AS CHAR), ?))) AS rn,
           COUNT(*) OVER (PARTITION BY c.`tenant_id`) AS total_children
    FROM `shop`.`users` c
    WHERE c.`tenant_id` IN (?, ?, ?)
  )
  SELECT id, tenant_id, name, email FROM ranked
  WHERE rn = 1 OR (rn <= 5 AND total_children > 1);
  ```

> [!TIP]
> **Referential Invariant Guarantee**: The window condition `rn = 1 OR (rn <= N AND total_children > 1)` guarantees that every parent with ≥1 child retains at least 1 child, while capping each parent at N children. The pseudo-random hash order `MD5(PK || seed)` ensures reproducible and deterministic subset selection across extraction runs.

### 3. Keyset Seek Pagination for Queues
Traditional `OFFSET` pagination suffers from $O(N)$ scan degradation as queue depths grow into millions of rows. `dbdrain` v0.8.0 implements composite keyset seek queries:

```sql
-- PostgreSQL Keyset Seek
SELECT id, user_id, total FROM "public"."orders"
WHERE ("user_id", "id") > ($1, $2)
ORDER BY "user_id", "id" LIMIT 1000;
```

```sql
-- MySQL Keyset Seek
SELECT id, user_id, total FROM `shop`.`orders`
WHERE (`user_id`, `id`) > (?, ?)
ORDER BY `user_id`, `id` LIMIT 1000;
```

This guarantees **$O(1)$ cursor seek time** regardless of whether page 1 or page 50,000 is being traversed.

### 4. Target-Directed Reverse Slicing (`--upstream`)
When diagnosing production incidents or extracting isolated test cases (e.g. `--from "charges WHERE id = 98234" --upstream`), standard forward subsetting pulls every sibling charge, invoice item, customer review, and audit event, exponentially bloating the slice.

`--upstream` flips the traversal engine into strict reverse mode:
- **Leaf-Anchor Isolation**: Treats the `--from` seed as an isolated leaf/incident anchor.
- **Upward-Only Closure**: Performs a strict recursive upward DAG closure along outgoing foreign keys, pruning all sibling down-traversals (never pulling siblings of parents or unrelated child tables).
- **Cycle Avoidance via Path Identity**: Maintains an identity path array (`visited_path`) to prevent circular parent loops.
- **Multi-Path Convergence**: When multiple FK paths converge on the same ancestor (e.g. `charges → invoices → customers` AND `charges → payments → customers`), computes the exact union without Cartesian duplication.

### 5. Declarative PostgreSQL Partitioning (PostgreSQL 10-16+)
- **Catalog Introspection**: Introspects `pg_partitioned_table`, `pg_inherits`, and `pg_class.relkind = 'p'` to map partitioned roots and child partition bounds.
- **Logical DAG Representation**: Partitioned tables are modeled as a single logical root node in the dependency graph, preserving child partition hierarchies and bounds expressions.
- **Transparent Root Routing**: Streams queries directly through the logical root relation so PostgreSQL handles transparent partition pruning and routing.
- **Target DDL Emission**: Automatically emits partition strategy DDL (`PARTITION BY RANGE/LIST/HASH`), attaches child partition tables with their bounds expressions, and streams rows without routing errors.

### 6. Row-Level Security (RLS) & Tenant Visibility
- **`--bypass-rls` Flag**: When enabled, executes `SET row_security = off;` on the PostgreSQL extraction connection (fails fast if the user lacks `BYPASSRLS` or superuser privileges).
- **Three-Tier Verification Auditing**:
  During `--verify`, `dbdrain` inspects `relrowsecurity` on parent tables and categorizes integrity results distinctly:
  * `Valid`: Parent exists and is visible.
  * `Policy-Excluded`: Parent exists globally in the database, but is filtered by the tenant's current RLS policy.
  * `Corrupted/Orphan`: Parent row is physically missing.

---

## Production Safety Controls & Adaptive Pacing (v0.7.0)

Extracting large data slices from live production primary or replica databases requires strict safety guarantees to prevent locking queries, catalog contention, or overwhelming database purge mechanisms. `dbdrain` v0.7.0 introduces an enterprise-grade safety architecture:

### 1. Fail-Fast Session Timeouts
For PostgreSQL sources, dedicated extraction connections automatically execute strict transaction-level session guards:
```sql
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '100ms';
SET LOCAL idle_in_transaction_session_timeout = '60s';
SET LOCAL application_name = 'dbdrain-worker';
```
- **Lock Timeout Guarding**: With `lock_timeout = '100ms'`, if concurrent DDL migrations (e.g. `ALTER TABLE`) are running, `dbdrain` fails fast with jittered exponential backoff rather than queuing behind locks and blocking production transactions.
- **Identifiable DBA Telemetry**: The dedicated `application_name = 'dbdrain-worker'` tag allows database administrators to clearly monitor and identify extractor queries in `pg_stat_activity`.

### 2. Autonomous Cluster Health Poller (`--safe-mode`)
When `--safe-mode` is enabled, an independent background monitor continuously inspects cluster load every 5 seconds via short, dedicated autocommit connections:
- **MySQL InnoDB Health**:
  - Monitors `trx_rseg_history_len` (InnoDB History List Length) via `information_schema.INNODB_METRICS` (or `SHOW ENGINE INNODB STATUS`).
  - Monitors replication lag (`Seconds_Behind_Source`) via `SHOW REPLICA STATUS`.
  - **Thresholds**:
    - History List Length > 100,000: Triggers adaptive backoff pacing.
    - History List Length > 1,000,000 or Replication Lag > `--max-lag`: Pauses new chunk extraction immediately until the cluster recovers.
- **PostgreSQL Health**:
  - Monitors active sessions, IO waiters, lock waiters, and oldest active transaction age (`now() - xact_start`) via `pg_stat_activity`, and replication slot lag via `pg_replication_slots`.
  - **Thresholds**:
    - Oldest transaction age > `--max-lag` or lock waiters spike (≥ 5): Pauses extraction immediately.
    - Elevated IO waiters (≥ 10) or tx age: Triggers adaptive backoff pacing.

> [!IMPORTANT]
> **Zero-Leak Snapshot Invariant**: When paused due to cluster backpressure, the extractor finishes or rolls back the current bounded chunk before pausing. It **never sleeps while holding an open consistent-read transaction**, preventing undo log bloat and allowing vacuum and purge threads to clear dead tuples freely.

### 3. Token-Bucket Rate Limiter & Concurrency Controller
Fine-tune throughput and resource usage during extractions:
- `--rate-limit <rows/sec>`: Implements a token-bucket rate limiter via `golang.org/x/time/rate` to bound row extraction rates and prevent network saturation.
- `--concurrency <workers>`: Limits parallel extraction workers using semaphore channels (default: 4).

### 4. Lipgloss Live Health Telemetry
When running interactively in terminal mode with `--safe-mode`, `dbdrain` displays real-time cluster health and extraction speed:
```text
⚡ Engine: Healthy | History List: 1,420 | Rate: 4,500 rows/s | Workers: 4
```
If backpressure thresholds are breached, the status monitor alerts the user and gracefully awaits cluster recovery:
```text
⚠ Paused: Target cluster under high load (history list > 100k). Waiting for recovery...
```

---

## Installation Matrix & Distribution

| Channel | Platform | Command |
|---------|----------|---------|
| **npm / npx** | Cross-Platform (Node.js 18+) | `npx @dbdrain/cli` or `npm install -g @dbdrain/cli` |
| **Homebrew** | macOS & Linux | `brew install x7ssss/tap/dbdrain` |
| **Scoop** | Windows 10/11 / Windows Server | `scoop bucket add x7ssss https://github.com/x7ssss/scoop-bucket`<br>`scoop install dbdrain` |
| **Direct Binary** | Linux (`amd64`, `arm64`)<br>macOS (`arm64`, `amd64`)<br>Windows (`amd64`, `arm64`) | [GitHub Releases](https://github.com/x7ssss/dbdrain/releases/latest) |
| **Go Install** | Go 1.22+ Toolchain | `go install github.com/x7ssss/dbdrain/cmd/dbdrain@v1.0.0` |

### 1. Run with npx (Zero Installation)

No manual binary installation required. Automatically fetches and runs the matching native binary for your OS and CPU:

```bash
# Run immediately via npx
npx @dbdrain/cli --source "postgres://user:pass@prod-host/mydb" \
                 --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
                 --anonymize-pii \
                 --output slice.sql

# Or install globally
npm install -g @dbdrain/cli
```

### 2. Homebrew (macOS / Linux)

```bash
brew install x7ssss/tap/dbdrain
dbdrain --version
```

### 3. Windows Scoop

```powershell
scoop bucket add x7ssss https://github.com/x7ssss/scoop-bucket
scoop install dbdrain
dbdrain --version
```

### 4. Direct Standalone Binary Downloads

Precompiled static binaries with `CGO_ENABLED=0` and zero external dependencies:

```bash
# Linux (x86_64 / amd64)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-linux-amd64 -o dbdrain && chmod +x dbdrain

# Linux (arm64 / aarch64)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-linux-arm64 -o dbdrain && chmod +x dbdrain

# macOS (Apple Silicon / arm64)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-darwin-arm64 -o dbdrain && chmod +x dbdrain

# macOS (Intel / amd64)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-darwin-amd64 -o dbdrain && chmod +x dbdrain

# Windows (x86_64 / PowerShell)
Invoke-WebRequest https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-windows-amd64.exe -OutFile dbdrain.exe

# Windows (arm64 / PowerShell)
Invoke-WebRequest https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-windows-arm64.exe -OutFile dbdrain.exe
```

---

## Quick Start

```bash
# PostgreSQL to SQL file
dbdrain --source "postgres://user:pass@prod-host/mydb" \
        --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
        --anonymize-pii \
        --output slice.sql

# PostgreSQL direct to local SQLite database (auto-transpiles DDL)
dbdrain --source "postgres://user:pass@prod-host/mydb" \
        --from "users LIMIT 50" \
        --target "./dev.db" \
        --anonymize-pii

# MySQL to SQL file
dbdrain --source "mysql://user:pass@prod-host:3306/mydb" \
        --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
        --anonymize-pii \
        --output slice.sql
```

### Build from source

```bash
git clone https://github.com/x7ssss/dbdrain.git
cd dbdrain
go build -o dbdrain ./cmd/dbdrain/
```

---

## Usage Examples

### 1. Slice PostgreSQL to local SQLite database (development / testing)

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE plan = 'pro' LIMIT 25" \
  --target "./dev.db" \
  --anonymize-pii \
  --verify
```

### 2. Slice PostgreSQL to SQL file (with PII masking)

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
  --anonymize-pii \
  --salt "my-secret-salt" \
  --output slice.sql
```

### 3. Slice MySQL to SQL file with Two-Phase Cycle Resolution

```bash
dbdrain \
  --source "mysql://root:secret@prod-host:3306/shop" \
  --from "orders WHERE created_at > '2026-01-01' LIMIT 100" \
  --anonymize-pii \
  --output slice.sql
```

### 4. Direct streaming hydration (PostgreSQL to PostgreSQL, or MySQL to MySQL)

```bash
# PostgreSQL to staging PostgreSQL
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE plan = 'enterprise' LIMIT 100" \
  --target "postgres://user:pass@localhost/staging" \
  --anonymize-pii \
  --depth 2 \
  --max-rows-per-table 500 \
  --verify

# MySQL to staging MySQL
dbdrain \
  --source "mysql://root:secret@prod-host:3306/mydb" \
  --from "users WHERE plan = 'enterprise' LIMIT 100" \
  --target "mysql://root:secret@localhost:3306/staging" \
  --anonymize-pii \
  --depth 2 \
  --max-rows-per-table 500 \
  --verify
```

---

## Flag Reference

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--source` | string | **required** | Source database connection string (`postgres://`, `mysql://`, `mariadb://`) |
| `--from` | string | **required** | Anchor query (repeatable): `"<table> [WHERE <clause>] [LIMIT <n>]"` |
| `--children-per-parent` | int | `0` (unlimited) | Quota of child rows pulled per parent entity down the DAG |
| `--seed` | string | `dbdrain-sampling` | Deterministic hash seed for reproducible stratified child sampling |
| `--output` | string | `-` (stdout) | Output SQL file path (`-` = stdout) |
| `--schema` | string | `public` (PG) / DSN db (MySQL) | Schema or database name to operate on |
| `--anonymize-pii` | bool | `false` | Enable deterministic PII masking |
| `--salt` | string | `dbdrain-secret-salt` | HMAC salt for reproducible masking |
| `--depth` | int | `0` (unlimited) | Max downward FK traversal depth (parents always unlimited) |
| `--max-rows-per-table` | int | `0` (unlimited) | Hard ceiling on rows pulled per child table |
| `--target` | string | — | Target DB string or file (PostgreSQL, MySQL, or SQLite `.db` file) |
| `--config` | string | `dbdrain.yaml` (auto) | Path to declarative config file |
| `--verify` | bool | `false` | Run post-hydration orphan integrity checks (requires `--target`) |
| `--rate-limit` | int | `0` (unlimited) | Rate limit in rows per second |
| `--concurrency` | int | `4` | Maximum parallel extraction workers |
| `--safe-mode` | bool | `false` | Enables active background cluster health polling and automatic adaptive throttling |
| `--max-lag` | duration | `30s` | Maximum allowed replica lag before pausing extraction |
| `--upstream` | bool | `false` | Treat `--from` as an isolated leaf anchor; perform strict upward-only reverse subsetting |
| `--bypass-rls` | bool | `false` | Bypass Row-Level Security via `SET row_security = off;` on PostgreSQL connection |

---

## `dbdrain.yaml` — Declarative Config

Place a `dbdrain.yaml` in your working directory (or point to it with `--config`):

```yaml
rules:
  # Exact table + column
  - table: users
    column: email
    transform: fake_email       # anon_<hmac>@drain.local

  - table: users
    column: password
    transform: redact           # [REDACTED]

  # Wildcard table, glob column pattern — catches access_token, refresh_token, etc.
  - table: "*"
    column: "*_token"
    transform: uuid_remap       # deterministic UUID v4 from HMAC

  # Exact table, wildcard column
  - table: audit_logs
    column: "*"
    transform: redact

virtual_foreign_keys:
  # Rails-style polymorphic association:
  # comments.commentable_id → posts.id (when commentable_type = 'post')
  # comments.commentable_id → videos.id (when commentable_type = 'video')
  - child_table: comments
    child_column: commentable_id
    parent_table_column: commentable_type   # runtime discriminator column
    mappings:
      post: posts.id
      video: videos.id

associations:
  # Skip FK traversal entirely; coerces dangling nullable FK column to NULL
  - source: charges
    target: audit_trail
    restriction: false

  # Conditional SQL predicate for selective relationship extraction
  - source: charges
    target: analytics_events
    restriction: "event_type = 'billing'"
```

### Available Transforms

| Transform | Output format |
|-----------|--------------|
| `fake_email` | `anon_<hmac[:10]>@drain.local` |
| `redact` | `[REDACTED]` |
| `uuid_remap` | Deterministic RFC 4122 UUID v4 |
| `hmac_phone` | `+1555<hmac[:7]>` |
| `hmac_name` | `User_<hmac[:6]>` |
| `hmac_token` | `tok_<hmac[:16]>` |
| `redact_password` | `$2a$12$e8rG.e9y1dYv9fD1v1.X7fakehash` |

---

## Automatic PII Masking Rules

When `--anonymize-pii` is enabled and no config rule matches, columns are auto-masked by name heuristic:

| Column name pattern | Auto-masked format |
|---------------------|--------------------|
| `email`, `*_email` | `anon_<hash[:10]>@drain.local` |
| `phone`, `mobile`, `*phone*` | `+1555<hash[:7]>` |
| `first_name` | `User_<hash[:6]>` |
| `last_name` | `Anon_<hash[:6]>` |
| `name`, `*_name` | `User_<hash[:6]>` |
| `password`, `passwd` | `$2a$12$e8rG.e9y1dYv9fD1v1.X7fakehash` |
| `token`, `secret`, `*token*` | `tok_<hash[:16]>` |

---

## Post-Hydration Integrity Verification (`--verify`)

When `--verify` is combined with `--target`, dbdrain runs orphan-check queries on the target database after loading:

```
  ✔  Integrity verified: 0 orphaned foreign keys across all drained tables.
```

If foreign key violations are detected:
```
  ✘  2 FK violation(s) detected:
     • comments.post_id → posts.id : 3 orphaned row(s)
     • likes.user_id → users.id : 1 orphaned row(s)
```

---

---

## Architecture

```
dbdrain --from "users WHERE id=1"
    │
    ├─ internal/db/
    │    ├─ DetectEngine (postgres:// vs mysql:// vs mariadb:// vs sqlite:// / .db)
    │    ├─ SourceDB & SourceTx abstractions (Postgres pgx vs MySQL database/sql)
    │    ├─ Pure-Go SQLite driver (modernc.org/sqlite, CGO_ENABLED=0)
    │    └─ High-throughput PRAGMAs: WAL, synchronous=OFF, cache_size=-64000
    │
    ├─ internal/transpile/
    │    └─ sqlite.go: DDL transpiler from PostgreSQL / MySQL schemas to SQLite
    │                  preserves compound PKs and compound FKs
    │                  maps UUID/JSON/ENUM/ARRAY/INET/DATETIME to SQLite types
    │
    ├─ internal/config/config.go
    │    └─ Loads dbdrain.yaml: masking rules, virtual FK declarations, associations
    │
    ├─ internal/introspect/
    │    ├─ postgres.go: information_schema + pg_constraint + Citus catalogs + pg_partitioned_table
    │    │               discovers Citus distribution columns, colocation groups & reference tables
    │    │               filters physical worker shards and validates distributed FK topology
    │    └─ mysql.go: information_schema catalog discovery + MyISAM warning
    │
    ├─ internal/graph/
    │    ├─ graph.go & query.go: Directed adjacency-list FK graph with Keyset queue
    │    ├─ tarjan.go: Tarjan's SCC algorithm for circular dependency detection
    │    └─ upstream.go: Strict upward-only recursive DAG closure for leaf anchors
    │
    ├─ internal/drain/
    │    ├─ export.go: Consistent snapshot traversal, recursive CTE self-ref, BFS upward/downward
    │    │             joint tenant boundary preservation across colocated Citus tables
    │    ├─ batcher.go: Keyset pagination & stratified child sampling with window rank functions
    │    ├─ twophase.go: Two-Phase Insert Resolution for MySQL (Phase 1 NULL insert, Phase 2 UPDATE)
    │    ├─ format.go: Engine-specific SQL literal formatting (Postgres, MySQL, SQLite)
    │    └─ copy_source.go: CursorCopySource (pgx.CopyFromSource) + sync.Pool buffer recycling
    │
    ├─ internal/safety/
    │    ├─ poller.go: Autonomous cluster health poller (InnoDB history list & replica lag)
    │    └─ adaptive.go: Proportional-integral backpressure controller
    │
    ├─ internal/throttle/
    │    └─ limiter.go: Token-bucket rate limiter and worker concurrency semaphore
    │
    ├─ internal/anonymize/mask.go
    │    └─ HMAC-SHA256 masking: fake_email, redact, uuid_remap, hmac_phone, hmac_name, hmac_token
    │
    ├─ internal/verify/verify.go
    │    ├─ LEFT JOIN orphan checks on target PostgreSQL / MySQL with RLS categorization
    │    └─ RunSQLite: PRAGMA foreign_key_check on target SQLite
    │
    ├─ .github/workflows/
    │    └─ release.yml: CI/CD tag release, GoReleaser, Homebrew, Scoop, npm Trusted Publishing
    │
    ├─ .goreleaser.yaml: Multi-architecture cross-compiler & packaging specification
    │
    └─ npm/
         ├─ cli/ (runner.cjs: cross-platform binary launcher for @dbdrain/cli)
         ├─ dbdrain/ (runner.cjs: launcher for dbdrain)
         └─ platforms/ (os/cpu targeted packages: linux-x64, linux-arm64, darwin-arm64, darwin-x64, win32-x64, win32-arm64)
```

```mermaid
flowchart TD
    A["Seed Anchor (--from)"] --> B["Coordinator & Engine Discovery (Postgres / Citus / MySQL)"]
    B --> C["Catalog Introspection (Columns, PKs, FKs, Partitions, Citus Shard Pruning)"]
    C --> D["Dependency Graph & Tarjan SCC Cycle Resolution"]
    D --> E["Non-Locking Consistent Snapshot (REPEATABLE READ)"]
    E --> F["DAG Traversal: Keyset Seek / BFS Upstream & Downward"]
    F --> G["Joint Tenant Boundary Preservation (Colocated Distributed Tables)"]
    G --> H["Deterministic HMAC-SHA256 Masking (dbdrain.yaml)"]
    H --> I{"Target Mode"}
    I -->|"Target DB Stream"| J["Zero-Copy Binary COPY / Target Transaction"]
    I -->|"SQLite Target"| K["Auto DDL Transpilation & Deferred FK Loading"]
    I -->|"SQL Dump"| L["Referentially Intact SQL File Emission"]
    J --> M["Integrity Verification (--verify)"]
    K --> M
```

---

## Development

```bash
# Run all tests
go test ./...

# Cross-compile release binaries (CGO_ENABLED=0 pure-Go)
GOOS=linux   GOARCH=amd64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-linux-amd64       ./cmd/dbdrain/
GOOS=linux   GOARCH=arm64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-linux-arm64       ./cmd/dbdrain/
GOOS=darwin  GOARCH=arm64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-darwin-arm64      ./cmd/dbdrain/
GOOS=darwin  GOARCH=amd64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-darwin-amd64      ./cmd/dbdrain/
GOOS=windows GOARCH=amd64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-windows-amd64.exe   ./cmd/dbdrain/
GOOS=windows GOARCH=arm64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-windows-arm64.exe   ./cmd/dbdrain/
```

---

## Changelog

### v1.0.0
- 🌐 **Citus Distributed Table Introspection & Colocation**: Native awareness for Citus distributed clusters. Detects Citus via `pg_extension` and `citus_tables`, categorizes tables into `Distributed`, `Reference`, and `Local`, discovers distribution columns, distribution methods (hash/reference/range), and colocation IDs.
- 🛡️ **Physical Worker Shard Pruning**: Prunes physical worker shards (`users_102008`) discovered from `pg_dist_shard` to ensure extractions route exclusively through the coordinator's logical relations.
- 🔗 **Colocation & Foreign Key Topology**: Validates that foreign keys between distributed tables include the distribution column and belong to the same colocation group.
- 🏢 **Joint Tenant Boundary Preservation**: In multi-tenant Citus setups, extracts strictly preserve the joint tenant boundary across colocated tables, preventing tenant leakage even when child entity primary keys collide across tenants.
- 🏗️ **Target Citus DDL Emission**: Automatically emits `SELECT create_distributed_table(...)` and `SELECT create_reference_table(...)` calls before streaming rows.
- 🚀 **GoReleaser Release Pipeline (`.goreleaser.yaml`)**: Static builds (`CGO_ENABLED=0`) across 6 architectures (`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, `windows/amd64`, `windows/arm64`) with SHA-256 `checksums.txt`.
- 🍺 **Homebrew Tap Automation**: Automated formula updates published to `x7ssss/homebrew-tap` (`dbdrain`).
- 🪣 **Windows Scoop Bucket Automation**: Automated Scoop manifest updates published to `x7ssss/scoop-bucket` (`dbdrain.json`).
- 📦 **npm OIDC Trusted Publishing Pipeline**: GitHub Actions release workflow publishing platform packages (`@dbdrain/*`) and root launcher (`@dbdrain/cli`) with provenance.

### v0.9.0
- 🌲 **Reverse Subsetting & Upstream Ancestry Pruning (`--upstream`)**: Treats `--from` as an isolated leaf incident anchor, computing a strict upward-only recursive DAG closure along outgoing foreign keys while pruning all sibling and downstream branches.
- 🔄 **Path-Based Cycle Detection & Multi-Path Convergence**: Employs an identity path array (`visited_path`) to prevent circular loops while correctly computing ancestor unions across convergent FK paths without Cartesian duplication.
- 🗄️ **Declarative PostgreSQL Partitioning (PostgreSQL 10-16+)**: Introspects `pg_partitioned_table`, `pg_inherits`, and `relkind = 'p'`. Models partitioned tables as a single logical root in the dependency graph while retaining child partition bounds and emitting partition strategy DDL (`PARTITION BY RANGE/LIST/HASH`).
- 🛡️ **Row-Level Security (RLS) & Tenant Visibility (`--bypass-rls`)**: Executes `SET row_security = off;` on Postgres extraction connections; `--verify` distinctly categorizes integrity checks into `Valid`, `Policy-Excluded` (parent exists globally but hidden by RLS), and `Corrupted/Orphan` (parent physically missing).
- 🚫 **Conditional FK Restrictions & Dangling Null Pruning (`dbdrain.yaml`)**: Supports `restriction: false` to skip FK relationships entirely and automatically coerces dangling nullable FK columns to `NULL`, as well as conditional SQL predicates.

### v0.8.0
- 🎯 **Multi-Anchor DAG Closures**: Repeatable `--from` flag extracts unified slices across heterogeneous roots with global entity deduplication.
- ✂️ **Stratified Child Sampling (`--children-per-parent`)**: Uniformly samples child entities per parent via window ranking functions while strictly guaranteeing referential invariants.
- ⏩ **Keyset Seek Pagination**: O(1) cursor seek pagination (`WHERE (table, pk) > ($1, $2)`) eliminates `OFFSET` scan degradation.

### v0.7.0
- 🛡️ **Autonomous Cluster Health Polling (`--safe-mode`)**: Adaptive backpressure monitoring for InnoDB history list length and replication lag.
- 🚦 **Token-Bucket Rate Limiter & Concurrency Control**: `--rate-limit` and `--concurrency` controls.
- ⚡ **Fail-Fast Session Timeouts**: Statement, lock, and idle timeouts preventing catalog contention.

### v0.6.0
- 🪶 **Native SQLite Target Transpilation & Hydration**: Direct subset hydration into local SQLite database files (e.g. `--target ./dev.db`) using pure-Go `modernc.org/sqlite` without CGO.
- 🛠️ **Automatic DDL Transpilation**: Converts PostgreSQL and MySQL schemas on the fly to SQLite DDL, mapping UUID, JSON/JSONB, ENUM, ARRAY, INET, DATETIME, SERIAL, and BOOLEAN types while preserving compound PKs and FKs.
- ⚡ **High-Throughput SQLite Loading**: Automatically applies WAL journal mode, `synchronous = OFF`, in-memory temp store, and 64MB cache size.
- 🔄 **Deferred Foreign Key Hydration**: Employs `PRAGMA foreign_keys = ON; PRAGMA defer_foreign_keys = ON;` inside target transactions to effortlessly resolve circular dependencies and mutual FK constraints.
- ✅ **SQLite Integrity Verification**: Automated `PRAGMA foreign_key_check;` validation verifying 0 orphan references post-hydration.

### v0.5.0
- 🐬 **Native MySQL 8.0+ & MariaDB 10.5+ Support**: Auto-detects engine type from URI scheme (`mysql://`, `mariadb://`) or standard DSN strings.
- 📸 **Non-Locking Consistent Snapshots**: Acquires extraction transactions with `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` and `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT` without locking reads.
- 🔄 **Two-Phase Circular FK Resolution**: Resolves cyclic SCC clusters without deferred constraints or dangerous `FOREIGN_KEY_CHECKS = 0` by emitting Phase 1 NULL FK inserts followed by Phase 2 updates.
- 🔍 **Catalog Introspection**: Queries `information_schema` for storage engines, columns, primary keys, and compound foreign keys, strictly preserving compound FK ordering by `ORDINAL_POSITION`.
- ⚠️ **Storage Engine Safeguards**: Warns when encountering non-transactional / non-FK storage engines like MyISAM or MEMORY.
- 🔤 **Dialect Formatting**: Full backtick identifier escaping and MySQL-compliant literals for booleans, dates, binary, and JSON types.

### v0.4.0
- ⚡ **Zero-Copy Streaming Hydration**: Refactored `ExportToTarget()` with `pgx.CopyFromSource` and `sync.Pool` buffer recycling for flat O(1) memory usage during millions of rows extractions.
- 🚀 **Query Planner & Batching**: Banned raw `WHERE id IN (...)` in child propagation queries; enforced `= ANY($1::type[])` parameterized array queries (≤10k keys) and temporary `UNLOGGED` scratch tables with binary COPY + indexed JOIN (>10k keys).
- 📦 **npm Distribution Layer**: Created production npm distribution in `npm/` following the `optionalDependencies` pattern with `npx dbdrain` wrapper for Linux, macOS (Apple Silicon & Intel), and Windows.

### v0.3.0
- ✨ Declarative `dbdrain.yaml` config: custom masking rules, wildcard/glob matching, transform pipeline
- 🧩 Polymorphic virtual FK support: resolve Rails-style `commentable_id`/`commentable_type` associations
- ✅ `--verify`: post-hydration orphan integrity checker with Lipgloss UI and exit code 1 on violations
- 🔧 New masking transforms: `redact`, `uuid_remap` (RFC 4122 v4), `hmac_phone`, `hmac_name`, `hmac_token`

### v0.2.0
- `--depth` and `--max-rows-per-table` explosion safeguards
- `--target` direct DB streaming hydration
- Self-referencing hierarchy recursive CTE resolution

### v0.1.0
- Initial release: anchor seed, FK closure, Tarjan SCC, deterministic HMAC masking, TTY spinner

---

## License

MIT © 2026 [x7ssss](https://github.com/x7ssss)
