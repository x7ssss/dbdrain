# dbdrain

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://go.dev)
[![npm](https://img.shields.io/npm/v/dbdrain?logo=npm&color=CB3837)](https://www.npmjs.com/package/dbdrain)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/x7sss/dbdrain)](https://github.com/x7sss/dbdrain/releases)
[![Runtime Dependencies](https://img.shields.io/badge/dependencies-0%20(static%20Go)-success.svg)](https://github.com/x7sss/dbdrain)

Zero-downtime, non-locking database subsetting, schema transpilation, and deterministic PII masking engine for PostgreSQL, MySQL, and SQLite packaged as a single static binary. Employs multi-anchor directed acyclic graph traversal, Tarjan SCC cycle resolution, stratified window sampling, and keyset seek pagination to extract referentially intact slices without production lock contention.

---

## 🏛️ System Architecture & Data Pipeline

`dbdrain` operates as an in-memory graph traversal pipeline and streaming data pump. It introspects relational topologies, resolves circular dependencies using Tarjan's Strongly Connected Components algorithm, and streams data between source engines and targets with flat O(1) memory allocation:

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Engine & Topology Discovery Layer                    │
│   • Source Detect: postgres:// | mysql:// | mariadb:// | sqlite://    │
│   • Citus Coordinator Metadata: citus_tables & pg_dist_partition       │
│   • Partition Trees: pg_partitioned_table (RANGE / LIST / HASH)       │
│   • Physical Shard Pruning: Strips worker shards (users_102008)        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│               Catalog Introspection & Directed DAG Engine              │
│   • Adjacency-list Foreign Key Graph with Compound Key Normalization   │
│   • Tarjan SCC Algorithm: Detects circular cycles & self-references    │
│   • Virtual Foreign Keys: Rails-style polymorphic discriminator maps   │
│   • Declarative Config: dbdrain.yaml rules & relationship pruning      │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Snapshot Isolation & Traversal Core                  │
│   • PostgreSQL: REPEATABLE READ READ ONLY (Lock timeout = 100ms)       │
│   • MySQL: START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT       │
│   • Autonomous Health Poller: Monitors InnoDB History List & Repl Lag  │
│   • Zero-Leak Invariant: Yields connection before backpressure sleeps  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         ▼                          ▼                          ▼
┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
│ Multi-Anchor BFS │       │ Keyset Seek Pump │       │ Upstream Reverse │
│ • Repeatable     │       │ • O(1) cursor    │       │ • Leaf incident  │
│   --from roots   │       │   pagination     │       │   anchor         │
│ • Global entity  │       │ • Window-ranked  │       │ • Upward-only    │
│   deduplication  │       │   child sampling │       │   DAG closure    │
└────────┬─────────┘       └────────┬─────────┘       └────────┬─────────┘
         │                          │                          │
         └──────────────────────────┼──────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Transformation & PII Masking Engine                  │
│   • Deterministic HMAC-SHA256: Consistent pseudonymization per salt    │
│   • Preserves UNIQUE constraints across re-mapped columns              │
│   • Transforms: fake_email, redact, uuid_remap, hmac_phone, hmac_token │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         ▼                          ▼                          ▼
┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
│ Streaming Target │       │ SQLite Transpile │       │ Two-Phase MySQL  │
│ • Binary COPY    │       │ • Native DDL map │       │ • Phase 1: NULL  │
│   (pgx sync.Pool)│       │ • WAL & cache    │       │   FK insert      │
│ • Flat O(1) RAM  │       │ • PRAGMA defer   │       │ • Phase 2: UPDATE│
└────────┬─────────┘       └────────┬─────────┘       └────────┬─────────┘
         │                          │                          │
         └──────────────────────────┼──────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                  Post-Hydration Verification Engine                    │
│   • PostgreSQL / MySQL: LEFT JOIN orphan queries & RLS categorization  │
│   • SQLite: Automated PRAGMA foreign_key_check audit                   │
└────────────────────────────────────────────────────────────────────────┘
```

1. **Topology Discovery**: Introspects source catalogs (`information_schema`, `pg_constraint`, `citus_tables`, `pg_partitioned_table`) to map relations, distribution keys, and partition bounds while pruning physical worker shards.
2. **Graph Construction & Cycle Detection**: Builds a directed foreign key graph. Executes Tarjan's Strongly Connected Components (SCC) algorithm to isolate circular reference loops.
3. **Non-Locking Snapshot Acquisition**: Initializes transaction-level read-only consistent snapshots (`REPEATABLE READ`). Continuously monitors cluster backpressure (`--safe-mode`).
4. **Relational DAG Slicing**: Traverses records across multi-anchor seeds (`--from`) or reverse incident trees (`--upstream`). Employs $O(1)$ keyset seek pagination and stratified window child sampling.
5. **Deterministic Masking**: Evaluates `dbdrain.yaml` rules, applying HMAC-SHA256 pseudonymization to PII while maintaining relational integrity and unique constraints.
6. **Target Hydration & Verification**: Streams data into PostgreSQL targets via zero-copy binary `COPY`, into SQLite via on-the-fly DDL transpilation, or into MySQL via two-phase cycle insertion. Concludes with comprehensive orphan integrity verification (`--verify`).

---

## 🎯 The Concrete Problem

Extracting development and testing subsets from large production databases exposes critical engineering bottlenecks that legacy dump utilities fail to resolve:

- **Cartesian Graph Explosions:** Naive recursive extractors traverse foreign keys blindly. Slicing a single tenant or user record frequently cascades down high-volume audit logs, analytics events, and shared lookup tables, extracting millions of unneeded rows and exhausting host memory.
- **Lock Convoys & Undo Log Saturation:** Traditional dump commands (`pg_dump`, `mysqldump`) execute long-running single transactions. In MySQL, holding read-view snapshots during large extractions causes InnoDB History List Length (`trx_rseg_history_len`) to spike into millions of pages, exhausting rollback segments and stalling production master writes. In PostgreSQL, long-running transactions block autovacuum dead tuple reclamation, causing catastrophic table bloat.
- **Circular Foreign Key Deadlocks in MySQL:** PostgreSQL supports deferred foreign keys via `SET CONSTRAINTS ALL DEFERRED`. MySQL and MariaDB do not support deferred constraints. Conventional tools bypass this by disabling constraints (`FOREIGN_KEY_CHECKS = 0`), which requires elevated administrative privileges (`SUPER` or `SYSTEM_VARIABLES_ADMIN`) and risks undetected target data corruption.
- **Fan-Out Skew & Child Starvation:** Setting table-level row quotas (`LIMIT N`) creates extreme distribution skew: a handful of high-activity parent entities (such as power users with 50,000 orders) exhaust the entire row budget, leaving all remaining parent entities with zero child records.
- **Keyset Scan Degradation ($O(N)$ OFFSET Latency):** Subsetting tools relying on SQL `OFFSET` pagination suffer severe latency degradation as traversal depths increase into millions of rows, forcing full index scans and heavy disk I/O.
- **Row-Level Security (RLS) Silent Exclusion:** In multi-tenant systems, RLS policies silently filter parent records during cross-table queries. Traditional subset verification cannot differentiate between actual data corruption and rows hidden by active security policies.
- **Citus Distributed Shard Contamination:** In Citus distributed clusters, unaware tools ingest physical worker shards (`users_102008`) directly, creating duplicate rows, broken colocation references, and primary key collisions across worker nodes.

---

## 🛡️ Core Engineering Invariants

- **Zero Third-Party Runtime Dependencies:** Compiles to a single static binary with `CGO_ENABLED=0` in Go 1.22+. Uses pure-Go database drivers (`pgx/v5`, `go-sql-driver/mysql`, `modernc.org/sqlite`).
- **Zero-Leak Snapshot Invariant:** When pauses occur due to cluster backpressure (`--safe-mode`), `dbdrain` finishes or rolls back the current bounded chunk before pausing. It never sleeps while holding an open consistent-read transaction, preventing undo log bloat and allowing database purge threads to clean dead tuples.
- **Flat O(1) Streaming Memory Footprint:** Buffers and serialization workers recycle memory allocations through `sync.Pool`. Combined with binary cursor streaming (`pgx.CopyFromSource`), memory consumption remains strictly bounded (< 50 MB) during multi-gigabyte extractions.
- **Privilege-Free MySQL Cycle Resolution:** Resolves circular foreign key loops in MySQL using Tarjan SCC two-phase planning (inserting with nullified foreign keys, then updating post-insert) without requiring `FOREIGN_KEY_CHECKS = 0` or administrative privileges.
- **Deterministic Pseudo-Random Sampling:** Child entity selection under `--children-per-parent` is ordered by `MD5(PK || seed)`, guaranteeing reproducible, referentially intact slices across repeated runs.
- **Strict Referential Closure:** Every extracted child entity retains complete foreign key lineages up to its designated root. The process guarantees zero orphan records upon target insertion.

---

## ⚡ Engine Compatibility Matrix

| Capability | PostgreSQL (12+) | MySQL (8.0+) & MariaDB (10.5+) | SQLite (3.x) |
| :--- | :--- | :--- | :--- |
| **Connection Scheme** | `postgres://`, `postgresql://` | `mysql://`, `mariadb://`, standard DSN | `sqlite://`, or `.db` / `.sqlite` file path |
| **Snapshot Isolation** | `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;` | `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;`<br>`START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT;` | Serialized transaction (`BEGIN TRANSACTION;`) with WAL mode |
| **Circular FK Resolution** | `SET CONSTRAINTS ALL DEFERRED;` | **Two-Phase Insert Resolution** (Phase 1 NULL FK insert, Phase 2 UPDATE) | `PRAGMA foreign_keys = ON;`<br>`PRAGMA defer_foreign_keys = ON;` |
| **Identifier Quoting** | Double quotes (`"users"."id"`) | Backticks (`` `users`.`id` ``) | Double quotes (`"users"."id"`) |
| **Parent/Child Batching** | `= ANY($1::type[])` (<=10k) / `UNLOGGED` temp table (>10k) | Chunked `IN (...)` queries in batches of 2,000 | Direct high-throughput batch hydration |
| **Target Hydration** | Zero-copy binary `COPY FROM` stream | Transactional bulk inserts & two-phase updates | High-throughput bulk loading PRAGMAs + deferred FK transaction |
| **Integrity Verification** | Custom LEFT JOIN orphan queries (`--verify`) | Custom LEFT JOIN orphan queries (`--verify`) | Automated `PRAGMA foreign_key_check;` (`--verify`) |

---

## 🌐 Citus Distributed Table Introspection & Colocation

`dbdrain` provides native awareness for Citus distributed clusters, transparently subsetting multi-tenant databases without manual coordinator configuration:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                  Citus Coordinator Logical Discovery                   │
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
│                Referentially Intact Tenant Extraction                  │
│                                                                        │
│   users WHERE tenant_id = 42 ---> orders WHERE tenant_id = 42          │
│   (Zero leakage to tenant 43 even if primary keys collide across IDs)  │
└────────────────────────────────────────────────────────────────────────┘
```

### Key Citus Capabilities:
1. **Coordinator Metadata Discovery**: Introspects `citus_tables` and `pg_dist_partition` to discover distribution columns (e.g. `tenant_id`), distribution methods (Hash, Reference, Range, Append), and colocation IDs.
2. **Physical Shard Pruning**: Queries `pg_dist_shard` to identify physical worker shards (e.g. `users_102008`). Prunes them from cataloging and graph node creation, routing queries exclusively through the coordinator's logical relations.
3. **Colocation & Foreign Key Topology**: Validates that foreign keys between distributed tables include the distribution column and belong to identical colocation groups.
4. **Joint Tenant Boundary Preservation**: In multi-tenant systems, colocated tables share the distribution key. When an extraction begins with an anchor query (e.g. `--from "users WHERE tenant_id = '42'"`), `dbdrain` captures the active tenant key set and injects the joint tenant boundary condition (`"tenant_id" IN ('42')`) into all downstream and upstream queries. This prevents cross-tenant row contamination even when primary keys collide across tenants.
5. **Target Citus DDL Emission**: Automatically emits distribution commands before streaming data:
   ```sql
   SELECT create_distributed_table('users', 'tenant_id');
   SELECT create_reference_table('countries');
   ```

---

## 🪶 Native SQLite Target Transpilation & Hydration

When `--target` points to an SQLite database file (e.g. `--target ./dev.db` or `--target sqlite://local.db`), `dbdrain` executes automatic schema transpilation and accelerated bulk loading:

1. **Automatic Schema Transpilation**: Converts source PostgreSQL or MySQL schemas into SQLite DDL, mapping types, compound primary keys, and foreign keys.
2. **High-Throughput Loading PRAGMAs**:
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
   -- Batched bulk inserts
   COMMIT;
   ```
4. **Automated Verification**: Automatically runs `PRAGMA foreign_key_check;` upon completion to verify zero broken foreign keys.

### Type Transpilation Matrix

| Source Column Type (PostgreSQL / MySQL) | SQLite Affine Type | Transpilation Behavior |
| :--- | :--- | :--- |
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

## 🔄 Two-Phase Circular FK Resolution (MySQL / MariaDB)

MySQL and MariaDB do not support deferred foreign keys. Using Tarjan's Strongly Connected Components algorithm, `dbdrain` detects circular dependency loops (such as `users.team_id -> teams.id` and `teams.lead_id -> users.id`, or self-referencing `users.manager_id -> users.id`) and executes a two-phase insertion plan without disabling foreign key checks:

```text
┌─────────────────────────────────────────────────────────────┐
│                       Tarjan SCC Cycle                      │
│                                                             │
│       users (team_id [nullable]) ----> teams                │
│         ^                                |                  │
│         |------------- lead_id ----------|                  │
└─────────────────────────────────────────────────────────────┘
```

1. **Phase 1 (Safe Insert)**:
   For every row in a cyclic component, any nullable foreign key pointing to a mutually dependent table within the SCC is temporarily coerced to `NULL`:
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

## 🔬 Advanced Subsetting Algorithms

### 1. Multi-Anchor DAG Closure
Instead of restricting extractions to a single root table, `dbdrain` accepts multiple repeatable `--from` flags to extract cohesive slices across disparate entity domains:

```bash
dbdrain --source "postgres://user:pass@prod-host/mydb" \
        --from "users WHERE id IN (101, 102) LIMIT 20" \
        --from "tenants WHERE tier = 'enterprise'" \
        --target "postgres://user:pass@staging-host/mydb"
```

- **Unified BFS Queue**: All seed rows across defined anchors are enqueued into a unified BFS traversal queue.
- **Global `(table_name, primary_key)` Deduplication**: Visited row tracking is shared globally across roots. Entities reachable from multiple roots are visited and emitted exactly once, preventing duplicate key violations and eliminating Cartesian explosion.

### 2. Stratified Child Sampling (`--children-per-parent`)
Table-level limits (`--max-rows-per-table`) suffer from fan-out skew. `--children-per-parent N` enforces a uniform child quota per parent entity down the DAG using database window ranking functions:

```bash
# Pull enterprise tenants, but limit each tenant to at most 5 users, and each user to at most 3 orders
dbdrain --source "postgres://user:pass@prod-host/mydb" \
        --from "tenants WHERE tier = 'enterprise'" \
        --children-per-parent 5 \
        --seed "deterministic-seed-alpha" \
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

The window condition `rn = 1 OR (rn <= N AND total_children > 1)` guarantees that every parent with >= 1 child retains at least 1 child, while capping each parent at N children.

### 3. Keyset Seek Pagination for Queues
Traditional `OFFSET` pagination suffers from $O(N)$ scan degradation as queue depths grow into millions of rows. `dbdrain` implements composite keyset seek queries:

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

This guarantees **$O(1)$ cursor seek time** regardless of traversal depth.

### 4. Target-Directed Reverse Slicing (`--upstream`)
When diagnosing production incidents or extracting isolated test cases (e.g. `--from "charges WHERE id = 98234" --upstream`), standard forward subsetting pulls every sibling charge, invoice item, customer review, and audit event.

`--upstream` flips the traversal engine into strict reverse mode:
- **Leaf-Anchor Isolation**: Treats the `--from` seed as an isolated leaf incident anchor.
- **Upward-Only Closure**: Performs a strict recursive upward DAG closure along outgoing foreign keys, pruning all sibling down-traversals.
- **Cycle Avoidance via Path Identity**: Maintains an identity path array (`visited_path`) to prevent circular parent loops.
- **Multi-Path Convergence**: When multiple FK paths converge on the same ancestor (e.g. `charges -> invoices -> customers` AND `charges -> payments -> customers`), computes the exact union without Cartesian duplication.

### 5. Declarative PostgreSQL Partitioning (PG 10-16+)
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

## 🛡️ Production Safety Controls & Adaptive Pacing

Extracting large data slices from live production primary or replica databases requires strict safety guarantees:

### 1. Fail-Fast Session Timeouts
For PostgreSQL sources, dedicated extraction connections automatically execute strict transaction-level session guards:
```sql
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '100ms';
SET LOCAL idle_in_transaction_session_timeout = '60s';
SET LOCAL application_name = 'dbdrain-worker';
```
With `lock_timeout = '100ms'`, if concurrent DDL migrations (such as `ALTER TABLE`) are running, `dbdrain` fails fast with jittered exponential backoff rather than queuing behind locks and blocking production transactions.

### 2. Autonomous Cluster Health Poller (`--safe-mode`)
When `--safe-mode` is enabled, an independent background monitor continuously inspects cluster load every 5 seconds via dedicated autocommit connections:
- **MySQL InnoDB Health**:
  - Monitors `trx_rseg_history_len` (InnoDB History List Length) via `information_schema.INNODB_METRICS`.
  - Monitors replication lag (`Seconds_Behind_Source`) via `SHOW REPLICA STATUS`.
  - **Thresholds**: History List Length > 100,000 triggers adaptive backoff pacing. History List Length > 1,000,000 or Replication Lag > `--max-lag` pauses new chunk extraction immediately until the cluster recovers.
- **PostgreSQL Health**:
  - Monitors active sessions, IO waiters, lock waiters, and oldest active transaction age (`now() - xact_start`) via `pg_stat_activity`, and replication slot lag via `pg_replication_slots`.
  - **Thresholds**: Oldest transaction age > `--max-lag` or lock waiters spike (>= 5) pauses extraction immediately. Elevated IO waiters (>= 10) triggers adaptive backoff pacing.

---

## 📋 Declarative Configuration (`dbdrain.yaml`)

Place a `dbdrain.yaml` in your working directory (or specify via `--config`):

```yaml
rules:
  # Exact table + column
  - table: users
    column: email
    transform: fake_email       # anon_<hmac>@drain.local

  - table: users
    column: password
    transform: redact           # [REDACTED]

  # Wildcard table, glob column pattern
  - table: "*"
    column: "*_token"
    transform: uuid_remap       # deterministic UUID v4 from HMAC

  # Exact table, wildcard column
  - table: audit_logs
    column: "*"
    transform: redact

virtual_foreign_keys:
  # Rails-style polymorphic association:
  # comments.commentable_id -> posts.id (when commentable_type = 'post')
  # comments.commentable_id -> videos.id (when commentable_type = 'video')
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

| Transform | Output Format | Description |
| :--- | :--- | :--- |
| `fake_email` | `anon_<hmac[:10]>@drain.local` | Deterministic anonymized email preserving uniqueness |
| `redact` | `[REDACTED]` | Constant static redaction string |
| `uuid_remap` | `RFC 4122 UUID v4` | Deterministic UUID derived from HMAC hash |
| `hmac_phone` | `+1555<hmac[:7]>` | E.164 compliant mock phone number |
| `hmac_name` | `User_<hmac[:6]>` | Pseudonymized display name |
| `hmac_token` | `tok_<hmac[:16]>` | Pseudonymized API token |
| `redact_password` | `$2a$12$...` | Static bcrypt hash string |

---

## 📦 Installation & Distribution

| Channel | Platform | Command |
| :--- | :--- | :--- |
| **npm / npx** | Cross-Platform (Node.js 18+) | `npx dbdrain` or `npm install -g dbdrain` |
| **Homebrew** | macOS & Linux | `brew install x7sss/tap/dbdrain` |
| **Scoop** | Windows 10/11 / Windows Server | `scoop bucket add x7sss https://github.com/x7sss/scoop-bucket`<br>`scoop install dbdrain` |
| **Direct Binary** | Linux (`amd64`, `arm64`)<br>macOS (`arm64`, `amd64`)<br>Windows (`amd64`, `arm64`) | [GitHub Releases](https://github.com/x7sss/dbdrain/releases/latest) |
| **Go Toolchain** | Go 1.22+ Toolchain | `go install github.com/x7sss/dbdrain/cmd/dbdrain@latest` |

---

## 🚀 Concrete Production Examples

### 1. PostgreSQL to Local SQLite Database (Development / Testing)

```bash
dbdrain \
  --source "postgres://user:secret@prod-db.internal:5432/production" \
  --from "users WHERE plan = 'pro' LIMIT 50" \
  --target "./dev.db" \
  --anonymize-pii \
  --verify
```

Terminal Output:
```text
======================================================================
  dbdrain v1.0.0: Relational Subsetting Engine
======================================================================
  Source:           PostgreSQL 16.2 (production)
  Target:           SQLite 3 (./dev.db)
  Anchors:          users WHERE plan = 'pro' LIMIT 50
  PII Masking:      HMAC-SHA256 active (salt: dbdrain-secret-salt)
----------------------------------------------------------------------
  [*] Transpiling PostgreSQL DDL to SQLite affine schema...
  [*] Hydrating SQLite database with high-throughput PRAGMAs (WAL, sync=OFF)...
  [✔] users:         50 rows
  [✔] profiles:      50 rows
  [✔] subscriptions: 50 rows
  [✔] invoices:     342 rows
  [✔] items:       1,204 rows
----------------------------------------------------------------------
  [*] Executing post-hydration integrity audit (PRAGMA foreign_key_check)...
  ✔ Integrity verified: 0 orphaned foreign keys across all drained tables.
======================================================================
```

### 2. Multi-Tenant Citus Cluster Extraction with Tenant Boundary Preservation

```bash
dbdrain \
  --source "postgres://postgres:secret@citus-coordinator.internal:5432/app" \
  --from "tenants WHERE id = 'tenant_9842'" \
  --children-per-parent 10 \
  --target "postgres://postgres:secret@staging.internal:5432/app" \
  --safe-mode \
  --verify
```

### 3. Isolated Reverse Incident Subsetting (`--upstream`)

```bash
# Extract isolated failed charge and its ancestors without pulling sibling charges or reviews
dbdrain \
  --source "postgres://user:secret@prod-db.internal:5432/production" \
  --from "charges WHERE id = 'ch_982341'" \
  --upstream \
  --target "./incident_repro.db" \
  --verify
```

### 4. MySQL Two-Phase Circular Dependency Resolution

```bash
dbdrain \
  --source "mysql://app:secret@prod-mysql.internal:3306/shop" \
  --from "teams WHERE id = 1" \
  --target "mysql://app:secret@localhost:3306/shop_dev" \
  --anonymize-pii \
  --verify
```

---

## 📖 CLI Flag Reference

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--source <dsn>` | string | **required** | Source database connection string (`postgres://`, `mysql://`, `mariadb://`) |
| `--from <query>` | string | **required** | Anchor query (repeatable): `"<table> [WHERE <clause>] [LIMIT <n>]"` |
| `--target <dsn>` | string | - | Target database connection string or SQLite file path (e.g. `./dev.db`) |
| `--output <path>` | string | `-` (stdout) | Output SQL dump file destination (`-` = stdout) |
| `--children-per-parent <n>`| int | `0` (unlimited) | Quota of child rows pulled per parent entity down the DAG |
| `--seed <string>` | string | `dbdrain-sampling` | Deterministic seed for reproducible stratified child sampling |
| `--schema <name>` | string | `public` / DSN db | Schema or database catalog name to operate on |
| `--anonymize-pii` | bool | `false` | Enables deterministic HMAC-SHA256 PII masking |
| `--salt <string>` | string | `dbdrain-secret-salt`| Secret salt for deterministic HMAC pseudonymization |
| `--depth <n>` | int | `0` (unlimited) | Maximum downward foreign key traversal depth (parents always traversed) |
| `--max-rows-per-table <n>`| int | `0` (unlimited) | Hard ceiling on total rows extracted per individual table |
| `--config <path>` | string | `dbdrain.yaml` | Path to declarative YAML configuration file |
| `--verify` | bool | `false` | Runs post-hydration orphan integrity verification against target |
| `--rate-limit <n>` | int | `0` (unlimited) | Rate limit throughput ceiling in rows per second |
| `--concurrency <n>` | int | `4` | Maximum parallel worker goroutines |
| `--safe-mode` | bool | `false` | Enables autonomous background cluster health polling and backpressure pacing |
| `--max-lag <duration>` | duration | `30s` | Maximum allowed replica lag or transaction age before pausing extraction |
| `--upstream` | bool | `false` | Treats `--from` as an isolated leaf anchor; executes strict upward-only closure |
| `--bypass-rls` | bool | `false` | Bypasses PostgreSQL Row-Level Security via `SET row_security = off;` |
| `-h, --help` | flag | - | Displays command help interface |
| `-v, --version` | flag | - | Displays installed version string |

---

## 🚦 SemVer 2.0 Exit Code Contract

`dbdrain` enforces a standardized exit code contract across all CLI subcommands:

| Exit Code | Classification | Description |
| :--- | :--- | :--- |
| `0` | `SUCCESS` | Subsetting, streaming hydration, or SQL dump completed with zero integrity errors |
| `1` | `ORPHAN_INTEGRITY_VIOLATION` | Post-hydration verification detected one or more broken foreign key relationships |
| `2` | `CONFIG_ARG_ERROR` | Missing required parameters, invalid connection DSN, or unparseable YAML configuration |
| `3` | `DB_AUTH_CONNECTION_ERROR` | Source or target database connection refused, network timeout, or authentication failure |
| `4` | `HEALTH_SAFETY_ABORT` | Safe-mode threshold breached (replication lag or history list length exceeded ceiling) |
| `5` | `SCHEMA_INTROSPECTION_ERROR` | Failed to introspect database schema, unsupported column type, or invalid FK topology |
| `6` | `IO_STREAMING_ERROR` | Disk full, socket broken during binary streaming, or unwriteable target destination |

---

## 🛡️ Failure & Production Safety Matrix

| Threat / Invariant | Risk Level | Internal Defense Mechanism | Override Flag |
| :--- | :--- | :--- | :--- |
| **Undo Log Saturation (MySQL)** | `CRITICAL` | Safe-mode poller tracks `trx_rseg_history_len`; pauses if > 100k | `--safe-mode=false` |
| **Catalog Lock Contention** | `HIGH` | Sets `lock_timeout = 100ms`; fails fast with backoff instead of queueing | None (Strict) |
| **Vacuum Blocker (PostgreSQL)** | `HIGH` | Enforces zero-leak snapshot invariant; yields transaction before sleeping | None (Automatic) |
| **Circular FK Deadlock (MySQL)** | `HIGH` | Tarjan SCC two-phase planning (insert NULL FKs, then update references) | None (Automatic) |
| **Cartesian Fan-Out Blowup** | `HIGH` | Global entity deduplication and depth limits (`--depth`, `--max-rows`) | `--depth <n>` |
| **Child Distribution Skew** | `MEDIUM` | Stratified window sampling ensures uniform child quota across all parents | `--children-per-parent` |
| **Replication Lag Spikes** | `HIGH` | Background health poller halts chunk ingestion when lag > `--max-lag` | `--max-lag <dur>` |
| **RLS Policy False Positives** | `MEDIUM` | Three-tier verification categorizes Valid, Policy-Excluded, and Orphan | `--bypass-rls` |
| **Citus Worker Shard Contamination**| `HIGH` | Introspects `pg_dist_shard` and prunes physical relations from DAG | None (Automatic) |
| **PII Data Leakage** | `CRITICAL` | Deterministic HMAC-SHA256 masking replaces PII before network transmission | `--anonymize-pii` |

---

## 📄 License

MIT © x7sss
