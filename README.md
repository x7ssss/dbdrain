<div align="center">

# 🚰 dbdrain

**Zero-downtime PostgreSQL & MySQL/MariaDB database subsetting, polymorphic FK resolution & deterministic PII masking — in a single static binary.**

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

- ⚡ **Dual Engine Support** — Native support for PostgreSQL 12+ and MySQL 8.0+ / MariaDB 10.5+
- 🔒 **Non-Locking Consistent Snapshots** — Isolation via `REPEATABLE READ READ ONLY` (Postgres) and `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT` (MySQL)
- 🔄 **Two-Phase Circular FK Resolution** — Resolves cyclic dependencies in MySQL without requiring `FOREIGN_KEY_CHECKS = 0` or deferred constraints
- ⚡ **Zero-Copy Streaming Engine** — direct `pgx.CopyFromSource` over live cursors with `sync.Pool` buffer recycling for flat O(1) memory usage during millions of rows extractions
- 🧠 **Smart Query Batching** — `= ANY($1::type[])` parameterized binding avoids GEQO degradation up to 10k keys; automatically promotes to `UNLOGGED` scratch tables + binary `COPY` + indexed `JOIN` for >10k keys
- 📦 **Zero-Config npm Distribution** — run instantly with `npx dbdrain` via platform-native `optionalDependencies` binaries
- 🔗 **Referentially intact subsets** — recursively follows every FK chain upward and downward from seed rows
- 🌿 **Self-referencing hierarchies** — `users.manager_id → users.id`, `categories.parent_id → categories.id` resolved via recursive CTEs
- 🧩 **Polymorphic associations** — virtual FKs for Rails-style `commentable_id` / `commentable_type` patterns
- 🎭 **Deterministic PII masking** — HMAC-SHA256 ensures the same input always produces the same output (no UNIQUE violations)
- 📋 **Declarative config** — `dbdrain.yaml` for custom masking rules and virtual FK declarations
- 🚀 **Direct target streaming** — bypass file I/O and hydrate a staging DB in one command
- ✅ **Integrity verification** — post-hydration orphan detection with `--verify`
- 🛡️ **Explosion safeguards** — `--depth` and `--max-rows-per-table` prevent pulling entire production datasets

---

## Engine Compatibility Table

| Capability | PostgreSQL (12+) | MySQL (8.0+) & MariaDB (10.5+) |
|------------|------------------|--------------------------------|
| **Connection Scheme** | `postgres://`, `postgresql://` | `mysql://`, `mariadb://`, or standard DSN |
| **Snapshot Isolation** | `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;` | `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;`<br>`START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT;` |
| **Circular FK Resolution** | `SET CONSTRAINTS ALL DEFERRED;` | **Two-Phase Insert Resolution** (Phase 1 NULL FK insert + Phase 2 UPDATE) |
| **Identifier Quoting** | Double quotes (`"users"."id"`) | Backticks (`` `users`.`id` ``) |
| **Parent/Child Batching** | `= ANY($1::type[])` (≤10k) / `UNLOGGED` temp table (>10k) | Chunked `IN (...)` queries in batches of 2,000 |
| **Target Hydration** | Zero-copy binary `COPY FROM` stream | Transactional bulk inserts & two-phase updates |
| **Storage Engine Inspection** | N/A (Standard PG heap) | `information_schema.TABLES` check; warns if MyISAM/MEMORY used |

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

When streaming directly to `--target` MySQL, Phase 1 inserts and Phase 2 updates are executed within the target transaction before commit.

---

## Quick Start

### Run with npx (Node.js)

No manual binary installation required:

```bash
# PostgreSQL
npx dbdrain --source "postgres://user:pass@prod-host/mydb" \
            --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
            --anonymize-pii \
            --output slice.sql

# MySQL / MariaDB
npx dbdrain --source "mysql://user:pass@prod-host:3306/mydb" \
            --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
            --anonymize-pii \
            --output slice.sql
```

Or install globally via npm:

```bash
npm install -g dbdrain
```

### Download Standalone Binary

```bash
# Linux (amd64)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-linux-amd64 -o dbdrain
chmod +x dbdrain

# macOS (Apple Silicon)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-darwin-arm64 -o dbdrain
chmod +x dbdrain

# macOS (Intel)
curl -L https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-darwin-amd64 -o dbdrain
chmod +x dbdrain

# Windows (PowerShell)
Invoke-WebRequest https://github.com/x7ssss/dbdrain/releases/latest/download/dbdrain-windows-amd64.exe -OutFile dbdrain.exe
```

### Build from source

```bash
git clone https://github.com/x7ssss/dbdrain.git
cd dbdrain
go build -o dbdrain ./cmd/dbdrain/
```

---

## Usage Examples

### 1. Slice PostgreSQL to SQL file (with PII masking)

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
  --anonymize-pii \
  --salt "my-secret-salt" \
  --output slice.sql
```

### 2. Slice MySQL to SQL file with Two-Phase Cycle Resolution

```bash
dbdrain \
  --source "mysql://root:secret@prod-host:3306/shop" \
  --from "orders WHERE created_at > '2026-01-01' LIMIT 100" \
  --anonymize-pii \
  --output slice.sql
```

### 3. Pipe directly into target DB (non-TTY: zero ANSI noise)

```bash
# PostgreSQL
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "orders WHERE created_at > NOW() - INTERVAL '30 days' LIMIT 200" \
  | psql postgres://user:pass@staging-host/stagingdb

# MySQL
dbdrain \
  --source "mysql://user:pass@prod-host:3306/mydb" \
  --from "orders WHERE created_at > '2026-01-01' LIMIT 200" \
  | mysql -u user -p -h staging-host stagingdb
```

### 4. Direct streaming hydration with integrity check

Streams directly from source database into target database with **zero disk writes**:

```bash
# PostgreSQL to PostgreSQL
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE plan = 'enterprise' LIMIT 100" \
  --target "postgres://user:pass@localhost/staging" \
  --anonymize-pii \
  --depth 2 \
  --max-rows-per-table 500 \
  --verify

# MySQL to MySQL
dbdrain \
  --source "mysql://root:secret@prod-host:3306/mydb" \
  --from "users WHERE plan = 'enterprise' LIMIT 100" \
  --target "mysql://root:secret@localhost:3306/staging" \
  --anonymize-pii \
  --depth 2 \
  --max-rows-per-table 500 \
  --verify
```

### 5. Declarative config with polymorphic FKs

```bash
dbdrain \
  --source "postgres://user:pass@prod/mydb" \
  --from "posts WHERE id = 42" \
  --config dbdrain.yaml \
  --target "postgres://user:pass@localhost/staging" \
  --verify
```

---

## Flag Reference

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--source` | string | **required** | Source database connection string (`postgres://`, `mysql://`, `mariadb://`) |
| `--from` | string | **required** | Anchor query: `"<table> [WHERE <clause>] [LIMIT <n>]"` |
| `--output` | string | `-` (stdout) | Output SQL file path (`-` = stdout) |
| `--schema` | string | `public` (PG) / DSN db (MySQL) | Schema or database name to operate on |
| `--anonymize-pii` | bool | `false` | Enable deterministic PII masking |
| `--salt` | string | `dbdrain-secret-salt` | HMAC salt for reproducible masking |
| `--depth` | int | `0` (unlimited) | Max downward FK traversal depth (parents always unlimited) |
| `--max-rows-per-table` | int | `0` (unlimited) | Hard ceiling on rows pulled per child table |
| `--target` | string | — | Target DB connection string for direct hydration (bypasses `--output`) |
| `--config` | string | `dbdrain.yaml` (auto) | Path to declarative config file |
| `--verify` | bool | `false` | Run post-hydration orphan integrity checks (requires `--target`) |

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

  # Another polymorphic example
  - child_table: attachments
    child_column: attachable_id
    parent_table_column: attachable_type
    mappings:
      post: posts.id
      user: users.id
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

```sql
-- Generated per FK relationship (PostgreSQL):
SELECT COUNT(*)
FROM public.comments c
LEFT JOIN public.posts p ON p.id = c.post_id
WHERE c.post_id IS NOT NULL AND p.id IS NULL;

-- Generated per FK relationship (MySQL):
SELECT COUNT(*)
FROM `mydb`.`comments` c
LEFT JOIN `mydb`.`posts` p ON p.`id` = c.`post_id`
WHERE c.`post_id` IS NOT NULL AND p.`id` IS NULL;
```

**TTY output (success):**
```
  ✔  Integrity verified: 0 orphaned foreign keys across all drained tables.
```

**TTY output (failure — exits with code 1):**
```
  ✘  2 FK violation(s) detected:
     • comments.post_id → posts.id : 3 orphaned row(s)
     • likes.user_id → users.id : 1 orphaned row(s)
```

---

## Architecture

```
dbdrain --from "users WHERE id=1"
    │
    ├─ internal/db/
    │    ├─ DetectEngine (postgres:// vs mysql:// vs mariadb://)
    │    ├─ SourceDB & SourceTx abstractions (Postgres pgx vs MySQL database/sql)
    │    └─ Non-locking snapshots: REPEATABLE READ & START TRANSACTION WITH CONSISTENT SNAPSHOT
    │
    ├─ internal/config/config.go
    │    └─ Loads dbdrain.yaml: masking rules, virtual FK declarations
    │
    ├─ internal/introspect/
    │    ├─ postgres.go: information_schema + pg_constraint catalog query
    │    └─ mysql.go: information_schema TABLES/COLUMNS/KEY_COLUMN_USAGE catalog query
    │                 warns on non-transactional/non-FK engines (MyISAM, MEMORY)
    │                 strictly preserves compound FK column ordering by ORDINAL_POSITION
    │
    ├─ internal/graph/
    │    ├─ graph.go & query.go: Directed adjacency-list FK graph
    │    └─ tarjan.go: Tarjan's SCC algorithm for cycle detection
    │
    ├─ internal/drain/
    │    ├─ export.go: Consistent snapshot traversal, recursive CTE self-ref, BFS upward/downward
    │    ├─ twophase.go: Two-Phase Insert Resolution for MySQL (Phase 1 NULL insert, Phase 2 UPDATE)
    │    ├─ format.go: Engine-specific SQL literal formatting (backticks, booleans, dates, binary)
    │    ├─ batcher.go: Query batcher router (ANY($1) array / temp table for PG, chunked IN for MySQL)
    │    └─ copy_source.go: CursorCopySource (pgx.CopyFromSource) + sync.Pool buffer recycling
    │
    ├─ internal/anonymize/mask.go
    │    └─ HMAC-SHA256 masking: fake_email, redact, uuid_remap, hmac_phone, hmac_name, hmac_token
    │
    ├─ internal/verify/verify.go
    │    └─ LEFT JOIN orphan checks on target DB (Postgres pgx & MySQL database/sql); exits 1 on violations
    │
    └─ npm/
         ├─ dbdrain/ (runner.cjs: cross-platform binary launcher for npx/npm)
         └─ platforms/ (os/cpu targeted packages: linux-x64, darwin-arm64, darwin-x64, win32-x64)
```

---

## Development

```bash
# Run all tests
go test ./...

# Cross-compile release binaries
GOOS=linux   GOARCH=amd64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-linux-amd64   ./cmd/dbdrain/
GOOS=darwin  GOARCH=arm64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-darwin-arm64  ./cmd/dbdrain/
GOOS=darwin  GOARCH=amd64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-darwin-amd64  ./cmd/dbdrain/
GOOS=windows GOARCH=amd64  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/dbdrain-windows-amd64.exe ./cmd/dbdrain/
```

---

## Changelog

### v0.5.0
- 🐬 **Native MySQL 8.0+ & MariaDB 10.5+ Support**: Auto-detects engine type from URI scheme (`mysql://`, `mariadb://`) or standard DSN strings.
- 📸 **Non-Locking Consistent Snapshots**: Acquires extraction transactions with `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` and `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT` without locking reads.
- 🔄 **Two-Phase Circular FK Resolution**: Resolves cyclic SCC clusters without deferred constraints or dangerous `FOREIGN_KEY_CHECKS = 0` by emitting Phase 1 NULL FK inserts followed by Phase 2 updates.
- 🔍 **Catalog Introspection**: Queries `information_schema` for storage engines, columns, primary keys, and compound foreign keys, strictly preserving compound FK ordering by `ORDINAL_POSITION`.
- ⚠️ **Storage Engine Safeguards**: Warns when encountering non-transactional / non-FK storage engines like MyISAM or MEMORY.
- 🔤 **Dialect Formatting**: Full backtick identifier escaping and MySQL-compliant literals for booleans, dates, binary, and JSON types.
- 🚀 **Dual-Engine Direct Target Hydration & Integrity Verification**: Hydrate and verify orphan integrity seamlessly across both PostgreSQL and MySQL targets.

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
