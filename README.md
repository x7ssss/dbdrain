<div align="center">

# 🚰 dbdrain

**Zero-downtime PostgreSQL database subsetting, polymorphic FK resolution & deterministic PII masking — in a single static binary.**

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
| **pg_dump** | Full database only — no subsetting |

**dbdrain** fills this void: a single Go binary, no runtime dependencies, no Docker, no YAML for basic usage.

- ⚡ **Zero-Copy Streaming Engine** — direct `pgx.CopyFromSource` over live cursors with `sync.Pool` buffer recycling for flat O(1) memory usage during millions of rows extractions
- 🧠 **Smart Query Batching** — `= ANY($1::type[])` parameterized binding avoids GEQO degradation up to 10k keys; automatically promotes to `UNLOGGED` scratch tables + binary `COPY` + indexed `JOIN` for >10k keys
- 📦 **Zero-Config npm Distribution** — run instantly with `npx dbdrain` via platform-native `optionalDependencies` binaries
- 🔗 **Referentially intact subsets** — recursively follows every FK chain upward and downward from seed rows
- 🔄 **Circular FK handling** — Tarjan's SCC detects cycles and emits `SET CONSTRAINTS ALL DEFERRED` automatically
- 🌿 **Self-referencing hierarchies** — `users.manager_id → users.id`, `categories.parent_id → categories.id` resolved via recursive CTEs
- 🧩 **Polymorphic associations** — virtual FKs for Rails-style `commentable_id` / `commentable_type` patterns
- 🎭 **Deterministic PII masking** — HMAC-SHA256 ensures the same input always produces the same output (no UNIQUE violations)
- 📋 **Declarative config** — `dbdrain.yaml` for custom masking rules and virtual FK declarations
- 🚀 **Direct target streaming** — bypass file I/O and hydrate a staging DB in one command
- ✅ **Integrity verification** — post-hydration orphan detection with `--verify`
- 🛡️ **Explosion safeguards** — `--depth` and `--max-rows-per-table` prevent pulling entire production datasets

---

## Quick Start

### Run with npx (Node.js)

No manual binary installation required:

```bash
npx dbdrain --source "postgres://user:pass@prod-host/mydb" \
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

### 1. Slice to SQL file (with PII masking)

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE id IN (1, 2, 3) LIMIT 50" \
  --anonymize-pii \
  --salt "my-secret-salt" \
  --output slice.sql
```

### 2. Pipe directly into psql (non-TTY: zero ANSI noise)

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "orders WHERE created_at > NOW() - INTERVAL '30 days' LIMIT 200" \
  | psql postgres://user:pass@staging-host/stagingdb
```

### 3. Direct streaming hydration with integrity check

Streams directly from source PostgreSQL into target PostgreSQL with **zero disk writes** and **flat O(1) memory consumption**:

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE plan = 'enterprise' LIMIT 100" \
  --target "postgres://user:pass@localhost/staging" \
  --anonymize-pii \
  --depth 2 \
  --max-rows-per-table 500 \
  --verify
```

### 4. Declarative config with polymorphic FKs

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
| `--source` | string | **required** | Source PostgreSQL connection string |
| `--from` | string | **required** | Anchor query: `"<table> [WHERE <clause>] [LIMIT <n>]"` |
| `--output` | string | `-` (stdout) | Output SQL file path (`-` = stdout) |
| `--schema` | string | `public` | PostgreSQL schema to operate on |
| `--anonymize-pii` | bool | `false` | Enable deterministic PII masking |
| `--salt` | string | `dbdrain-secret-salt` | HMAC salt for reproducible masking |
| `--depth` | int | `0` (unlimited) | Max downward FK traversal depth (parents always unlimited) |
| `--max-rows-per-table` | int | `0` (unlimited) | Hard ceiling on rows pulled per child table |
| `--target` | string | — | Target DB for direct streaming hydration (bypasses `--output`) |
| `--config` | string | `dbdrain.yaml` (auto) | Path to declarative config file |
| `--verify` | bool | `false` | Run post-hydration orphan integrity checks (requires `--target`) |

---

## Streaming Engine & Query Planner (v0.4.0)

### 1. Zero-Copy `pgx.CopyFromSource` Architecture

When `--target` is specified, `dbdrain` does not buffer records into memory (`[]Row` or `[][]any`). Instead, it wires a custom `CursorCopySource` directly between the source cursor and the target PostgreSQL binary COPY stream:

```
Source Postgres (Snapshot Tx)
      │
      ▼  (wire protocol rows)
pgx.Rows Cursor
      │
      ▼  (row-by-row)
CursorCopySource (implements pgx.CopyFromSource)
      ├── sync.Pool buffer recycling (0 allocations per row)
      ├── In-place HMAC deterministic PII masking
      └── Real-time progress metric callbacks
      │
      ▼  (binary COPY stream)
Target Postgres (targetTx.CopyFrom with DEFERRED constraints)
```

- **O(1) Heap Memory**: Regardless of whether you extract 10 rows or 10,000,000 rows, memory consumption remains flat.
- **Pooled Buffers**: Slice buffers are recycled through `sync.Pool` and zeroed out for GC safety.

### 2. Parameterized Array Query Batching (`= ANY($1)`)

When resolving parent and child relationships across tables, dbdrain avoids naive `WHERE col IN (...)` query generation:

- **1 to 10,000 keys**: Queries are compiled into parameterized array lookups:
  ```sql
  SELECT "id", "user_id", "total"
  FROM "public"."orders"
  WHERE "user_id" = ANY($1::bigint[]);
  ```
  `$1` is passed as a typed native array (`[]int64`, `[]string`), preserving server-side prepared statement cache hits and avoiding Genetic Query Optimizer (GEQO) threshold triggers.

- **> 10,000 keys**: To avoid query text explosion and memory limits, keys are streamed into an `UNLOGGED` temporary scratch table via binary COPY:
  ```sql
  CREATE TEMP TABLE "_dbdrain_k_orders" (key text PRIMARY KEY) ON COMMIT DROP;
  -- Stream parent keys via COPY ...
  SELECT t.* FROM "public"."orders" t
  INNER JOIN "_dbdrain_k_orders" k ON k.key = t."user_id"::text;
  ```

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
-- Generated per FK relationship:
SELECT COUNT(*)
FROM public.comments c
LEFT JOIN public.posts p ON p.id = c.post_id
WHERE c.post_id IS NOT NULL AND p.id IS NULL;
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
    ├─ internal/config/config.go
    │    └─ Loads dbdrain.yaml: masking rules, virtual FK declarations
    │       Priority-based wildcard FindRule (exact > glob)
    │
    ├─ internal/introspect/postgres.go
    │    └─ Queries information_schema + pg_constraint
    │       Discovers columns, PKs, composite FKs, deferrability
    │
    ├─ internal/graph/graph.go & internal/graph/query.go
    │    ├─ Builds directed adjacency-list FK graph
    │    ├─ Registers virtual FK edges from config
    │    └─ Query planner: BuildANYQuery (= ANY($1::type[])), BuildTempTableJoinQuery
    │
    ├─ internal/graph/tarjan.go
    │    └─ Tarjan's SCC → detects circular FK dependencies
    │       Emits SET CONSTRAINTS ALL DEFERRED when cycles found
    │
    ├─ internal/drain/
    │    ├─ export.go: REPEATABLE READ snapshot, CTE self-ref, BFS upward/downward
    │    ├─ batcher.go: Query planner router (ANY($1) array vs UNLOGGED temp table COPY + JOIN)
    │    └─ copy_source.go: CursorCopySource (pgx.CopyFromSource) + sync.Pool O(1) buffer recycling
    │
    ├─ internal/anonymize/mask.go
    │    └─ HMAC-SHA256 masking: fake_email, redact, uuid_remap, hmac_phone, hmac_name, hmac_token
    │
    ├─ internal/verify/verify.go
    │    └─ LEFT JOIN orphan queries on target DB; exits 1 on violations
    │
    └─ npm/
         ├─ dbdrain/ (runner.cjs: cross-platform binary launcher for npx/npm)
         └─ platforms/ (os/cpu targeted packages: linux-x64, darwin-arm64, darwin-x64, win32-x64)
```

---

## Development

```bash
# Run all tests (71 tests across 6 packages)
go test ./...

# Cross-compile release binaries
GOOS=linux   GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-linux-amd64   ./cmd/dbdrain/
GOOS=darwin  GOARCH=arm64  CGO_ENABLED=0 go build -o dist/dbdrain-darwin-arm64  ./cmd/dbdrain/
GOOS=darwin  GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-darwin-amd64  ./cmd/dbdrain/
GOOS=windows GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-windows-amd64.exe ./cmd/dbdrain/
```

---

## Changelog

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
