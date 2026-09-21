<div align="center">

# 🚰 dbdrain

**Zero-downtime PostgreSQL database subsetting & deterministic PII anonymization — in a single static binary.**

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://go.dev)
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

**dbdrain** fills this void: a single Go binary, no runtime dependencies, no YAML, no Docker.

- 🔗 **Referentially intact subsets** — recursively follows every FK chain upward and downward from your seed rows
- 🔄 **Circular FK handling** — Tarjan's SCC detects cycles and emits `SET CONSTRAINTS ALL DEFERRED` automatically
- 🌿 **Self-referencing hierarchies** — `users.manager_id → users.id`, `categories.parent_id → categories.id` resolved via recursive CTEs
- 🎭 **Deterministic PII masking** — HMAC-SHA256 — same input always produces the same masked output (no UNIQUE violations)
- 🚀 **Direct target streaming** — bypass file I/O and hydrate a staging DB in one command
- 🛡️ **Explosion safeguards** — `--depth` and `--max-rows-per-table` prevent pulling entire production datasets

---

## Quick Start

### Download

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

### 3. Direct streaming hydration (--target)

```bash
dbdrain \
  --source "postgres://user:pass@prod-host/mydb" \
  --from "users WHERE plan = 'enterprise' LIMIT 100" \
  --target "postgres://user:pass@localhost/staging" \
  --anonymize-pii \
  --depth 2 \
  --max-rows-per-table 500
```

### 4. Limit traversal depth and row explosion

```bash
# Only pull 2 levels of children, cap each table at 1000 rows
dbdrain \
  --source "postgres://user:pass@prod/mydb" \
  --from "tenants WHERE id = 42" \
  --depth 2 \
  --max-rows-per-table 1000 \
  --output tenant_42.sql
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
| `--depth` | int | `0` (unlimited) | Max downward FK traversal depth (parent traversal always unlimited) |
| `--max-rows-per-table` | int | `0` (unlimited) | Hard ceiling on rows pulled per child table |
| `--target` | string | — | Target DB connection string for direct streaming hydration |

---

## PII Masking Rules

When `--anonymize-pii` is enabled, columns are masked deterministically using **HMAC-SHA256** keyed by `--salt`:

| Column name pattern | Masked format |
|---------------------|---------------|
| `email`, `*_email` | `anon_<hash[:10]>@drain.local` |
| `phone`, `mobile`, `*phone*` | `+1555<hash[:7]>` |
| `first_name`, `name`, `*_name` | `User_<hash[:6]>` |
| `last_name` | `Anon_<hash[:6]>` |
| `password`, `passwd` | `$2a$12$e8rG.e9y1dYv9fD1v1.X7fakehash` |
| `token`, `secret`, `*token*` | `tok_<hash[:16]>` |

> [!NOTE]
> The same input value + salt always produces the same masked output, so foreign key references across tables remain consistent and UNIQUE constraints are never violated.

---

## Architecture

```
dbdrain --from "users WHERE id=1"
    │
    ├─ introspect/postgres.go
    │    └─ Queries information_schema + pg_constraint
    │       Discovers columns, PKs, composite FKs, deferrability
    │
    ├─ graph/graph.go
    │    └─ Builds directed adjacency-list FK graph
    │
    ├─ graph/tarjan.go
    │    └─ Tarjan's SCC → detects circular FK dependencies
    │       Emits SET CONSTRAINTS ALL DEFERRED when cycles found
    │
    ├─ drain/export.go
    │    ├─ REPEATABLE READ snapshot transaction on source
    │    ├─ Self-ref tables → recursive CTE (WITH RECURSIVE hierarchy AS ...)
    │    ├─ Upward BFS: mandatory parent traversal (unlimited depth)
    │    ├─ Downward BFS: child traversal (bounded by --depth, --max-rows-per-table)
    │    ├─ Visited-set deduplication prevents infinite loops
    │    └─ Mode A: emit SQL text  |  Mode B: stream to --target via pgx batched inserts
    │
    ├─ anonymize/mask.go
    │    └─ HMAC-SHA256 deterministic masking by column name heuristics
    │
    └─ ui/progress.go
         ├─ TTY detected → animated Lipgloss spinner + summary table (stderr)
         └─ Non-TTY / pipe → pure SQL only, no ANSI codes
```

### Circular FK Resolution

```
          orders ──────────────────► users
            │                          ▲
            └──► order_items           │
                      │                │
                      └──► products ───┘
                                │
                          (cycle detected by Tarjan SCC)
                          → SET CONSTRAINTS ALL DEFERRED;
```

### Self-Referencing Hierarchy

```sql
-- dbdrain generates this automatically:
WITH RECURSIVE hierarchy AS (
  SELECT * FROM categories WHERE id = 5          -- seed
  UNION
  SELECT t.* FROM categories t                   -- ancestors
    INNER JOIN hierarchy h ON t.id = h.parent_id
  UNION
  SELECT t.* FROM categories t                   -- descendants
    INNER JOIN hierarchy h ON t.parent_id = h.id
)
SELECT DISTINCT * FROM hierarchy;
```

---

## Development

```bash
# Run all tests
go test ./...

# Build for all platforms
make build-all   # or see dist/ build commands below

# Cross-compile
GOOS=linux   GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-linux-amd64   ./cmd/dbdrain/
GOOS=darwin  GOARCH=arm64  CGO_ENABLED=0 go build -o dist/dbdrain-darwin-arm64  ./cmd/dbdrain/
GOOS=darwin  GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-darwin-amd64  ./cmd/dbdrain/
GOOS=windows GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-windows-amd64.exe ./cmd/dbdrain/
```

---

## License

MIT © 2026 [x7ssss](https://github.com/x7ssss)
