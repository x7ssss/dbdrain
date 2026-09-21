<div align="center">

# 🚰 dbdrain

**Zero-downtime PostgreSQL database subsetting, polymorphic FK resolution & deterministic PII masking — in a single static binary.**

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

**dbdrain** fills this void: a single Go binary, no runtime dependencies, no Docker, no YAML for basic usage.

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

### 3. Direct streaming hydration with integrity check

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

> [!NOTE]
> If no config rule matches a column, dbdrain falls back to automatic PII heuristics based on column name patterns (`email`, `*_token`, `password`, etc.).

### Wildcard Matching Rules

| Pattern | Matches |
|---------|---------|
| `*` | Everything |
| `*_token` | `access_token`, `refresh_token`, `api_token` |
| `user_*` | `user_id`, `user_email`, `user_name` |
| `*email*` | `email`, `user_email`, `email_address` |
| `exact` | Only `exact` |

**Priority** (highest wins): exact table + exact column > exact table + glob column > glob table + exact column > all wildcards.

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
    ├─ internal/graph/graph.go
    │    ├─ Builds directed adjacency-list FK graph
    │    └─ Registers virtual FK edges from config
    │
    ├─ internal/graph/tarjan.go
    │    └─ Tarjan's SCC → detects circular FK dependencies
    │       Emits SET CONSTRAINTS ALL DEFERRED when cycles found
    │
    ├─ internal/drain/export.go
    │    ├─ REPEATABLE READ snapshot transaction on source
    │    ├─ Self-ref tables → recursive CTE (WITH RECURSIVE hierarchy AS ...)
    │    ├─ Upward BFS: mandatory parent traversal (unlimited depth)
    │    ├─ Downward BFS: child traversal (bounded by --depth, --max-rows-per-table)
    │    ├─ Polymorphic BFS: virtual FK parent resolution via discriminator column
    │    ├─ Visited-set deduplication prevents infinite loops
    │    ├─ Config-rule masking (priority) → auto-heuristic fallback
    │    └─ Mode A: emit SQL text  |  Mode B: stream to --target via pgx batched inserts
    │
    ├─ internal/anonymize/mask.go
    │    └─ HMAC-SHA256 masking: fake_email, redact, uuid_remap, hmac_phone, hmac_name, hmac_token
    │
    ├─ internal/verify/verify.go
    │    └─ LEFT JOIN orphan queries on target DB; exits 1 on violations
    │
    └─ internal/ui/progress.go
         ├─ TTY detected → animated Lipgloss spinner + summary table (stderr)
         ├─ Non-TTY / pipe → pure SQL only, no ANSI codes
         └─ Integrity section: ✔ pass / ✘ violation list
```

### Polymorphic Virtual FK Resolution

```yaml
# dbdrain.yaml
virtual_foreign_keys:
  - child_table: comments
    child_column: commentable_id
    parent_table_column: commentable_type
    mappings:
      post: posts.id
      video: videos.id
```

During BFS, when rows are fetched from `comments`, dbdrain captures each `(commentable_type, commentable_id)` pair. It then groups by discriminator value and fetches the corresponding parent rows:

```
comments rows:
  {commentable_type: "post",  commentable_id: 5}  → fetch posts WHERE id IN ('5')
  {commentable_type: "video", commentable_id: 9}  → fetch videos WHERE id IN ('9')
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
# Run all tests (62 tests across 6 packages)
go test ./...

# Cross-compile release binaries
GOOS=linux   GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-linux-amd64   ./cmd/dbdrain/
GOOS=darwin  GOARCH=arm64  CGO_ENABLED=0 go build -o dist/dbdrain-darwin-arm64  ./cmd/dbdrain/
GOOS=darwin  GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-darwin-amd64  ./cmd/dbdrain/
GOOS=windows GOARCH=amd64  CGO_ENABLED=0 go build -o dist/dbdrain-windows-amd64.exe ./cmd/dbdrain/
```

---

## Changelog

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
