# mcp-db-access

MCP server (Streamable HTTP) for SQL access to several databases. Each database gets its own connection pool.
Supported: PostgreSQL (asyncpg) and OceanBase in MySQL mode (aiomysql).

Pools are opened at startup; a database that is unreachable then is logged as a warning and does not stop
the server — its pool is opened on the first call to it (and retried on every call until it succeeds).

## Run

```sh
cp config.example.toml config.toml   # describe your databases
export MAIN_PG_PASSWORD=... OB_PASSWORD=...
uv run python server.py config.toml
```

Endpoint: `http://127.0.0.1:8000/mcp`. Connect from Claude Code:

```sh
claude mcp add --transport http db http://127.0.0.1:8000/mcp
```

## Tools

- `list_databases()` — configured databases.
- `execute_sql(database, sql, params?)` — one statement, autocommit, with the DB account's privileges.
  Placeholders: `$1, $2…` for postgres, `%s` for oceanbase. Results are capped at `max_rows` (default 200)
  (`truncated: true` when cut); every query is limited by `query_timeout` (default 120 s).
- Schema search — `list_tables`, `list_columns`, `list_indexes`, `list_views`, `list_sequences`.
  Filters `schema`, `table`, `pattern` are LIKE patterns (`order%`); without `schema` system schemas are skipped.
  For OceanBase a schema is a MySQL database; sequences need OceanBase >= 4.0 and support only `pattern`.
- `info_table(database, table, schema?)` — columns, indexes and constraints (PK, unique, FK, check) of one table.
  Names are exact, not patterns; without `schema` the current schema is used (OceanBase: the configured database).
  For OceanBase, CHECK constraints are listed without their expression.
- `explain_query(database, sql, params?, analyze?)` — execution plan with estimated cost and rows (`EXPLAIN`).
  `analyze: true` (Postgres only) runs the statement for actual time, rows and buffers (`EXPLAIN (ANALYZE, BUFFERS)`)
  in a transaction that is always rolled back: data changes are discarded, sequence increments are not.

Performance (Postgres: grant `pg_monitor` to see other users' sessions and queries):

- `top_queries(database, order_by?, pattern?, limit?)` — heaviest statements: calls, total/mean/max ms, rows,
  cache hit. `order_by`: `total_time` (default), `mean_time`, `calls`. Postgres needs `pg_stat_statements`
  (`shared_preload_libraries = 'pg_stat_statements'`, restart, `CREATE EXTENSION pg_stat_statements`);
  OceanBase reads `GV$OB_PLAN_CACHE_PLAN_STAT`.
- `active_sessions(database)` — non-idle sessions with their query and duration; Postgres adds wait event,
  transaction age and `blocked_by`.
- `table_stats(database, schema?, table?)` — table sizes, live/dead rows, seq/index scans, last vacuum/analyze;
  Postgres also index size and scan count (`idx_scan = 0` → unused). OceanBase: row estimate and data/index size.
- `db_settings(database, pattern?)` — settings changed from defaults, or all matching `pattern`
  (OceanBase: `GV$OB_PARAMETERS`; changed-only needs >= 4.2.2).
- `db_stats(database, pattern?)` — Postgres: size, connections, commits/rollbacks, cache hit %, temp files,
  deadlocks. OceanBase: `GV$SYSSTAT` counters (needs `enable_perf_event`), `pattern` filters by name.

## Query log

Every tool call is written to a SQLite file (`[server] query_log`, default `queries.sqlite` in the working
directory), table `tool_calls`: `ts` (UTC), `client`, `tool`, `database`, `sql`, `arguments` (JSON, including
`params`), `duration_ms`, `status` (`ok` / `error`), `error`. Failed calls are logged too: SQL errors, timeouts,
invalid arguments, unknown tools. The file is never pruned.

```sh
sqlite3 queries.sqlite "SELECT ts, tool, database, status, duration_ms, sql FROM tool_calls ORDER BY id DESC LIMIT 20"
```

## Authentication (optional)

With `[auth] enabled = true` every HTTP request must carry `Authorization: Bearer <token>`; otherwise the
server answers `401`. Users (`id`, `cert_sn` — certificate serial number) and their tokens live in a SQLite
file (`[auth] db`, default `auth.sqlite`); only a SHA-256 hash of each token is stored. Changes apply
immediately, without a restart. The authenticated user id is written to the query log (`user` column).

```sh
uv run python auth.py config.toml user-add alice 0A:1B:2C   # user id, certificate serial number
uv run python auth.py config.toml token-add alice           # prints the token once — save it
uv run python auth.py config.toml list                      # users with their token ids
uv run python auth.py config.toml token-del 3               # revoke a token
uv run python auth.py config.toml user-del alice            # delete a user and all their tokens

claude mcp add --transport http db http://127.0.0.1:8000/mcp --header "Authorization: Bearer <token>"
```

## Security

Authentication is off by default. Tokens travel in plain text over HTTP, so beyond localhost use TLS
(e.g. a reverse proxy). SQL runs with full privileges of the configured accounts.
Keep `host = "127.0.0.1"` (DNS-rebinding protection is on for localhost) or put the server behind
an authenticating proxy. Restrict what the server can do via the DB accounts' grants.
