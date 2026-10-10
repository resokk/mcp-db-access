# mcp-db-access

MCP server (Streamable HTTP) for SQL access to several databases. Each database gets its own connection pool.
Supported: PostgreSQL (asyncpg) and OceanBase in MySQL mode (aiomysql).

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

## Security

There is no HTTP auth, and SQL runs with full privileges of the configured accounts.
Keep `host = "127.0.0.1"` (DNS-rebinding protection is on for localhost) or put the server behind
an authenticating proxy. Restrict what the server can do via the DB accounts' grants.
