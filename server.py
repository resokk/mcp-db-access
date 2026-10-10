"""MCP server (streamable HTTP) giving access to several databases, each with its own connection pool."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import tomllib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

import databases
from databases import BACKENDS, Database, DBConfig, Result, describe_table, search_catalog
from auth import BearerAuth, TokenStore, auth_db_path
from query_log import QueryLog


@dataclass
class Settings:
    host: str
    port: int
    max_rows: int
    query_timeout: float
    query_log: str  # SQLite file recording every tool call
    auth_db: str | None  # SQLite file with users and tokens; None: authentication is off
    databases: dict[str, DBConfig]


def load_settings(path: str) -> Settings:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    server = raw.get("server", {})
    dbs: dict[str, DBConfig] = {}
    for name, db in raw.get("databases", {}).items():
        db = dict(db)
        if db.get("type") not in BACKENDS:
            raise ValueError(f"database {name!r}: type must be one of {sorted(BACKENDS)}")
        # Passwords are never stored in the config file, only the name of the env variable holding it.
        env = db.pop("password_env", None)
        if env:
            if env not in os.environ:
                raise ValueError(f"database {name!r}: environment variable {env} is not set")
            db["password"] = os.environ[env]
        dbs[name] = DBConfig(name=name, **db)
    if not dbs:
        raise ValueError("no [databases.*] sections in config")
    return Settings(
        host=server.get("host", "127.0.0.1"),
        port=server.get("port", 8000),
        max_rows=server.get("max_rows", 200),
        query_timeout=server.get("query_timeout", 120),
        query_log=server.get("query_log", "queries.sqlite"),
        auth_db=auth_db_path(raw) if raw.get("auth", {}).get("enabled", False) else None,
        databases=dbs,
    )


log = logging.getLogger("db-access")
settings = load_settings(sys.argv[1] if len(sys.argv) > 1 else "config.toml")
query_log = QueryLog(settings.query_log)


@asynccontextmanager
async def lifespan(_: MCPServer) -> AsyncIterator[dict[str, Database]]:
    dbs: dict[str, Database] = {name: BACKENDS[cfg.type](cfg) for name, cfg in settings.databases.items()}
    try:
        # An unreachable database must not stop the server: its pool is opened again on first use.
        results = await asyncio.gather(*(db.open() for db in dbs.values()), return_exceptions=True)
        for name, result in zip(dbs, results):
            if isinstance(result, Exception):
                log.warning("database %r is unavailable, will retry on use: %s: %s", name, type(result).__name__, result)
        yield dbs
    finally:
        await asyncio.gather(*(db.close() for db in dbs.values()), return_exceptions=True)


mcp = MCPServer(
    "db-access",
    instructions=(
        "Access to several SQL databases. Call list_databases first; explore schema with list_tables, "
        "list_columns, list_indexes, list_views, list_sequences, info_table; run SQL with execute_sql; "
        "check a query's plan and cost with explain_query. Performance: top_queries, active_sessions, "
        "table_stats, db_settings, db_stats. "
        "Postgres placeholders are $1, $2...; OceanBase (MySQL mode) placeholders are %s."
    ),
    lifespan=lifespan,
    middleware=[query_log],
)


@mcp.tool()
def list_databases() -> list[dict[str, str]]:
    """List configured databases: name, type (postgres | oceanbase), database and description."""
    return [
        {"name": c.name, "type": c.type, "database": c.database, "description": c.description}
        for c in settings.databases.values()
    ]


@mcp.tool()
async def execute_sql(database: str, sql: str, ctx: Context, params: list[Any] | None = None) -> dict[str, Any]:
    """Execute one SQL statement in the given database and return its result.

    Statements run in autocommit mode with the privileges of the database account.
    Row-returning statements return at most the server's row limit; `truncated` shows whether rows were cut.
    Use placeholders with `params`: $1, $2... for postgres; %s for oceanbase.
    """
    return await _run(ctx, database, lambda db: db.execute(sql, params or [], settings.max_rows))


@mcp.tool()
async def explain_query(
    database: str, sql: str, ctx: Context, params: list[Any] | None = None, analyze: bool = False
) -> dict[str, Any]:
    """Show the execution plan of one statement: plan nodes with estimated cost and rows.

    analyze=True (postgres only) also runs the statement and reports actual time, rows, loops and buffers.
    It runs inside a transaction that is always rolled back, so data changes are discarded; effects outside
    transactions (sequence increments) remain, and the run takes as long as the query itself.
    Placeholders as in execute_sql.
    """
    return await _run(ctx, database, lambda db: db.explain(sql, params or [], analyze, settings.max_rows))


@mcp.tool()
async def top_queries(
    database: str,
    ctx: Context,
    order_by: Literal["total_time", "mean_time", "calls"] = "total_time",
    pattern: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Heaviest statements by cumulative statistics: calls, total/mean/max time (ms), rows, cache/disk reads.
    `pattern` is a LIKE pattern on the query text. Postgres needs the pg_stat_statements extension
    (shared_preload_libraries + CREATE EXTENSION) and shows this database only; oceanbase uses the plan cache."""
    limit = max(1, min(limit, settings.max_rows))
    return await _run(ctx, database, lambda db: databases.top_queries(db, order_by, pattern, limit))


@mcp.tool()
async def active_sessions(database: str, ctx: Context) -> dict[str, Any]:
    """Sessions doing something now (idle ones skipped): user, state, current query and how long it runs.
    Postgres also shows the wait event, transaction age and `blocked_by` (pids holding the locks it waits for)."""
    return await _catalog(ctx, database, "active_sessions")


@mcp.tool()
async def table_stats(
    database: str, ctx: Context, schema: str | None = None, table: str | None = None
) -> dict[str, Any]:
    """Table sizes and usage, largest first. Postgres: live/dead rows, rows modified since ANALYZE, seq vs index
    scans, last (auto)vacuum/analyze, plus per-index size and scan count (idx_scan = 0: unused index).
    Oceanbase: row estimate and data/index size. `schema` and `table` are LIKE patterns."""
    return await _run(ctx, database, lambda db: databases.table_stats(db, schema, table, settings.max_rows))


@mcp.tool()
async def db_settings(database: str, ctx: Context, pattern: str | None = None) -> dict[str, Any]:
    """Server settings with current and default values. Without `pattern` only settings changed from their
    defaults are shown; `pattern` is a LIKE pattern on the name (e.g. '%mem%'). Oceanbase shows cluster
    parameters; listing only changed ones needs OceanBase >= 4.2.2."""
    kind = "settings" if pattern else "changed_settings"
    return await _catalog(ctx, database, kind, name=pattern)


@mcp.tool()
async def db_stats(database: str, ctx: Context, pattern: str | None = None) -> dict[str, Any]:
    """Database-wide counters. Postgres: size, connections, commits/rollbacks, cache hit %, temp files,
    deadlocks. Oceanbase: tenant counters from GV$SYSSTAT (needs enable_perf_event); `pattern` (oceanbase only)
    is a LIKE pattern on the counter name."""
    return await _catalog(ctx, database, "db_stats", name=pattern)


@mcp.tool()
async def list_tables(
    database: str, ctx: Context, schema: str | None = None, pattern: str | None = None
) -> dict[str, Any]:
    """Find tables. `schema` and `pattern` (table name) are LIKE patterns, e.g. 'order%'.
    Without `schema`, system schemas are skipped. For oceanbase a schema is a MySQL database."""
    return await _catalog(ctx, database, "tables", schema=schema, name=pattern)


@mcp.tool()
async def list_columns(
    database: str, ctx: Context, schema: str | None = None, table: str | None = None, pattern: str | None = None
) -> dict[str, Any]:
    """Find columns (of tables and views) with type, nullability, default and comment.
    `schema`, `table` and `pattern` (column name) are LIKE patterns."""
    return await _catalog(ctx, database, "columns", schema=schema, table=table, name=pattern)


@mcp.tool()
async def list_indexes(
    database: str, ctx: Context, schema: str | None = None, table: str | None = None, pattern: str | None = None
) -> dict[str, Any]:
    """Find indexes with uniqueness and definition/columns.
    `schema`, `table` and `pattern` (index name) are LIKE patterns."""
    return await _catalog(ctx, database, "indexes", schema=schema, table=table, name=pattern)


@mcp.tool()
async def list_views(
    database: str, ctx: Context, schema: str | None = None, pattern: str | None = None
) -> dict[str, Any]:
    """Find views (and materialized views in postgres) with their definitions.
    `schema` and `pattern` (view name) are LIKE patterns."""
    return await _catalog(ctx, database, "views", schema=schema, name=pattern)


@mcp.tool()
async def list_sequences(
    database: str, ctx: Context, schema: str | None = None, pattern: str | None = None
) -> dict[str, Any]:
    """Find sequences with their parameters. `schema` and `pattern` (sequence name) are LIKE patterns.
    For oceanbase (>= 4.0) only `pattern` is supported."""
    return await _catalog(ctx, database, "sequences", schema=schema, name=pattern)


@mcp.tool()
async def info_table(database: str, table: str, ctx: Context, schema: str | None = None) -> dict[str, Any]:
    """Describe one table: its columns, indexes and constraints (PK, unique, foreign keys, checks).
    `schema` and `table` are exact names (not patterns); without `schema` the connection's current schema
    is used (postgres: current_schema(), oceanbase: the configured database)."""
    return await _run(ctx, database, lambda db: describe_table(db, schema, table, settings.max_rows))


async def _catalog(ctx: Context, database: str, kind: str, **filters: str | None) -> dict[str, Any]:
    return await _run(ctx, database, lambda db: search_catalog(db, kind, filters, settings.max_rows))


async def _run(
    ctx: Context, database: str, op: Callable[[Database], Awaitable[Result | dict[str, Result]]]
) -> dict[str, Any]:
    dbs: dict[str, Database] = ctx.request_context.lifespan_context
    db = dbs.get(database)
    if db is None:
        raise ToolError(f"unknown database {database!r}; available: {sorted(dbs)}")
    try:
        result = await asyncio.wait_for(op(db), settings.query_timeout)
    except TimeoutError:
        raise ToolError(f"query timed out after {settings.query_timeout}s") from None
    except Exception as e:
        raise ToolError(f"{type(e).__name__}: {e}") from e
    if isinstance(result, Result):
        return result.to_dict()
    return {key: r.to_dict() for key, r in result.items()}


if __name__ == "__main__":
    import uvicorn

    app = mcp.streamable_http_app(host=settings.host)
    store = TokenStore(settings.auth_db) if settings.auth_db else None
    if store:
        if not store.users():
            log.warning("authentication is on but %s has no users: every request will be rejected", settings.auth_db)
        app = BearerAuth(app, store)
    try:
        uvicorn.run(app, host=settings.host, port=settings.port)
    finally:
        query_log.close()
        if store:
            store.close()
