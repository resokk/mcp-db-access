"""MCP server (streamable HTTP) giving access to several databases, each with its own connection pool."""

from __future__ import annotations

import asyncio
import os
import sys
import tomllib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from databases import BACKENDS, Database, DBConfig, Result, describe_table, search_catalog


@dataclass
class Settings:
    host: str
    port: int
    max_rows: int
    query_timeout: float
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
        databases=dbs,
    )


settings = load_settings(sys.argv[1] if len(sys.argv) > 1 else "config.toml")


@asynccontextmanager
async def lifespan(_: MCPServer) -> AsyncIterator[dict[str, Database]]:
    dbs: dict[str, Database] = {}
    try:
        for name, cfg in settings.databases.items():
            db = BACKENDS[cfg.type](cfg)
            await db.open()
            dbs[name] = db
        yield dbs
    finally:
        await asyncio.gather(*(db.close() for db in dbs.values()), return_exceptions=True)


mcp = MCPServer(
    "db-access",
    instructions=(
        "Access to several SQL databases. Call list_databases first; explore schema with list_tables, "
        "list_columns, list_indexes, list_views, list_sequences; run SQL with execute_sql. "
        "Postgres placeholders are $1, $2...; OceanBase (MySQL mode) placeholders are %s."
    ),
    lifespan=lifespan,
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
    mcp.run("streamable-http", host=settings.host, port=settings.port)
