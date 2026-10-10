"""Database backends: one connection pool per configured database."""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import decimal
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

import aiomysql
import asyncpg


@dataclass
class DBConfig:
    name: str
    type: str  # "postgres" | "oceanbase"
    host: str
    port: int
    user: str
    password: str
    database: str
    description: str = ""
    pool_min: int = 1
    pool_max: int = 10


@dataclass
class Result:
    columns: list[str] | None  # None for statements that return no rows
    rows: list[list[Any]]
    truncated: bool
    status: str  # command tag (postgres) or affected row count (oceanbase)

    def to_dict(self) -> dict[str, Any]:
        if self.columns is None:
            return {"status": self.status}
        return {
            "columns": self.columns,
            "rows": [[_jsonable(v) for v in row] for row in self.rows],
            "row_count": len(self.rows),
            "truncated": self.truncated,
        }


@dataclass(frozen=True)
class CatalogQuery:
    """Catalog lookup: `filters` maps a filter name (schema/table/name) to the SQL expression it matches."""

    select: str
    filters: dict[str, str]
    where: str = "TRUE"
    tail: str = ""  # GROUP BY / ORDER BY


class Database(Protocol):
    config: DBConfig
    CATALOG: dict[str, CatalogQuery]
    LIKE: str
    SYSTEM_SCHEMAS: str  # condition on {col} excluding system schemas when no schema filter is given
    CURRENT_SCHEMA: str  # SQL expression for the connection's current schema

    async def open(self) -> None: ...  # idempotent: opens the pool unless it is already open
    async def close(self) -> None: ...
    async def execute(self, sql: str, params: list[Any], max_rows: int) -> Result: ...
    async def explain(self, sql: str, params: list[Any], analyze: bool, max_rows: int) -> Result: ...
    def placeholder(self, n: int) -> str: ...


async def search_catalog(
    db: Database, kind: str, filters: dict[str, str | None], max_rows: int, exact: bool = False
) -> Result:
    """Run a catalog lookup. Filters are LIKE patterns (% and _ wildcards), or exact names when `exact`.

    Without a schema filter, a pattern search skips system schemas and an exact one uses the current schema.
    """
    sql, params = _catalog_sql(db, kind, filters, exact)
    return await db.execute(sql, params, max_rows)


def _catalog_sql(db: Database, kind: str, filters: dict[str, str | None], exact: bool) -> tuple[str, list[Any]]:
    if kind not in db.CATALOG:
        raise ValueError(f"{kind} is not supported for {db.config.type}")
    q = db.CATALOG[kind]
    conds, params = [q.where], []
    for key, value in filters.items():
        if value is None:
            continue
        if key not in q.filters:
            raise ValueError(f"filter {key!r} is not supported for {kind} in {db.config.type}")
        params.append(value)
        conds.append(f"{q.filters[key]} {'=' if exact else db.LIKE} {db.placeholder(len(params))}")
    if filters.get("schema") is None and "schema" in q.filters:
        col = q.filters["schema"]
        conds.append(f"{col} = {db.CURRENT_SCHEMA}" if exact else db.SYSTEM_SCHEMAS.format(col=col))
    return f"{q.select} WHERE {' AND '.join(conds)} {q.tail}", params


async def describe_table(db: Database, schema: str | None, table: str, max_rows: int) -> dict[str, Result]:
    """Columns, indexes and constraints of one table, matched by exact name."""
    filters = {"schema": schema, "table": table}
    info = {kind: await search_catalog(db, kind, filters, max_rows, exact=True)
            for kind in ("columns", "indexes", "constraints")}
    if not info["columns"].rows:
        where = f"schema {schema!r}" if schema else "the current schema"
        raise ValueError(f"table {table!r} not found in {where}")
    return info


# Sort keys of top_queries: column aliases shared by both backends' "top_queries" catalog queries.
TOP_QUERIES_ORDER = {"total_time": "total_ms", "mean_time": "mean_ms", "calls": "calls"}


async def top_queries(db: Database, order_by: str, pattern: str | None, limit: int) -> Result:
    """Heaviest statements by cumulative statistics; `pattern` is a LIKE pattern on the query text."""
    sql, params = _catalog_sql(db, "top_queries", {"query": pattern}, exact=False)
    return await db.execute(f"SELECT * FROM ({sql}) q ORDER BY {TOP_QUERIES_ORDER[order_by]} DESC", params, limit)


async def table_stats(db: Database, schema: str | None, table: str | None, max_rows: int) -> dict[str, Result]:
    """Size, row churn, maintenance and scan statistics of tables (and index usage where the backend has it)."""
    filters = {"schema": schema, "table": table}
    return {kind.removesuffix("_stats"): await search_catalog(db, kind, filters, max_rows)
            for kind in ("table_stats", "index_stats") if kind in db.CATALOG}


class PostgresDatabase:
    LIKE = "ILIKE"
    SYSTEM_SCHEMAS = "{col} !~ '^pg_' AND {col} <> 'information_schema'"
    CURRENT_SCHEMA = "current_schema()"
    CATALOG = {
        "tables": CatalogQuery(
            """SELECT n.nspname AS schema_name, c.relname AS table_name,
                      CASE c.relkind WHEN 'p' THEN 'partitioned table' ELSE 'table' END AS kind,
                      c.reltuples::bigint AS row_estimate, obj_description(c.oid, 'pg_class') AS comment
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace""",
            {"schema": "n.nspname", "name": "c.relname"},
            where="c.relkind IN ('r', 'p')",
            tail="ORDER BY 1, 2",
        ),
        "columns": CatalogQuery(
            """SELECT n.nspname AS schema_name, c.relname AS table_name, a.attname AS column_name,
                      format_type(a.atttypid, a.atttypmod) AS data_type, NOT a.attnotnull AS nullable,
                      pg_get_expr(d.adbin, d.adrelid) AS default_value, col_description(c.oid, a.attnum) AS comment
               FROM pg_attribute a
               JOIN pg_class c ON c.oid = a.attrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum""",
            {"schema": "n.nspname", "table": "c.relname", "name": "a.attname"},
            where="a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p', 'v', 'm', 'f')",
            tail="ORDER BY 1, 2, a.attnum",
        ),
        "indexes": CatalogQuery(
            """SELECT n.nspname AS schema_name, t.relname AS table_name, i.relname AS index_name,
                      ix.indisunique AS is_unique, ix.indisprimary AS is_primary,
                      pg_get_indexdef(ix.indexrelid) AS definition
               FROM pg_index ix
               JOIN pg_class i ON i.oid = ix.indexrelid
               JOIN pg_class t ON t.oid = ix.indrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace""",
            {"schema": "n.nspname", "table": "t.relname", "name": "i.relname"},
            tail="ORDER BY 1, 2, 3",
        ),
        "constraints": CatalogQuery(
            """SELECT n.nspname AS schema_name, t.relname AS table_name, con.conname AS constraint_name,
                      CASE con.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'f' THEN 'FOREIGN KEY'
                           WHEN 'c' THEN 'CHECK' WHEN 'x' THEN 'EXCLUDE' ELSE con.contype::text END AS constraint_type,
                      pg_get_constraintdef(con.oid) AS definition
               FROM pg_constraint con
               JOIN pg_class t ON t.oid = con.conrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace""",
            {"schema": "n.nspname", "table": "t.relname", "name": "con.conname"},
            where="con.contype <> 'n'",  # NOT NULL (postgres 18+) is already in columns.nullable
            tail="ORDER BY 1, 2, 4, 3",
        ),
        "views": CatalogQuery(
            """SELECT n.nspname AS schema_name, c.relname AS view_name,
                      CASE c.relkind WHEN 'm' THEN 'materialized view' ELSE 'view' END AS kind,
                      pg_get_viewdef(c.oid) AS definition, obj_description(c.oid, 'pg_class') AS comment
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace""",
            {"schema": "n.nspname", "name": "c.relname"},
            where="c.relkind IN ('v', 'm')",
            tail="ORDER BY 1, 2",
        ),
        "sequences": CatalogQuery(
            """SELECT schemaname AS schema_name, sequencename AS sequence_name, data_type::text AS data_type,
                      start_value, min_value, max_value, increment_by, cycle, cache_size, last_value
               FROM pg_sequences""",
            {"schema": "schemaname", "name": "sequencename"},
            tail="ORDER BY 1, 2",
        ),
        "top_queries": CatalogQuery(  # needs the pg_stat_statements extension; this database only
            """SELECT s.queryid::text AS query_id, r.rolname AS user_name, s.calls,
                      round(s.total_exec_time::numeric, 2) AS total_ms, round(s.mean_exec_time::numeric, 2) AS mean_ms,
                      round(s.max_exec_time::numeric, 2) AS max_ms, s.rows,
                      round(100.0 * s.shared_blks_hit / nullif(s.shared_blks_hit + s.shared_blks_read, 0), 1)
                          AS cache_hit_pct,
                      s.temp_blks_written, s.query
               FROM pg_stat_statements s LEFT JOIN pg_roles r ON r.oid = s.userid""",
            {"query": "s.query"},
            where="s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())",
        ),
        "active_sessions": CatalogQuery(
            """SELECT a.pid, a.usename AS user_name, a.datname AS database_name, a.application_name,
                      a.client_addr::text AS client_addr, a.backend_type, a.state, a.wait_event_type, a.wait_event,
                      pg_blocking_pids(a.pid) AS blocked_by, now() - a.xact_start AS transaction_age,
                      now() - a.query_start AS query_age, a.query
               FROM pg_stat_activity a""",
            {},
            where="a.pid <> pg_backend_pid() AND a.state <> 'idle'",
            tail="ORDER BY a.query_start NULLS LAST",
        ),
        "table_stats": CatalogQuery(
            """SELECT s.schemaname AS schema_name, s.relname AS table_name,
                      pg_size_pretty(pg_total_relation_size(s.relid)) AS total_size,
                      pg_size_pretty(pg_relation_size(s.relid)) AS table_size,
                      pg_size_pretty(pg_indexes_size(s.relid)) AS indexes_size,
                      s.n_live_tup AS live_rows, s.n_dead_tup AS dead_rows, s.n_mod_since_analyze AS modified_since_analyze,
                      s.seq_scan, s.seq_tup_read, s.idx_scan, s.n_tup_ins AS inserted, s.n_tup_upd AS updated,
                      s.n_tup_del AS deleted, s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze
               FROM pg_stat_user_tables s""",
            {"schema": "s.schemaname", "table": "s.relname"},
            tail="ORDER BY pg_total_relation_size(s.relid) DESC",
        ),
        "index_stats": CatalogQuery(
            """SELECT s.schemaname AS schema_name, s.relname AS table_name, s.indexrelname AS index_name,
                      pg_size_pretty(pg_relation_size(s.indexrelid)) AS size, s.idx_scan, s.idx_tup_read,
                      s.idx_tup_fetch, i.indisunique AS is_unique, i.indisprimary AS is_primary
               FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid""",
            {"schema": "s.schemaname", "table": "s.relname"},
            tail="ORDER BY pg_relation_size(s.indexrelid) DESC",
        ),
        "settings": CatalogQuery(
            "SELECT name, setting, unit, source, boot_val AS default_value, short_desc AS description FROM pg_settings",
            {"name": "name"},
            tail="ORDER BY name",
        ),
        "changed_settings": CatalogQuery(
            "SELECT name, setting, unit, source, boot_val AS default_value, short_desc AS description FROM pg_settings",
            {},
            where="source NOT IN ('default', 'override')",
            tail="ORDER BY name",
        ),
        "db_stats": CatalogQuery(
            """SELECT datname AS database_name, pg_size_pretty(pg_database_size(datname)) AS database_size,
                      numbackends AS connections, xact_commit AS commits, xact_rollback AS rollbacks,
                      round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 2) AS cache_hit_pct,
                      tup_returned AS rows_returned, tup_fetched AS rows_fetched, tup_inserted AS rows_inserted,
                      tup_updated AS rows_updated, tup_deleted AS rows_deleted, temp_files,
                      pg_size_pretty(temp_bytes) AS temp_size, deadlocks, conflicts, stats_reset
               FROM pg_stat_database""",
            {},
            where="datname = current_database()",
        ),
    }

    def __init__(self, config: DBConfig):
        self.config = config
        self._pool: asyncpg.Pool | None = None
        self._open_lock = asyncio.Lock()

    async def open(self) -> None:
        async with self._open_lock:
            if self._pool is None:
                c = self.config
                self._pool = await asyncpg.create_pool(
                    host=c.host, port=c.port, user=c.user, password=c.password, database=c.database,
                    min_size=c.pool_min, max_size=c.pool_max,
                )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    def placeholder(self, n: int) -> str:
        return f"${n}"

    async def execute(self, sql: str, params: list[Any], max_rows: int) -> Result:
        await self.open()  # a database unreachable at startup is retried here
        async with self._pool.acquire() as conn:
            stmt = await conn.prepare(sql)
            attrs = stmt.get_attributes()
            if not attrs:
                await stmt.fetch(*params)
                return Result(None, [], False, stmt.get_statusmsg())
            # A cursor fetches only max_rows + 1 rows from the server instead of the whole result.
            async with conn.transaction():
                cur = await stmt.cursor(*params)
                rows = await cur.fetch(max_rows + 1)
            return Result(
                [a.name for a in attrs], [list(r) for r in rows[:max_rows]],
                len(rows) > max_rows, stmt.get_statusmsg(),
            )

    async def explain(self, sql: str, params: list[Any], analyze: bool, max_rows: int) -> Result:
        await self.open()
        options = "(ANALYZE, BUFFERS) " if analyze else ""
        async with self._pool.acquire() as conn:
            # ANALYZE really runs the statement: do it in a transaction that is always rolled back.
            # fetch() uses the extended protocol, so `sql` can't smuggle in a second statement (e.g. COMMIT).
            try:
                async with conn.transaction():
                    rows = await conn.fetch(f"EXPLAIN {options}{sql}", *params)
                    raise _Rollback
            except _Rollback:
                pass
        return Result(["plan"], [[r[0]] for r in rows[:max_rows]], len(rows) > max_rows, "EXPLAIN")


class _Rollback(Exception):
    pass


class OceanBaseDatabase:
    """OceanBase in MySQL mode (MySQL wire protocol). A "schema" here is a MySQL database."""

    LIKE = "LIKE"  # case-insensitive under the default collations
    SYSTEM_SCHEMAS = "{col} NOT IN ('information_schema', 'mysql', 'oceanbase', 'performance_schema', 'sys')"
    CURRENT_SCHEMA = "DATABASE()"
    CATALOG = {
        "tables": CatalogQuery(
            """SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, TABLE_TYPE AS kind,
                      TABLE_ROWS AS row_estimate, TABLE_COMMENT AS comment
               FROM information_schema.TABLES""",
            {"schema": "TABLE_SCHEMA", "name": "TABLE_NAME"},
            where="TABLE_TYPE = 'BASE TABLE'",
            tail="ORDER BY 1, 2",
        ),
        "columns": CatalogQuery(
            """SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COLUMN_NAME AS column_name,
                      COLUMN_TYPE AS data_type, IS_NULLABLE = 'YES' AS nullable, COLUMN_DEFAULT AS default_value,
                      COLUMN_KEY AS column_key, EXTRA AS extra, COLUMN_COMMENT AS comment
               FROM information_schema.COLUMNS""",
            {"schema": "TABLE_SCHEMA", "table": "TABLE_NAME", "name": "COLUMN_NAME"},
            tail="ORDER BY 1, 2, ORDINAL_POSITION",
        ),
        "indexes": CatalogQuery(
            """SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, INDEX_NAME AS index_name,
                      MIN(NON_UNIQUE) = 0 AS is_unique, INDEX_NAME = 'PRIMARY' AS is_primary,
                      GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_columns, MIN(INDEX_TYPE) AS index_type
               FROM information_schema.STATISTICS""",
            {"schema": "TABLE_SCHEMA", "table": "TABLE_NAME", "name": "INDEX_NAME"},
            tail="GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME ORDER BY 1, 2, 3",
        ),
        # CHECK constraints come without their expression: KEY_COLUMN_USAGE covers only key constraints.
        "constraints": CatalogQuery(
            """SELECT tc.TABLE_SCHEMA AS schema_name, tc.TABLE_NAME AS table_name,
                      tc.CONSTRAINT_NAME AS constraint_name, tc.CONSTRAINT_TYPE AS constraint_type,
                      GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION) AS constraint_columns,
                      MIN(k.REFERENCED_TABLE_SCHEMA) AS referenced_schema,
                      MIN(k.REFERENCED_TABLE_NAME) AS referenced_table,
                      GROUP_CONCAT(k.REFERENCED_COLUMN_NAME ORDER BY k.ORDINAL_POSITION) AS referenced_columns
               FROM information_schema.TABLE_CONSTRAINTS tc
               LEFT JOIN information_schema.KEY_COLUMN_USAGE k
                 ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND k.TABLE_NAME = tc.TABLE_NAME
                AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME""",
            {"schema": "tc.TABLE_SCHEMA", "table": "tc.TABLE_NAME", "name": "tc.CONSTRAINT_NAME"},
            tail="GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE ORDER BY 1, 2, 4, 3",
        ),
        "views": CatalogQuery(
            """SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS view_name, VIEW_DEFINITION AS definition
               FROM information_schema.VIEWS""",
            {"schema": "TABLE_SCHEMA", "name": "TABLE_NAME"},
            tail="ORDER BY 1, 2",
        ),
        # OceanBase >= 4.0; the view identifies the database only by DATABASE_ID, so no schema filter.
        "sequences": CatalogQuery(
            "SELECT * FROM oceanbase.DBA_OB_SEQUENCE_OBJECTS",
            {"name": "SEQUENCE_NAME"},
            tail="ORDER BY SEQUENCE_NAME",
        ),
        # Cumulative per plan, summed over OBServer nodes. Columns not verified on a live cluster.
        "top_queries": CatalogQuery(
            """SELECT SQL_ID AS query_id, SUM(EXECUTIONS) AS calls, ROUND(SUM(ELAPSED_TIME) / 1000, 2) AS total_ms,
                      ROUND(SUM(ELAPSED_TIME) / NULLIF(SUM(EXECUTIONS), 0) / 1000, 2) AS mean_ms,
                      ROUND(MAX(SLOWEST_EXE_USEC) / 1000, 2) AS max_ms, SUM(ROWS_PROCESSED) AS `rows`,
                      SUM(DISK_READS) AS disk_reads, SUM(BUFFER_GETS) AS buffer_gets, MIN(QUERY_SQL) AS query
               FROM oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT""",
            {"query": "QUERY_SQL"},
            tail="GROUP BY SQL_ID",
        ),
        "active_sessions": CatalogQuery(
            """SELECT ID AS session_id, USER AS user_name, HOST AS client, DB AS database_name, COMMAND AS command,
                      TIME AS seconds, STATE AS state, INFO AS query
               FROM information_schema.PROCESSLIST""",
            {},
            where="COMMAND <> 'Sleep' AND ID <> CONNECTION_ID()",
            tail="ORDER BY TIME DESC",
        ),
        "table_stats": CatalogQuery(
            """SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, TABLE_ROWS AS row_estimate,
                      DATA_LENGTH AS data_bytes, INDEX_LENGTH AS index_bytes, CREATE_TIME AS created, UPDATE_TIME AS updated
               FROM information_schema.TABLES""",
            {"schema": "TABLE_SCHEMA", "table": "TABLE_NAME"},
            where="TABLE_TYPE = 'BASE TABLE'",
            tail="ORDER BY DATA_LENGTH + INDEX_LENGTH DESC",
        ),
        # Cluster parameters (OceanBase >= 4.0; DEFAULT_VALUE/ISDEFAULT need >= 4.2.2), deduplicated over nodes.
        "settings": CatalogQuery(
            "SELECT DISTINCT NAME AS name, VALUE AS value, SCOPE AS scope, INFO AS description FROM oceanbase.GV$OB_PARAMETERS",
            {"name": "NAME"},
            tail="ORDER BY name",
        ),
        "changed_settings": CatalogQuery(
            """SELECT DISTINCT NAME AS name, VALUE AS value, DEFAULT_VALUE AS default_value, SCOPE AS scope,
                      INFO AS description
               FROM oceanbase.GV$OB_PARAMETERS""",
            {},
            where="ISDEFAULT = 'NO'",
            tail="ORDER BY name",
        ),
        # Tenant counters summed over nodes; needs enable_perf_event = True.
        "db_stats": CatalogQuery(
            "SELECT NAME AS name, CLASS AS class, SUM(VALUE) AS value FROM oceanbase.GV$SYSSTAT",
            {"name": "NAME"},
            tail="GROUP BY NAME, CLASS ORDER BY NAME",
        ),
    }

    def __init__(self, config: DBConfig):
        self.config = config
        self._pool: aiomysql.Pool | None = None
        self._open_lock = asyncio.Lock()

    async def open(self) -> None:
        async with self._open_lock:
            if self._pool is None:
                c = self.config
                self._pool = await aiomysql.create_pool(
                    host=c.host, port=c.port, user=c.user, password=c.password, db=c.database,
                    minsize=c.pool_min, maxsize=c.pool_max, autocommit=True, charset="utf8mb4",
                )

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    def placeholder(self, n: int) -> str:
        return "%s"

    async def explain(self, sql: str, params: list[Any], analyze: bool, max_rows: int) -> Result:
        if analyze:
            raise ValueError("analyze is not supported for oceanbase; use the estimated plan")
        return await self.execute(f"EXPLAIN {sql}", params, max_rows)

    async def execute(self, sql: str, params: list[Any], max_rows: int) -> Result:
        await self.open()  # a database unreachable at startup is retried here
        async with self._pool.acquire() as conn:
            try:
                # Unbuffered cursor: rows are streamed, so a huge result is not loaded into memory.
                cur = await conn.cursor(aiomysql.SSCursor)
                # params=None disables %-formatting, so literal '%' in SQL works without params.
                await cur.execute(sql, params or None)
                if cur.description is None:
                    await cur.close()
                    return Result(None, [], False, f"{cur.rowcount} rows affected")
                columns = [d[0] for d in cur.description]
                rows = await cur.fetchmany(max_rows + 1)
                if len(rows) > max_rows:
                    # Draining the rest of an unbuffered result could take forever; drop the connection.
                    conn.close()
                    return Result(columns, [list(r) for r in rows[:max_rows]], True, "")
                await cur.close()
                return Result(columns, [list(r) for r in rows], False, "")
            except BaseException:
                # Failed or interrupted (e.g. timeout) mid-protocol: connection state is unknown, don't reuse it.
                conn.close()
                raise


BACKENDS: dict[str, type] = {"postgres": PostgresDatabase, "oceanbase": OceanBaseDatabase}


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, decimal.Decimal):
        return str(v)  # keep exact precision
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"base64": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (uuid.UUID, dt.timedelta)):
        return str(v)
    return str(v)
