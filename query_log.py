"""Audit log: every MCP tool call is recorded in a SQLite database."""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
import time
from typing import Any

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext

log = logging.getLogger("db-access")

SCHEMA = """
CREATE TABLE IF NOT EXISTS tool_calls (
    id          INTEGER PRIMARY KEY,
    ts          TEXT NOT NULL,     -- UTC, ISO 8601, when the call arrived
    client      TEXT,              -- client address
    user        TEXT,              -- authenticated user id (when authentication is on)
    token_id    INTEGER,           -- id of the token used (when authentication is on)
    tool        TEXT,
    database    TEXT,
    sql         TEXT,
    arguments   TEXT,              -- all tool arguments as JSON
    duration_ms REAL NOT NULL,
    status      TEXT NOT NULL,     -- 'ok' | 'error'
    error       TEXT
);
CREATE INDEX IF NOT EXISTS tool_calls_ts ON tool_calls (ts);
"""


class QueryLog:
    """Server middleware writing one row per `tools/call` request, successful or not.

    Writes are synchronous: in WAL mode with synchronous=NORMAL an insert does not fsync, so it takes
    well under a millisecond. A failed write is logged and never fails the tool call itself.
    """

    def __init__(self, path: str):
        self._db = sqlite3.connect(path, isolation_level=None)  # autocommit: each insert is its own transaction
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = NORMAL")
        self._db.executescript(SCHEMA)
        columns = {row[1] for row in self._db.execute("PRAGMA table_info(tool_calls)")}
        for name, ddl in (("user", "TEXT"), ("token_id", "INTEGER")):  # log files created before these existed
            if name not in columns:
                self._db.execute(f"ALTER TABLE tool_calls ADD COLUMN {name} {ddl}")

    def close(self) -> None:
        self._db.close()

    async def __call__(self, ctx: ServerRequestContext[Any, Any], call_next: CallNext) -> HandlerResult:
        if ctx.method != "tools/call":
            return await call_next(ctx)
        ts = dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds")
        start = time.perf_counter()
        status, error = "ok", None
        try:
            result = await call_next(ctx)
            error = _tool_error(result)
            if error is not None:
                status = "error"
            return result
        except BaseException as e:  # protocol errors, invalid params, cancellation
            status, error = "error", f"{type(e).__name__}: {e}"
            raise
        finally:
            self._write(ctx, ts, (time.perf_counter() - start) * 1000, status, error)

    def _write(self, ctx: ServerRequestContext[Any, Any], ts: str, duration_ms: float, status: str, error: str | None) -> None:
        params = ctx.params or {}
        args = params.get("arguments") or {}
        client = getattr(getattr(ctx.request, "client", None), "host", None)
        state = getattr(ctx.request, "state", None)
        user, token_id = getattr(state, "user", None), getattr(state, "token_id", None)
        try:
            self._db.execute(
                "INSERT INTO tool_calls"
                " (ts, client, user, token_id, tool, database, sql, arguments, duration_ms, status, error)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ts, client, user, token_id, params.get("name"), _str(args.get("database")), _str(args.get("sql")),
                    json.dumps(args, ensure_ascii=False, default=str), round(duration_ms, 3), status, error,
                ),
            )
        except Exception:
            log.exception("failed to write the query log")


def _tool_error(result: HandlerResult) -> str | None:
    """Text of a tool-level error (the call itself succeeded but the tool returned isError)."""
    if result is None:
        return None
    data = result if isinstance(result, dict) else result.model_dump(by_alias=True)
    if not data.get("isError"):
        return None
    return "\n".join(c.get("text", "") for c in data.get("content") or [] if isinstance(c, dict)) or "error"


def _str(v: Any) -> str | None:
    return v if isinstance(v, str) or v is None else json.dumps(v, default=str)
