"""Optional bearer-token authentication: users and their tokens in a SQLite file.

Users and tokens are never deleted physically, so the query log can always be traced back to them:
a user is marked deleted, a token is revoked. A token is accepted while it is active, not expired
(expires_at empty = never) and its user is not deleted.

Management CLI (reads the [auth] section of the server config):
    uv run python auth.py config.toml user-add <user_id> <cert_sn>
    uv run python auth.py config.toml user-del <user_id>                  # mark deleted: its tokens stop working
    uv run python auth.py config.toml user-restore <user_id>
    uv run python auth.py config.toml token-add <user_id> [<expires>]     # prints the new token once
    uv run python auth.py config.toml token-revoke <token_id>
    uv run python auth.py config.toml token-activate <token_id>
    uv run python auth.py config.toml token-expires <token_id> <expires|never>
    uv run python auth.py config.toml list

<expires>: ISO date or date-time, e.g. 2026-12-31 (valid through that day, UTC) or 2026-12-31T18:00+03:00.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import secrets
import sqlite3
import sys
import tomllib
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id         TEXT PRIMARY KEY,
    cert_sn    TEXT NOT NULL,          -- certificate serial number
    deleted_at TEXT                    -- UTC, ISO 8601; NULL = not deleted
);
CREATE TABLE IF NOT EXISTS tokens (
    id         INTEGER PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users (id),
    token_hash TEXT NOT NULL UNIQUE,   -- SHA-256 of the token; the token itself is never stored
    created_at TEXT NOT NULL,
    active     INTEGER NOT NULL DEFAULT 1,
    expires_at TEXT                    -- UTC, ISO 8601; NULL = never expires
);
"""
# Columns added after the first release, for auth files created before them.
_NEW_COLUMNS = {
    "users": {"deleted_at": "TEXT"},
    "tokens": {"active": "INTEGER NOT NULL DEFAULT 1", "expires_at": "TEXT"},
}


class TokenStore:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.executescript(SCHEMA)
        for table, new in _NEW_COLUMNS.items():
            columns = {row[1] for row in self._db.execute(f"PRAGMA table_info({table})")}
            for name, ddl in new.items():
                if name not in columns:
                    self._db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    def close(self) -> None:
        self._db.close()

    def lookup(self, token: str) -> tuple[str, int] | None:
        """(user_id, token_id) of an active, unexpired token of a user that is not deleted."""
        return self._db.execute(
            "SELECT t.user_id, t.id FROM tokens t JOIN users u ON u.id = t.user_id"
            " WHERE t.token_hash = ? AND t.active AND (t.expires_at IS NULL OR t.expires_at > ?)"
            " AND u.deleted_at IS NULL",
            (_hash(token), _now()),
        ).fetchone()

    def add_user(self, user_id: str, cert_sn: str) -> None:
        self._db.execute("INSERT INTO users (id, cert_sn) VALUES (?, ?)", (user_id, cert_sn))

    def delete_user(self, user_id: str) -> bool:
        return self._db.execute(
            "UPDATE users SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL", (_now(), user_id)
        ).rowcount > 0

    def restore_user(self, user_id: str) -> bool:
        return self._db.execute(
            "UPDATE users SET deleted_at = NULL WHERE id = ? AND deleted_at IS NOT NULL", (user_id,)
        ).rowcount > 0

    def add_token(self, user_id: str, expires_at: str | None = None) -> tuple[int, str]:
        if self._db.execute("SELECT 1 FROM users WHERE id = ? AND deleted_at IS NOT NULL", (user_id,)).fetchone():
            raise ValueError(f"user {user_id!r} is deleted")
        token = secrets.token_urlsafe(32)
        cur = self._db.execute(
            "INSERT INTO tokens (user_id, token_hash, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (user_id, _hash(token), _now(), expires_at),
        )
        return cur.lastrowid, token

    def set_active(self, token_id: int, active: bool) -> bool:
        return self._db.execute("UPDATE tokens SET active = ? WHERE id = ?", (int(active), token_id)).rowcount > 0

    def set_expires(self, token_id: int, expires_at: str | None) -> bool:
        return self._db.execute("UPDATE tokens SET expires_at = ? WHERE id = ?", (expires_at, token_id)).rowcount > 0

    def users(self) -> list[dict[str, Any]]:
        now = _now()
        users = {uid: {"id": uid, "cert_sn": sn, "deleted_at": deleted, "tokens": []}
                 for uid, sn, deleted in self._db.execute("SELECT id, cert_sn, deleted_at FROM users ORDER BY id")}
        rows = self._db.execute("SELECT id, user_id, created_at, active, expires_at FROM tokens ORDER BY id")
        for tid, uid, created, active, expires in rows:
            status = "revoked" if not active else "expired" if expires and expires <= now else "valid"
            users[uid]["tokens"].append({"id": tid, "created_at": created, "expires_at": expires, "status": status})
        return list(users.values())


class BearerAuth:
    """ASGI middleware: every HTTP request needs `Authorization: Bearer <token>` of a known user.

    The user and token ids are put into the request state (`request.state.user`, `.token_id`) for the query log.
    """

    def __init__(self, app: Any, store: TokenStore):
        self.app = app
        self.store = store

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":  # lifespan
            return await self.app(scope, receive, send)
        headers = dict(scope["headers"])
        scheme, _, token = headers.get(b"authorization", b"").decode("latin-1").partition(" ")
        found = self.store.lookup(token.strip()) if scheme.lower() == "bearer" and token.strip() else None
        if found is None:
            return await _unauthorized(send)
        state = scope.setdefault("state", {})
        state["user"], state["token_id"] = found
        await self.app(scope, receive, send)


async def _unauthorized(send: Any) -> None:
    body = json.dumps({"error": "invalid_token", "error_description": "missing, unknown, revoked or expired bearer token"}).encode()
    await send({
        "type": "http.response.start",
        "status": 401,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
            (b"www-authenticate", b'Bearer error="invalid_token"'),
        ],
    })
    await send({"type": "http.response.body", "body": body})


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def parse_expires(value: str) -> str | None:
    """CLI expiry to the stored form (UTC, same format as _now() so strings compare in time order)."""
    if value.lower() in ("never", "none", ""):
        return None
    if len(value) == 10:  # a date: valid through that day
        moment = dt.datetime.combine(dt.date.fromisoformat(value) + dt.timedelta(days=1), dt.time(), dt.UTC)
    else:
        moment = dt.datetime.fromisoformat(value)
        if moment.tzinfo is None:
            raise ValueError(f"{value!r}: add a time zone, e.g. {value}+03:00 or {value}Z")
    return moment.astimezone(dt.UTC).isoformat(timespec="seconds")


def auth_db_path(config: dict[str, Any]) -> str:
    return config.get("auth", {}).get("db", "auth.sqlite")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    with open(argv[0], "rb") as f:
        store = TokenStore(auth_db_path(tomllib.load(f)))
    cmd, args = argv[1], argv[2:]
    try:
        if cmd == "user-add" and len(args) == 2:
            store.add_user(*args)
            print(f"user {args[0]!r} added")
        elif cmd == "user-del" and len(args) == 1:
            print(f"user {args[0]!r} marked deleted" if store.delete_user(args[0]) else f"no active user {args[0]!r}")
        elif cmd == "user-restore" and len(args) == 1:
            print(f"user {args[0]!r} restored" if store.restore_user(args[0]) else f"no deleted user {args[0]!r}")
        elif cmd == "token-add" and len(args) in (1, 2):
            expires = parse_expires(args[1]) if len(args) == 2 else None
            token_id, token = store.add_token(args[0], expires)
            print(f"token {token_id} for {args[0]!r}, expires {expires or 'never'} (shown only once):\n{token}")
        elif cmd in ("token-revoke", "token-activate") and len(args) == 1:
            done = store.set_active(int(args[0]), cmd == "token-activate")
            print(f"token {args[0]} {cmd.removeprefix('token-')}d" if done else f"no token {args[0]}")
        elif cmd == "token-expires" and len(args) == 2:
            expires = parse_expires(args[1])
            done = store.set_expires(int(args[0]), expires)
            print(f"token {args[0]} expires {expires or 'never'}" if done else f"no token {args[0]}")
        elif cmd == "list" and not args:
            print(json.dumps(store.users(), indent=2, ensure_ascii=False))
        else:
            print(__doc__)
            return 2
    except (sqlite3.IntegrityError, ValueError) as e:  # duplicate/unknown/deleted user, bad date
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
