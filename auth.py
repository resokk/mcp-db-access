"""Optional bearer-token authentication: users and their tokens in a SQLite file.

Management CLI (reads the [auth] section of the server config):
    uv run python auth.py config.toml user-add <user_id> <cert_sn>
    uv run python auth.py config.toml user-del <user_id>          # also deletes the user's tokens
    uv run python auth.py config.toml token-add <user_id>         # prints the new token once
    uv run python auth.py config.toml token-del <token_id>
    uv run python auth.py config.toml list
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
    id      TEXT PRIMARY KEY,
    cert_sn TEXT NOT NULL              -- certificate serial number
);
CREATE TABLE IF NOT EXISTS tokens (
    id         INTEGER PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,   -- SHA-256 of the token; the token itself is never stored
    created_at TEXT NOT NULL
);
"""


class TokenStore:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    def user_for(self, token: str) -> str | None:
        row = self._db.execute("SELECT user_id FROM tokens WHERE token_hash = ?", (_hash(token),)).fetchone()
        return row[0] if row else None

    def add_user(self, user_id: str, cert_sn: str) -> None:
        self._db.execute("INSERT INTO users (id, cert_sn) VALUES (?, ?)", (user_id, cert_sn))

    def delete_user(self, user_id: str) -> bool:
        return self._db.execute("DELETE FROM users WHERE id = ?", (user_id,)).rowcount > 0

    def add_token(self, user_id: str) -> tuple[int, str]:
        token = secrets.token_urlsafe(32)
        cur = self._db.execute(
            "INSERT INTO tokens (user_id, token_hash, created_at) VALUES (?, ?, ?)",
            (user_id, _hash(token), dt.datetime.now(dt.UTC).isoformat(timespec="seconds")),
        )
        return cur.lastrowid, token

    def delete_token(self, token_id: int) -> bool:
        return self._db.execute("DELETE FROM tokens WHERE id = ?", (token_id,)).rowcount > 0

    def users(self) -> list[dict[str, Any]]:
        users = {uid: {"id": uid, "cert_sn": sn, "tokens": []}
                 for uid, sn in self._db.execute("SELECT id, cert_sn FROM users ORDER BY id")}
        for tid, uid, created in self._db.execute("SELECT id, user_id, created_at FROM tokens ORDER BY id"):
            users[uid]["tokens"].append({"id": tid, "created_at": created})
        return list(users.values())


class BearerAuth:
    """ASGI middleware: every HTTP request needs `Authorization: Bearer <token>` of a known user.

    The user id is put into the request state (`request.state.user`) for the query log.
    """

    def __init__(self, app: Any, store: TokenStore):
        self.app = app
        self.store = store

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":  # lifespan
            return await self.app(scope, receive, send)
        headers = dict(scope["headers"])
        scheme, _, token = headers.get(b"authorization", b"").decode("latin-1").partition(" ")
        user = self.store.user_for(token.strip()) if scheme.lower() == "bearer" and token.strip() else None
        if user is None:
            return await _unauthorized(send)
        scope.setdefault("state", {})["user"] = user
        await self.app(scope, receive, send)


async def _unauthorized(send: Any) -> None:
    body = json.dumps({"error": "invalid_token", "error_description": "missing or unknown bearer token"}).encode()
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
            print(f"user {args[0]!r} deleted" if store.delete_user(args[0]) else f"no user {args[0]!r}")
        elif cmd == "token-add" and len(args) == 1:
            token_id, token = store.add_token(args[0])
            print(f"token {token_id} for {args[0]!r} (shown only once):\n{token}")
        elif cmd == "token-del" and len(args) == 1:
            print(f"token {args[0]} deleted" if store.delete_token(int(args[0])) else f"no token {args[0]}")
        elif cmd == "list" and not args:
            print(json.dumps(store.users(), indent=2, ensure_ascii=False))
        else:
            print(__doc__)
            return 2
    except sqlite3.IntegrityError as e:  # duplicate user, token for an unknown user
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
