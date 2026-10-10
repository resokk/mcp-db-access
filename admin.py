"""Read-only web console (Homebrew-terminal style) for the users and tokens of the auth database.

    uv run python admin.py config.toml        # http://127.0.0.1:8001

A separate process from the MCP server and always bound to localhost: the MCP endpoint may sit behind a
reverse proxy, the console must not. Port: `[admin] port` in the config.
"""

from __future__ import annotations

import sys
import tomllib
from html import escape
from typing import Any
from urllib.parse import quote

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.routing import Route

from auth import TokenStore, auth_db_path

STYLE = """
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body { margin: 0; background: #1c1c1c; color: #d0d0d0;
       font: 14px/1.55 "SF Mono", Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
main { max-width: 960px; margin: 0 auto; padding: 24px 16px 48px; }
pre { margin: 0; white-space: pre; overflow-x: auto; font: inherit; }
a { color: inherit; text-decoration: underline; text-decoration-color: #5a5a5a; text-underline-offset: 3px; }
a:hover { color: #fff; text-decoration-color: #fff; }
a:focus-visible { outline: 1px solid #5f87ff; outline-offset: 2px; }
.prompt { color: #8a8a8a; }
.arrow { color: #5f87ff; font-weight: bold; }
.title { color: #fff; font-weight: bold; }
.dim { color: #8a8a8a; }
.ok { color: #5fd75f; }
.warn { color: #d7af5f; }
.err { color: #ff5f5f; font-weight: bold; }
.cursor { display: inline-block; width: .6em; height: 1.1em; vertical-align: text-bottom; background: #d0d0d0;
          animation: blink 1.1s steps(1) infinite; }
@keyframes blink { 50% { opacity: 0; } }
@media (prefers-reduced-motion: reduce) { .cursor { animation: none; } }
"""

STATUS_MARK = {"valid": ("ok", "✔"), "expired": ("warn", "⚠"), "revoked": ("err", "✘")}


def page(title: str, command: str, body: str) -> HTMLResponse:
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title><style>{STYLE}</style></head>
<body><main><pre><span class="prompt">$</span> {escape(command)}
{body}
<span class="prompt">$</span> <span class="cursor"></span></pre></main></body></html>"""
    return HTMLResponse(html)


def header(text: str) -> str:
    return f'<span class="arrow">==&gt;</span> <span class="title">{escape(text)}</span>'


def user_status(user: dict[str, Any]) -> str:
    if user["deleted_at"]:
        return f'<span class="err">deleted</span> <span class="dim">{escape(user["deleted_at"])}</span>'
    return '<span class="ok">active</span>'


async def users_page(request: Request) -> HTMLResponse:
    users = request.app.state.store.users()
    if not users:
        return page("Users", "db-access users", f'{header("Users (0)")}\n<span class="dim">No users yet. '
                    "Add one: uv run python auth.py config.toml user-add &lt;id&gt; &lt;cert_sn&gt;</span>\n")
    width = max(len(u["id"]) for u in users) + 2
    sn_width = max(len(u["cert_sn"]) for u in users) + 2
    lines = [header(f"Users ({len(users)})")]
    for u in users:
        valid = sum(t["status"] == "valid" for t in u["tokens"])
        link = f'<a href="/users/{escape(quote(u["id"], safe=""))}">{escape(u["id"])}</a>'
        tokens = f'{len(u["tokens"])} token{"s" * (len(u["tokens"]) != 1)}, {valid} valid'
        lines.append(
            f'{link}{" " * (width - len(u["id"]))}{escape(u["cert_sn"])}{" " * (sn_width - len(u["cert_sn"]))}'
            f'{tokens:<22}{user_status(u)}'
        )
    return page("Users", "db-access users", "\n".join(lines) + "\n")


async def user_page(request: Request) -> HTMLResponse:
    user_id = request.path_params["user_id"]
    user = next((u for u in request.app.state.store.users() if u["id"] == user_id), None)
    command = f"db-access user {user_id}"
    if user is None:
        body = f'<span class="err">Error:</span> No such user: {escape(user_id)}\n\n<a href="/">← all users</a>\n'
        return page("Not found", command, body)
    lines = [
        header(user["id"]),
        f'<span class="dim">cert_sn:</span> {escape(user["cert_sn"])}',
        f'<span class="dim">status:</span>  {user_status(user)}',
        "",
        header(f"Tokens ({len(user['tokens'])})"),
    ]
    if not user["tokens"]:
        lines.append('<span class="dim">No tokens.</span>')
    for t in user["tokens"]:
        cls, mark = STATUS_MARK[t["status"]]
        lines.append(
            f'<span class="{cls}">{mark} {t["status"]:<8}</span>#{t["id"]:<5}'
            f'<span class="dim">created</span> {escape(t["created_at"])}  '
            f'<span class="dim">expires</span> {escape(t["expires_at"] or "never")}'
        )
    lines += ["", '<a href="/">← all users</a>']
    return page(user["id"], command, "\n".join(lines) + "\n")


def main(argv: list[str]) -> None:
    path = argv[0] if argv else "config.toml"
    with open(path, "rb") as f:
        config = tomllib.load(f)
    app = Starlette(routes=[Route("/", users_page), Route("/users/{user_id:path}", user_page)])
    app.state.store = TokenStore(auth_db_path(config))
    uvicorn.run(app, host="127.0.0.1", port=config.get("admin", {}).get("port", 8001))


if __name__ == "__main__":
    main(sys.argv[1:])
