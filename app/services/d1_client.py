"""Cloudflare D1 transport - the one place that knows whether "the D1
database" means Cloudflare's HTTP query API or a local SQLite mirror (see
DB_MODE in app/core/config.py). Every repository (app/repositories/d1/*)
and app/services/d1.py's own remaining functions call d1_query() here
instead of touching sqlite3/httpx directly, so a future swap (a different
storage backend entirely, or just D1's API changing) has exactly one
place to change.

Split out of app/services/d1.py so the per-table repositories can depend on
this transport without depending on d1.py itself (which, the other way
around, re-exports the repositories' functions for callers that still do
`from app.services.d1 import persist_post` etc.) - importing d1.py from a
repository would be a circular import."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import Any

import httpx

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.cloudflare.com/client/v4"

_local_conn: sqlite3.Connection | None = None
_local_lock = threading.Lock()
_logged_db_target = False

# Shared remote HTTP client + concurrency cap. Creating a fresh AsyncClient
# per query (TLS handshake to api.cloudflare.com) under a dashboard fan-out
# of /stats/* + ingest writes saturates the event loop and has hung the
# whole API (even /health) after switching DB_MODE=remote.
_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock()
_remote_sem = asyncio.Semaphore(8)


def _get_local_conn() -> sqlite3.Connection:
    global _local_conn
    if _local_conn is None:
        # timeout=30: the stdlib default (5s) is what was actually expiring
        # into "database is locked" (confirmed live 2026-09-17 - a burst of
        # these every ~5s while scripts/pull_local_db.py was rebuilding this
        # same file concurrently) - 30s comfortably outlasts one INSERT/
        # UPDATE's own lock hold, not just this script's occasional
        # multi-minute rebuild.
        #
        # WAL journal mode: the stdlib default (DELETE/rollback-journal)
        # takes an exclusive lock on the *whole file* for a write's
        # duration, blocking even unrelated readers - WAL lets readers
        # proceed against the last-committed snapshot while a write is in
        # flight, which is what this service's read-heavy dashboard
        # endpoints actually need alongside Kafka-driven writes.
        conn = sqlite3.connect(settings.local_db_path, check_same_thread=False, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        _local_conn = conn
    return _local_conn


def _run_local_query(sql: str, params: list[Any] | None) -> list[dict[str, Any]]:
    """Runs synchronously on a worker thread (see d1_query below) - Python's
    stdlib sqlite3 has no async API, and blocking the event loop directly
    would stall every other in-flight request for the query's duration."""
    conn = _get_local_conn()
    # One shared connection, many asyncio.to_thread workers - SQLite will
    # otherwise interleave execute/fetch and raise "bad parameter or other
    # API misuse" / IndexError on dict(row).
    with _local_lock:
        cursor = conn.execute(sql, params or [])
        # D1's HTTP API returns [] (not an error) for a successful INSERT/UPDATE/
        # DELETE with no rows to return - mirrored here so callers' `is None`
        # (failure) vs `[]`/rows (success) checks behave identically in both modes.
        if cursor.description is None:
            conn.commit()
            return []
        return [dict(row) for row in cursor.fetchall()]


def _configured() -> bool:
    if settings.db_mode == "local":
        return Path(settings.local_db_path).exists()
    return bool(settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id)


async def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        return _http_client
    async with _http_client_lock:
        if _http_client is not None and not _http_client.is_closed:
            return _http_client
        # Default timeout overridden per-request below; limits keep a
        # dashboard refresh from opening dozens of Cloudflare TLS sessions.
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )
        return _http_client


async def d1_query(
    sql: str,
    params: list[Any] | None = None,
    *,
    quiet: bool = False,
    timeout: float = 10.0,
) -> list[dict[str, Any]] | None:
    """Runs one SQL statement against the configured D1 database. Returns
    the result rows, or None if D1 isn't configured or the call failed.
    quiet=True skips failure logs (idempotent migrations that expect
    'duplicate column' on already-migrated DBs)."""
    if not _configured():
        return None

    global _logged_db_target
    if not _logged_db_target:
        _logged_db_target = True
        logger.info(
            "d1_target",
            mode=settings.db_mode,
            path=settings.local_db_path if settings.db_mode == "local" else None,
        )

    if settings.db_mode == "local":
        try:
            return await asyncio.to_thread(_run_local_query, sql, params)
        except (sqlite3.Error, IndexError) as exc:
            if not quiet:
                logger.warning("local_db_query_failed", error=str(exc), sql=sql[:200])
            return None

    url = (
        f"{_BASE_URL}/accounts/{settings.cloudflare_account_id}/d1/database/{settings.cloudflare_d1_database_id}/query"
    )
    headers = {"Authorization": f"Bearer {settings.cloudflare_api_token}"}

    try:
        async with _remote_sem:
            client = await _get_http_client()
            resp = await client.post(
                url,
                headers=headers,
                json={"sql": sql, "params": params or []},
                timeout=timeout,
            )
    except httpx.HTTPError as exc:
        if not quiet:
            logger.warning("d1_request_failed", error=str(exc))
        return None

    if resp.status_code >= 400:
        if not quiet:
            logger.warning("d1_request_failed", status=resp.status_code, body=resp.text[:500])
        return None

    data = resp.json()
    if not data.get("success"):
        if not quiet:
            logger.warning("d1_query_failed", errors=data.get("errors"))
        return None

    results = data.get("result") or []
    return results[0].get("results", []) if results else []


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
