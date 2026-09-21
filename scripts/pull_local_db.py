"""Pulls a full local SQLite mirror of the real D1 database (cinemark-
scraper's, see app/core/config.py's cloudflare_d1_database_id) so
DB_MODE=local can be used for local dev/testing without spending the real
D1 account's daily row-read quota - see that setting's own comment for why
this exists (this repo's replacement for a sibling cinemark-be repo's pull
script, which no longer exists in this workspace).

Recreates every table's exact schema (introspected fresh from D1's own
sqlite_master, so it can't drift from reality) AND copies every row of
every table - unlike an earlier, narrower version of this script that only
seeded movies/keywords, this is a genuine full clone (the dashboard's
Tổng quan/Bài viết pages read through the same DB_MODE switch, so a partial
mirror made them look like almost all historical data had vanished).
Fetches each table in LIMIT/OFFSET pages (see _PAGE_SIZE) rather than one
giant SELECT *, since a few of these tables (posts, dropped_posts,
post_engagement_snapshots - each in the five digits, some rows carrying a
sizeable raw_json blob) risk exceeding D1's own per-query response-size
cap in a single unpaged request.

Always reads from the real D1 regardless of the current DB_MODE setting
(forces remote for the duration of this script) - the whole point is to
copy *from* remote *into* local, so it must never accidentally read the
stale local file it's about to overwrite.

Safe to re-run anytime in isolation - drops and recreates every table fresh
each time. NOT safe to run while app.main's server (uvicorn) is up and
using DB_MODE=local against the same path: that process holds its own
long-lived sqlite3 connection (see app/services/d1.py's _local_conn) opened
against the file's current inode; unlink() here detaches the path from
that inode without closing anyone else's already-open handle to it, so the
running server keeps silently reading/writing an orphaned copy nothing can
see by path anymore, while every *new* connection (including this script's
own) gets the fresh one - confirmed live (2026-09-17): running this
alongside an active server produced a burst of "database is locked"
errors from the server's Kafka-driven writes colliding with this script's
own commits on the same path mid-rebuild, and would have gone on
diverging silently afterward even once the locked errors stopped. Stop the
server first, pull, then restart it - restarting is what makes it open a
fresh connection against the new file.

A full pull of a database this size takes a couple of minutes and spends
real D1 read quota (proportional to total row count) - that's the
one-time, one-directional cost of having a local mirror at all; nothing
afterward (local dev/testing with DB_MODE=local) spends any further quota
until the next refresh.

    python -m scripts.pull_local_db
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Rows per D1 page - comfortably under D1's response-size ceiling even for
# posts' raw_json-carrying rows, while still keeping the total request
# count for a ~12k-row table in the tens, not hundreds.
_PAGE_SIZE = 500


async def _copy_table(d1_query, conn: sqlite3.Connection, table_name: str) -> None:
    offset = 0
    total = 0
    columns: list[str] | None = None
    while True:
        rows = await d1_query(f"SELECT * FROM {table_name} LIMIT {_PAGE_SIZE} OFFSET {offset}")
        if rows is None:
            raise RuntimeError(f"D1 read failed for table={table_name!r} at offset={offset} - see logged error above.")
        if not rows:
            break
        if columns is None:
            columns = list(rows[0].keys())
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        conn.executemany(insert_sql, [[row[c] for c in columns] for row in rows])  # type: ignore[union-attr]
        conn.commit()
        total += len(rows)
        offset += _PAGE_SIZE
        if len(rows) < _PAGE_SIZE:
            break
    if total:
        logger.info("local_db_table_copied", table=table_name, rows=total)
    else:
        logger.info("local_db_table_empty", table=table_name)


async def pull() -> None:
    # See module docstring - running this while app.main's server is up
    # against the same local_db_path causes real, confirmed problems
    # (lock contention during the rebuild, silent divergence after it).
    logger.warning(
        "local_db_pull_starting",
        path=settings.local_db_path,
        warning="stop any server using DB_MODE=local against this path first, and restart it after this finishes",
    )
    settings.db_mode = "remote"  # see module docstring - never read the local file here
    from app.services.d1 import d1_query  # imported after forcing remote, not at module load

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to pull from "
            "the real D1 - this script has nothing to copy without them."
        )

    tables = await d1_query(
        "SELECT name, sql FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' AND name NOT LIKE 'd1_%'"
    )
    if not tables:
        raise RuntimeError("Could not read the real D1's schema (empty/failed response) - check D1 credentials/quota.")

    local_path = Path(settings.local_db_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        local_path.unlink()  # fresh file every run - see module docstring

    conn = sqlite3.connect(local_path)
    try:
        for table in tables:
            conn.execute(table["sql"])
        conn.commit()
        logger.info("local_db_schema_created", tables=[t["name"] for t in tables], path=str(local_path))

        for table in tables:
            await _copy_table(d1_query, conn, table["name"])
    finally:
        conn.close()

    logger.info("local_db_pull_finished", path=str(local_path), telegram=True)


if __name__ == "__main__":
    asyncio.run(pull())
