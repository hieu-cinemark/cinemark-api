"""Pulls new-to-local posts from the real D1 database - the safe,
additive counterpart to scripts/push_local_data_to_remote.py's own posts
push. Written for 2026-09-22: after that earlier push, remote has posts
local never had (remote keeps whatever was pushed; local's own crawl
meanwhile keeps generating comments/dropped_posts remote doesn't have
yet - see that day's dropped_posts/posts count check). This brings
local's *posts* table back to parity with remote without touching
comments/dropped_posts (local is already ahead there - nothing to pull)
and without the destructive drop-and-recreate scripts/pull_local_db.py
does (that would also wipe local-only comments/dropped_posts rows not
yet pushed to remote, and isn't safe to run while a server/consumer holds
this local db file open - see that script's own docstring). This script
only ever INSERTs (OR IGNORE) into the existing local tables, so it's
safe to run alongside an active DB_MODE=local server/consumer - WAL mode
already lets a reader/writer proceed against the last-committed snapshot
(see app/services/d1_client.py's _get_local_conn comment).

Movie/keyword ids can differ between local and remote for the same
logical row (a local-only movie/keyword created under DB_MODE=local
before ever being pushed gets a different uuid than the row a later push
matched it to by slug/keyword-text - see push_local_data_to_remote.py's
own remap functions). Same idea here, just in the opposite id direction,
so a pulled post's movie_id/keyword_id points at the LOCAL row other
local posts already reference, not a dangling remote-only id.

Safe to re-run anytime - diffs by id like the push script, so an id
already present locally is just skipped.

    python -m scripts.pull_new_posts_from_remote
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Same D1 response-size rationale as scripts/pull_local_db.py's own _PAGE_SIZE.
_PAGE_SIZE = 500


def _local_ids(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[0] for row in conn.execute(f"SELECT id FROM {table}")}


async def _movie_id_remap_from_remote(d1_query, local_conn: sqlite3.Connection) -> dict[str, str]:
    """remote_id -> local_id for movies that are the same row (by slug) under a different uuid."""
    remote = await d1_query("SELECT id, slug FROM movies")
    if remote is None:
        raise RuntimeError("Could not read remote movies - check D1 credentials/quota.")
    local_id_by_slug = {row["slug"]: row["id"] for row in local_conn.execute("SELECT id, slug FROM movies")}
    remap = {r["id"]: local_id_by_slug[r["slug"]] for r in remote if r["slug"] in local_id_by_slug and local_id_by_slug[r["slug"]] != r["id"]}
    if remap:
        logger.info("movie_ids_remapped_from_remote", count=len(remap))
    return remap


async def _keyword_id_remap_from_remote(
    d1_query, local_conn: sqlite3.Connection, movie_id_remap: dict[str, str]
) -> dict[str, str]:
    """remote_id -> local_id for keywords that are the same row (by movie/platform/text) under a different uuid."""
    remote = await d1_query("SELECT id, movie_id, platform, keyword FROM keywords")
    if remote is None:
        raise RuntimeError("Could not read remote keywords - check D1 credentials/quota.")
    local_id_by_key = {
        (row["movie_id"], row["platform"], row["keyword"]): row["id"]
        for row in local_conn.execute("SELECT id, movie_id, platform, keyword FROM keywords")
    }
    remap: dict[str, str] = {}
    for r in remote:
        local_movie_id = movie_id_remap.get(r["movie_id"], r["movie_id"])
        local_id = local_id_by_key.get((local_movie_id, r["platform"], r["keyword"]))
        if local_id and local_id != r["id"]:
            remap[r["id"]] = local_id
    if remap:
        logger.info("keyword_ids_remapped_from_remote", count=len(remap))
    return remap


def _remap_row(
    row: dict[str, Any], movie_id_remap: dict[str, str], keyword_id_remap: dict[str, str]
) -> dict[str, Any]:
    remapped = dict(row)
    if remapped.get("movie_id") in movie_id_remap:
        remapped["movie_id"] = movie_id_remap[remapped["movie_id"]]
    if remapped.get("keyword_id") in keyword_id_remap:
        remapped["keyword_id"] = keyword_id_remap[remapped["keyword_id"]]
    return remapped


async def pull_new_posts() -> None:
    settings.db_mode = "remote"  # reads come from remote; writes go straight to the local sqlite file below
    from app.services.d1 import d1_query  # imported after forcing remote, not at module load

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to read "
            "from the real D1 - this script has nothing to pull from without them."
        )

    local_path = Path(settings.local_db_path)
    if not local_path.exists():
        raise RuntimeError(f"No local mirror at {local_path} - nothing to pull into.")

    # timeout=30 + WAL, same as app/services/d1_client.py's _get_local_conn -
    # a running ingest/crawl consumer may hold this file open concurrently.
    local_conn = sqlite3.connect(local_path, timeout=30.0)
    local_conn.execute("PRAGMA journal_mode=WAL")
    local_conn.execute("PRAGMA busy_timeout=30000")
    local_conn.row_factory = sqlite3.Row

    try:
        movie_id_remap = await _movie_id_remap_from_remote(d1_query, local_conn)
        keyword_id_remap = await _keyword_id_remap_from_remote(d1_query, local_conn, movie_id_remap)

        local_ids = _local_ids(local_conn, "posts")
        columns: list[str] | None = None
        insert_sql: str | None = None
        offset = 0
        total_seen = 0
        total_pulled = 0
        while True:
            rows = await d1_query(f"SELECT * FROM posts LIMIT {_PAGE_SIZE} OFFSET {offset}")
            if rows is None:
                raise RuntimeError(f"D1 read failed for posts at offset={offset} - see logged error above.")
            if not rows:
                break
            total_seen += len(rows)
            new_rows = [r for r in rows if r["id"] not in local_ids]
            if new_rows:
                if columns is None:
                    columns = list(new_rows[0].keys())
                    placeholders = ", ".join("?" for _ in columns)
                    insert_sql = f"INSERT OR IGNORE INTO posts ({', '.join(columns)}) VALUES ({placeholders})"
                remapped = [_remap_row(r, movie_id_remap, keyword_id_remap) for r in new_rows]
                local_conn.executemany(insert_sql, [[r[c] for c in columns] for r in remapped])
                local_conn.commit()
                total_pulled += len(new_rows)
                logger.info("posts_batch_pulled", pulled_so_far=total_pulled, remote_seen=total_seen)
            offset += _PAGE_SIZE
            if len(rows) < _PAGE_SIZE:
                break
    finally:
        local_conn.close()

    logger.info("pull_new_posts_finished", telegram=True, remote_seen=total_seen, pulled=total_pulled)


if __name__ == "__main__":
    asyncio.run(pull_new_posts())
