"""Pushes new data from the local SQLite mirror (see scripts/pull_local_db.py)
up to the real D1 database - the one-off reverse direction that script never
needed, written for 2026-09-15's session: DB_MODE=local was on while a full
crawl batch ran, so every post/comment (and any keyword added via the
dashboard meanwhile) landed only in .local-db/scraper.sqlite, invisible to
production until pushed up.

Scope (by request - dropped_posts/post_engagement_snapshots deliberately
left out, both are either disposable or regeneratable, not worth the extra
D1 write quota):
  - keywords (and, as a hard FK prerequisite, any movies they need) that
    exist locally but not yet in remote
  - every local post, keyword_id validated against the now-migrated remote keywords
  - every local comment, whose post_id row must already exist in remote

Insert order matters (FK constraints): movies -> keywords -> posts ->
comments. Every insert uses "INSERT OR IGNORE" and is safe to re-run - an
id that already made it to remote (a previous partial run, or genuinely
already there) is silently skipped rather than erroring the whole batch.

Local movies/keywords can share a unique slug/text with a remote row but a
different uuid (dashboard created them while DB_MODE=local). Those ids are
rewritten to the remote row before child inserts.

Always forces db_mode="remote" for the writes (same rationale as
pull_local_db.py: reads the *local* sqlite file directly with its own
connection, never through d1_query, so forcing remote can't accidentally
point d1_query back at the file this is reading from) and pages inserts in
batches to stay under D1's per-statement size/response limits and this
project's own 10s D1 HTTP timeout.

    python -m scripts.push_local_data_to_remote
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# D1's HTTP query API rejects a statement above some bound-parameter count
# well under SQLite's own usual 999 (confirmed live 2026-09-15: a 16-row,
# 12-column movies batch - 192 params - already got rejected as "too many
# SQL variables"). Not documented anywhere obvious, so staying well clear
# of it rather than hunting the exact number: cap every batch at this many
# *parameters* (rows * column count), not a fixed row count, so a wide
# table (posts, 19 columns) automatically gets a smaller row-batch than a
# narrow one (comments, 15) without needing its own hardcoded constant.
_MAX_PARAMS_PER_BATCH = 90


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


async def _remote_ids(d1_query, table: str) -> set[str]:
    """Page through remote ids - a single SELECT of every posts.id can
    exceed d1_query's 10s HTTP timeout once the table is tens of thousands
    of rows, which would abort the whole push before any writes."""
    ids: set[str] = set()
    page_size = 2000
    offset = 0
    while True:
        rows = await d1_query(f"SELECT id FROM {table} LIMIT {page_size} OFFSET {offset}")
        if rows is None:
            raise RuntimeError(f"Could not read remote {table!r} ids - check D1 credentials/quota.")
        ids.update(row["id"] for row in rows)
        if len(rows) < page_size:
            return ids
        offset += page_size


def _row_values(
    row: sqlite3.Row,
    columns: list[str],
    movie_id_remap: dict[str, str],
    keyword_id_remap: dict[str, str],
) -> list[Any]:
    values: list[Any] = []
    for column in columns:
        value = row[column]
        if column == "movie_id" and value in movie_id_remap:
            value = movie_id_remap[value]
        elif column == "keyword_id" and value in keyword_id_remap:
            value = keyword_id_remap[value]
        values.append(value)
    return values


async def _movie_id_remap(d1_query, local_conn: sqlite3.Connection) -> dict[str, str]:
    remote = await d1_query("SELECT id, slug FROM movies")
    if remote is None:
        raise RuntimeError("Could not read remote movies for slug remap - check D1 credentials/quota.")
    remote_id_by_slug = {row["slug"]: row["id"] for row in remote}
    remap: dict[str, str] = {}
    for row in local_conn.execute("SELECT id, slug FROM movies"):
        remote_id = remote_id_by_slug.get(row["slug"])
        if remote_id and remote_id != row["id"]:
            remap[row["id"]] = remote_id
    if remap:
        logger.info("movie_ids_remapped_by_slug", count=len(remap), local_ids=sorted(remap))
    return remap


async def _keyword_id_remap(
    d1_query,
    local_conn: sqlite3.Connection,
    movie_id_remap: dict[str, str],
) -> dict[str, str]:
    remote = await d1_query("SELECT id, movie_id, platform, keyword FROM keywords")
    if remote is None:
        raise RuntimeError("Could not read remote keywords for remap - check D1 credentials/quota.")
    remote_id_by_key = {(row["movie_id"], row["platform"], row["keyword"]): row["id"] for row in remote}
    remap: dict[str, str] = {}
    for row in local_conn.execute("SELECT id, movie_id, platform, keyword FROM keywords"):
        movie_id = movie_id_remap.get(row["movie_id"], row["movie_id"])
        remote_id = remote_id_by_key.get((movie_id, row["platform"], row["keyword"]))
        if remote_id and remote_id != row["id"]:
            remap[row["id"]] = remote_id
    if remap:
        logger.info("keyword_ids_remapped", count=len(remap))
    return remap


async def _insert_batch(
    d1_query,
    table: str,
    columns: list[str],
    rows: list[sqlite3.Row],
    movie_id_remap: dict[str, str],
    keyword_id_remap: dict[str, str],
) -> int:
    if not rows:
        return 0
    placeholders = "(" + ", ".join("?" for _ in columns) + ")"
    sql = f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES " + ", ".join([placeholders] * len(rows))
    params: list[Any] = []
    for row in rows:
        params.extend(_row_values(row, columns, movie_id_remap, keyword_id_remap))
    result = await d1_query(sql, params)
    if result is None:
        if len(rows) > 1:
            inserted = 0
            for row in rows:
                inserted += await _insert_batch(
                    d1_query, table, columns, [row], movie_id_remap, keyword_id_remap
                )
            return inserted
        logger.warning("row_skipped_fk", table=table, row_id=rows[0]["id"])
        return 0
    return len(rows)


async def _push_new_rows(
    d1_query,
    local_conn: sqlite3.Connection,
    table: str,
    movie_id_remap: dict[str, str] | None = None,
    keyword_id_remap: dict[str, str] | None = None,
) -> int:
    columns = _table_columns(local_conn, table)
    batch_size = max(1, _MAX_PARAMS_PER_BATCH // len(columns))
    remote_ids = await _remote_ids(d1_query, table)
    local_rows = local_conn.execute(f"SELECT * FROM {table}").fetchall()
    new_rows = [r for r in local_rows if r["id"] not in remote_ids]
    movie_remap = movie_id_remap or {}
    keyword_remap = keyword_id_remap or {}
    if table == "keywords" and keyword_remap:
        new_rows = [r for r in new_rows if r["id"] not in keyword_remap]

    logger.info(
        "table_diffed",
        table=table,
        local_total=len(local_rows),
        already_remote=len(remote_ids),
        new=len(new_rows),
        batch_size=batch_size,
    )

    pushed = 0
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]
        pushed += await _insert_batch(d1_query, table, columns, batch, movie_remap, keyword_remap)
        logger.info("table_batch_pushed", table=table, pushed_so_far=pushed, of=len(new_rows))
    return pushed


async def push() -> None:
    settings.db_mode = "remote"  # see module docstring - writes target real D1, reads come from the local file directly
    from app.services.d1 import d1_query  # imported after forcing remote, not at module load

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to push to "
            "the real D1 - this script has nothing to write to without them."
        )

    local_path = Path(settings.local_db_path)
    if not local_path.exists():
        raise RuntimeError(f"No local mirror at {local_path} - nothing to push.")

    local_conn = sqlite3.connect(local_path)
    local_conn.row_factory = sqlite3.Row
    try:
        movies_pushed = await _push_new_rows(d1_query, local_conn, "movies")
        movie_id_remap = await _movie_id_remap(d1_query, local_conn)
        keyword_id_remap = await _keyword_id_remap(d1_query, local_conn, movie_id_remap)
        keywords_pushed = await _push_new_rows(
            d1_query, local_conn, "keywords", movie_id_remap, keyword_id_remap
        )
        keyword_id_remap = await _keyword_id_remap(d1_query, local_conn, movie_id_remap)
        posts_pushed = await _push_new_rows(
            d1_query, local_conn, "posts", movie_id_remap, keyword_id_remap
        )
        comments_pushed = await _push_new_rows(d1_query, local_conn, "comments")
    finally:
        local_conn.close()

    logger.info(
        "push_finished",
        telegram=True,
        movies_pushed=movies_pushed,
        keywords_pushed=keywords_pushed,
        posts_pushed=posts_pushed,
        comments_pushed=comments_pushed,
    )


if __name__ == "__main__":
    asyncio.run(push())
