"""One-off: pushes movies -> keywords -> posts (not comments) to remote D1.

Reuses push_local_data_to_remote.py's own helper functions - this just
calls its push() sequence minus the final comments step, for the case
where only a posts backfill is wanted this run (see that script's own
push() for the full movies+keywords+posts+comments version).

    python -m scripts.push_posts_only
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.logging import get_logger
from scripts.push_local_data_to_remote import _keyword_id_remap, _movie_id_remap, _push_new_rows

logger = get_logger(__name__)


async def push() -> None:
    settings.db_mode = "remote"
    from app.services.d1 import d1_query

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError("CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set in .env.")

    local_path = Path(settings.local_db_path)
    if not local_path.exists():
        raise RuntimeError(f"No local mirror at {local_path} - nothing to push.")

    local_conn = sqlite3.connect(local_path)
    local_conn.row_factory = sqlite3.Row
    try:
        movies_pushed = await _push_new_rows(d1_query, local_conn, "movies")
        movie_id_remap = await _movie_id_remap(d1_query, local_conn)
        keyword_id_remap = await _keyword_id_remap(d1_query, local_conn, movie_id_remap)
        keywords_pushed = await _push_new_rows(d1_query, local_conn, "keywords", movie_id_remap, keyword_id_remap)
        keyword_id_remap = await _keyword_id_remap(d1_query, local_conn, movie_id_remap)
        posts_pushed = await _push_new_rows(d1_query, local_conn, "posts", movie_id_remap, keyword_id_remap)
    finally:
        local_conn.close()

    logger.info(
        "push_posts_only_finished",
        telegram=True,
        movies_pushed=movies_pushed,
        keywords_pushed=keywords_pushed,
        posts_pushed=posts_pushed,
    )


if __name__ == "__main__":
    asyncio.run(push())
