"""Replays rows from dropped_posts back through the current mapper/persist
logic - run this after fixing whatever caused the drop (missing mapper,
bad keyword_id, ...) to recover posts that would otherwise sit archived
forever. Safe to re-run: a row is only deleted after persist_post succeeds.

Usage:
    python -m scripts.replay_dropped_posts --reason mapper
    python -m scripts.replay_dropped_posts --reason mapper --platform tiktok
    python -m scripts.replay_dropped_posts --reason mapper --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.core.logging import get_logger
from app.services.d1 import d1_query, get_keyword, persist_post
from app.services.platforms import get_post_mapper

logger = get_logger(__name__)

async def replay(reason: str, platform: str | None, dry_run: bool) -> None:
    sql = "SELECT id, platform, keyword_id, raw_json FROM dropped_posts WHERE reason = ?"
    params: list[str] = [reason]
    
    if platform:
        sql += " AND platform = ?"
        params.append(platform)
        
    rows = await d1_query(sql, params) or []
    logger.info("replay_started", reason=reason, platform=platform, count=len(rows))
    
    replayed = skipped = 0
    for row in rows:
        payload = json.loads(row["raw_json"])
        mapper = get_post_mapper(row["platform"])
        if mapper is None:
            logger.warning("replay_skip_no_mapper", id=row["id"], platform=row["platform"])
            skipped += 1
            continue
        
        keyword_id = payload.get("keyword_id")
        keyword = await get_keyword(keyword_id, platform=row["platform"]) if keyword_id else None
        
        if keyword is None:
            logger.warning("replay_skip_no_keyword", id=row["id"], keyword_id=keyword_id)
            skipped += 1
            continue
        
        if dry_run:
            logger.info("replay_dry_run_would_persist", id=row["id"], platform=row["platform"])
            replayed += 1
            continue
        
        draft = mapper(payload)
        ok = await persist_post(
            movie_id=keyword["movie_id"],
            keyword_id=keyword_id,
            keyword=keyword["keyword"],
            platform=row["platform"],
            draft=draft
        )
        if not ok:
            # persist_post already logged why (D1 still down, most likely for
            # a d1_write_failed row) - leave the row in dropped_posts so a
            # later re-run can pick it back up, instead of losing it here too.
            logger.warning("replay_skip_persist_failed", id=row["id"], platform=row["platform"])
            skipped += 1
            continue
        await d1_query("DELETE FROM dropped_posts WHERE id = ?", [row["id"]])
        replayed += 1
        
    logger.info("replay_finished", replayed=replayed, skipped=skipped, dry_run=dry_run)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reason", required=True, choices=["mapper", "missing_keyword_id", "unknown_keyword_id", "d1_write_failed"]
    )
    parser.add_argument("--platform", help="Only replay this platform")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen, don't write anything")
    args = parser.parse_args()
    asyncio.run(replay(args.reason, args.platform, args.dry_run))