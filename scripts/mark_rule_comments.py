"""Đánh dấu sentiment_source='rule' cho comment đã có nhãn mà thực ra không mang nội dung (sticker, emoji, tag tên,
tiếng cười, rỗng - app/ai/tasks/sentiment_rules.rule_sentiment).

Trước 19/10/2026 sentiment sweep gán nhãn luật nhưng không ghi nguồn, nên những dòng đó trông như nhãn Kira/Jev và
dashboard tính vào tỉ lệ cảm xúc. Chỉ đổi sentiment_source (giữ nguyên nhãn); chạy lại an toàn.

Cách dùng:
    python -m scripts.mark_rule_comments --dry-run      # đếm, không ghi
    python -m scripts.mark_rule_comments
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter

from app.ai.tasks.sentiment_rules import rule_sentiment
from app.core.logging import get_logger
from app.services.d1 import d1_query

logger = get_logger(__name__)

PAGE_SIZE = 2000


async def run(dry_run: bool) -> None:
    totals: Counter[str] = Counter()
    cursor = ""
    while True:
        rows = await d1_query(
            "SELECT id, message FROM comments WHERE id > ? AND sentiment IS NOT NULL AND sentiment_source IS NULL "
            "ORDER BY id LIMIT ?",
            [cursor, PAGE_SIZE],
            timeout=90.0,
        )
        if rows is None:
            raise RuntimeError("mark_rule_select_failed")
        if not rows:
            break
        cursor = str(rows[-1]["id"])
        ids = [row["id"] for row in rows if rule_sentiment(row.get("message")) is not None]
        totals["scanned"] += len(rows)
        totals["rule"] += len(ids)
        if ids and not dry_run:
            # Tra theo khoá chính qua json_each; "+" chặn planner dùng index khác (xem visobert_label._save).
            result = await d1_query(
                "UPDATE comments SET sentiment_source = 'rule' FROM (SELECT value AS id FROM json_each(?1)) AS j "
                "WHERE comments.id = j.id AND +comments.sentiment_source IS NULL",
                [json.dumps(ids)],
                timeout=60.0,
            )
            if result is None:
                logger.warning("mark_rule_update_failed", count=len(ids))
                totals["failed"] += len(ids)
        logger.info("mark_rule_page", dry_run=dry_run, **totals)
    logger.info("mark_rule_finished", dry_run=dry_run, **totals)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="chỉ đếm, không ghi")
    asyncio.run(run(parser.parse_args().dry_run))


if __name__ == "__main__":
    main()
