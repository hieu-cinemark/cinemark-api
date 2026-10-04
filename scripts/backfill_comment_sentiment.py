"""Phân loại cảm xúc cho các comment mà lượt quét của ingest consumer không tới được
(app/workers/ingest_consumer/sentiment_sweep.py chỉ xem 48 giờ gần nhất) - hàng tồn
cũ hơn, hoặc các dòng mà Kira cứ lỗi mãi. Cùng đường code gom lô Kira như lượt quét.
Chỉ đụng tới các dòng có sentiment IS NULL, nên chạy lại an toàn - một lượt chạy dở
chỉ để phần còn lại cho lượt sau.

Cách dùng:
    python -m scripts.backfill_comment_sentiment
    python -m scripts.backfill_comment_sentiment --platform facebook
    python -m scripts.backfill_comment_sentiment --limit 50 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

from app.core.logging import get_logger
from app.workers.ingest_consumer.sentiment_sweep import classify_pending

logger = get_logger(__name__)

PAGE_SIZE = 200
PAUSE_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
MAX_FAILED_PAGES = 5


async def backfill(platform: str | None, limit: int | None, dry_run: bool) -> None:
    logger.info("backfill_sentiment_started", platform=platform, limit=limit, dry_run=dry_run)
    totals: Counter[str] = Counter()
    given_up: set[str] = set()
    failed_pages = 0
    while limit is None or totals["selected"] < limit:
        page = PAGE_SIZE if limit is None else min(PAGE_SIZE, limit - totals["selected"])
        result = await classify_pending(limit=page, platform=platform, exclude=given_up, dry_run=dry_run)
        failed_ids = result.pop("failed_ids")
        totals.update(result)
        logger.info("backfill_sentiment_page", **result, totals=dict(totals))
        if not result["selected"] or dry_run:
            break  # không còn gì (chạy thử không ghi gì, nên sẽ lại chọn đúng trang đó)
        if result["classified"]:
            # Kira đang trả lời: comment nào nó không gắn nhãn được thì bỏ qua trong phần còn lại
            # của lượt chạy này thay vì bị chọn lại mãi.
            given_up.update(failed_ids)
            failed_pages = 0
        else:
            # Cả trang đều lỗi - là do Kira sập/bị giới hạn, không phải do các comment này.
            failed_pages += 1
            if failed_pages >= MAX_FAILED_PAGES:
                logger.error("backfill_sentiment_aborted", reason="bee_failing", failed_pages=failed_pages)
                break
        await asyncio.sleep(min(PAUSE_SECONDS * 2**failed_pages, MAX_BACKOFF_SECONDS))
    logger.info("backfill_sentiment_finished", **totals, dry_run=dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", help="Only backfill this platform")
    parser.add_argument("--limit", type=int, help="Max rows to process this run")
    parser.add_argument("--dry-run", action="store_true", help="Classify one page and log it, write nothing")
    args = parser.parse_args()
    asyncio.run(backfill(args.platform, args.limit, args.dry_run))
