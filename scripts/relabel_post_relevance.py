"""Gán lại nhãn độ liên quan Kira cho các bài đã vào D1 mà Kira chưa kết luận (relevance_confidence IS NULL): lúc
Kira tắt, lỗi/504, vượt hạn mức ngày, hoặc khi ingest chạy với KIRA_INGEST_RELEVANCE=false (ingest không chờ Kira).
Cùng bộ gom lô (app/ai/tasks/post_relevance.py) và cùng logic chốt nhãn (relevance_rules.resolve_relevance) như
ingest consumer - khác duy nhất: bài Kira gán "not_related" được ẨN (relevance_label='not_related', keyword_match=0)
thay vì xoá, nên một nhãn sai vẫn sửa lại được.

Sau mỗi vòng relevance, gắn luôn cảm xúc/khía cạnh cho NỘI DUNG bài liên quan còn thiếu trong cùng khoảng ngày
(sentiment_sweep.classify_pending_posts) - lượt quét trong consumer chỉ xem 48 giờ gần nhất.

Chỉ đụng tới dòng relevance_confidence IS NULL / sentiment IS NULL, nên chạy lại an toàn.

Cách dùng:
    python -m scripts.relabel_post_relevance --dry-run --limit 20
    python -m scripts.relabel_post_relevance --since 2026-10-06
    python -m scripts.relabel_post_relevance --since 2026-10-06 --follow   # chạy nền mãi, bắt cả bài mới vào
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from app.ai.tasks.post_relevance import classify_post_relevance_kira
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import MIN_CONTENT_LENGTH, contains_keyword
from app.services.d1 import d1_query
from app.services.relevance_rules import resolve_relevance
from app.workers.ingest_consumer.main import _tracked_movies, _tracked_titles
from app.workers.ingest_consumer.sentiment_sweep import MAX_ATTEMPTS, classify_pending_posts

logger = get_logger(__name__)

PAGE_SIZE = 60  # 6 lô Kira (BATCH_SIZE 10), bộ gom lô chạy tối đa 3 lô song song
MAX_BACKOFF_SECONDS = 300.0
IDLE_SECONDS = 120.0


async def _select(since: str, movie_id: str | None, limit: int, exclude: set[str]) -> list[dict[str, Any]]:
    sql = (
        "SELECT p.id, p.movie_id, p.platform, p.content, k.keyword "
        "FROM posts p LEFT JOIN keywords k ON k.id = p.keyword_id "
        f"WHERE p.relevance_confidence IS NULL AND p.scraped_at >= ? AND length(trim(p.content)) >= {MIN_CONTENT_LENGTH}"
    )
    params: list[str | int] = [since]
    if movie_id:
        sql += " AND p.movie_id = ?"
        params.append(movie_id)
    sql += " ORDER BY p.scraped_at DESC LIMIT ?"
    params.append(limit + len(exclude))
    rows = await d1_query(sql, params)
    if rows is None:
        raise RuntimeError("relabel_select_failed")
    return [row for row in rows if row["id"] not in exclude][:limit]


async def _relabel(
    row: dict[str, Any], movies: dict[str, dict[str, Any]], titles: list[str], dry_run: bool
) -> str | None:
    """Nhãn cuối cùng đã ghi, hoặc None khi Kira không kết luận (để lại cho vòng sau)."""
    movie = movies.get(row["movie_id"])
    if movie is None:
        return None
    verdict = await classify_post_relevance_kira(
        content=row["content"], movie=movie, keyword=row["keyword"], platform=row["platform"], other_titles=titles
    )
    if verdict is None:
        return None
    has_keyword = contains_keyword(row["content"], row["keyword"])
    if verdict["label"] == "not_related":
        ai_relevant: bool | None = False
        label: str | None = "not_related"
    else:
        ai_relevant, label, _ = resolve_relevance(
            verdict["label"],
            row["content"],
            movie,
            has_keyword=has_keyword,
            strict=movie.get("slug") in settings.strict_relevance_movies,
        )
    keyword_match = int(ai_relevant if ai_relevant is not None else has_keyword)
    if not dry_run:
        await d1_query(
            "UPDATE posts SET relevance_label = ?, relevance_confidence = ?, relevance_labeled_at = ?, keyword_match = ? "
            "WHERE id = ? AND relevance_confidence IS NULL",
            [label, verdict["confidence"], datetime.now(tz=UTC).isoformat(), keyword_match, row["id"]],
        )
    else:
        logger.info("relabel_dry_run", post_id=row["id"], label=label, kira=verdict["label"], reason=verdict["reason"])
    return label or "none"


async def run(since: str, movie_id: str | None, limit: int | None, dry_run: bool, follow: bool) -> None:
    logger.info("relabel_started", since=since, movie_id=movie_id, limit=limit, dry_run=dry_run, follow=follow)
    totals: Counter[str] = Counter()
    attempts: Counter[str] = Counter()
    failed_pages = 0
    while True:
        exclude = {pid for pid, n in attempts.items() if n >= MAX_ATTEMPTS}
        page = PAGE_SIZE if limit is None else min(PAGE_SIZE, limit - totals["selected"])
        rows = await _select(since, movie_id, page, exclude) if page > 0 else []
        if rows:
            movies, titles = await _tracked_movies(), await _tracked_titles()
            labels = await asyncio.gather(*(_relabel(row, movies, titles, dry_run) for row in rows))
            done = [label for label in labels if label is not None]
            totals.update(selected=len(rows), labeled=len(done))
            totals.update(done)
            logger.info("relabel_page", selected=len(rows), labeled=len(done), totals=dict(totals))
            if dry_run:
                break
            if done:
                # Kira đang trả lời: bài nào nó không kết luận được thì chỉ thử lại vài lần.
                attempts.update(row["id"] for row, label in zip(rows, labels, strict=True) if label is None)
                failed_pages = 0
                continue
            # Cả trang đều lỗi - Kira tắt/sập/hết tiền, không phải do các bài này: chờ lâu dần rồi thử lại.
            failed_pages += 1
            delay = min(10 * 2**failed_pages, MAX_BACKOFF_SECONDS)
            logger.warning("relabel_kira_unavailable", failed_pages=failed_pages, retry_in=delay)
            if not follow and failed_pages >= 5:
                break
            await asyncio.sleep(delay)
            continue
        # Hết bài cần gán relevance: gắn cảm xúc cho nội dung bài liên quan còn thiếu trong cùng khoảng ngày.
        if not dry_run:
            sentiment = await classify_pending_posts(limit=PAGE_SIZE, since=datetime.fromisoformat(since))
            sentiment.pop("failed_ids")
            if sentiment["selected"]:
                logger.info("relabel_post_sentiment", **sentiment)
            if sentiment["classified"]:
                continue
        if not follow or (limit is not None and totals["selected"] >= limit):
            break
        await asyncio.sleep(IDLE_SECONDS)
    logger.info("relabel_finished", **totals, dry_run=dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="2026-10-06", help="Only posts scraped on/after this date (YYYY-MM-DD)")
    parser.add_argument("--movie-id", help="Only this movie's posts")
    parser.add_argument("--limit", type=int, help="Max posts to relabel this run")
    parser.add_argument("--dry-run", action="store_true", help="Classify one page and log it, write nothing")
    parser.add_argument("--follow", action="store_true", help="Keep running, picking up new posts as they arrive")
    args = parser.parse_args()
    asyncio.run(run(args.since, args.movie_id, args.limit, args.dry_run, args.follow))
