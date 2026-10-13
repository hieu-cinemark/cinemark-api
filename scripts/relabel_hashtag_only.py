"""Tìm các bài TikTok/Facebook/Threads đang relevance_label='related' trong N ngày gần nhất mà chỉ nối với phim bằng
hashtag (relevance_rules.hashtag_only_mention - "... anh hùng💕 #traibuonnguoi #fyp"), in số bài theo từng phim; với
--write thì ẩn chúng (relevance_label='not_related', keyword_match=0 - KHÔNG xoá, vẫn xem lại/gán lại được).

Cùng ngưỡng với ingest consumer (relevance_rules.resolve_relevance): bài mà bộ phân loại gán "related" với
relevance_confidence >= HASHTAG_ONLY_MIN_CONFIDENCE được giữ. Chỉ hạ cấp related -> not_related nên chạy lại an toàn.

Cách dùng:
    python -m scripts.relabel_hashtag_only               # dry run, 30 ngày
    python -m scripts.relabel_hashtag_only --days 14 --samples 3
    python -m scripts.relabel_hashtag_only --write
"""

from __future__ import annotations

import argparse
import asyncio
import random
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from app.services.d1 import d1_query
from app.services.relevance_rules import HASHTAG_ONLY_MIN_CONFIDENCE, hashtag_only_mention
from app.workers.ingest_consumer.main import _tracked_movies

PLATFORMS = ("tiktok", "facebook", "threads")
PAGE_SIZE = 500
ROWS_PER_UPDATE = 50


async def _hide(ids: list[str], now: str) -> None:
    for i in range(0, len(ids), ROWS_PER_UPDATE):
        batch = ids[i : i + ROWS_PER_UPDATE]
        placeholders = ", ".join("?" for _ in batch)
        await d1_query(
            "UPDATE posts SET relevance_label = 'not_related', keyword_match = 0, relevance_labeled_at = ? "
            f"WHERE relevance_label = 'related' AND id IN ({placeholders})",
            [now, *batch],
        )


async def run(days: int, write: bool, samples: int, random_samples: int) -> None:
    movies = await _tracked_movies()
    since = (datetime.now(tz=UTC) - timedelta(days=days)).isoformat()
    platforms = ", ".join("?" for _ in PLATFORMS)
    scanned = 0
    hits: dict[str, list[str]] = defaultdict(list)
    confident: Counter[str] = Counter()
    per_platform: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    flagged: list[tuple[str, str, str]] = []
    cursor = "9999"
    while True:
        rows: list[dict[str, Any]] | None = await d1_query(
            "SELECT p.id, p.movie_id, p.platform, p.content, p.scraped_at, p.relevance_confidence, k.keyword "
            "FROM posts p LEFT JOIN keywords k ON k.id = p.keyword_id "
            f"WHERE p.relevance_label = 'related' AND p.platform IN ({platforms}) "
            "AND p.scraped_at >= ? AND p.scraped_at < ? ORDER BY p.scraped_at DESC LIMIT ?",
            [*PLATFORMS, since, cursor, PAGE_SIZE],
            timeout=60.0,
        )
        if rows is None:
            raise RuntimeError("select_failed")
        if not rows:
            break
        cursor = rows[-1]["scraped_at"]
        scanned += len(rows)
        for row in rows:
            movie = movies.get(row["movie_id"])
            if movie is None or not hashtag_only_mention(row.get("content"), movie, [row.get("keyword")]):
                continue
            title = movie["title"]
            if (row.get("relevance_confidence") or 0) >= HASHTAG_ONLY_MIN_CONFIDENCE:
                confident[title] += 1
                continue
            hits[title].append(row["id"])
            per_platform[row["platform"]] += 1
            flagged.append((title, row["platform"], " ".join(str(row.get("content") or "").split())[:160]))
            if len(examples[title]) < samples:
                examples[title].append(" ".join(str(row.get("content") or "").split())[:140])
        if len(rows) < PAGE_SIZE:
            break

    total = sum(len(ids) for ids in hits.values())
    print(f"scanned {scanned} related posts ({', '.join(PLATFORMS)}) scraped since {since[:10]}")
    print(f"hashtag-only to hide: {total} | by platform: {dict(per_platform)}")
    print(f"hashtag-only kept (confidence >= {HASHTAG_ONLY_MIN_CONFIDENCE}): {sum(confident.values())}")
    for title, ids in sorted(hits.items(), key=lambda item: -len(item[1])):
        print(f"  {len(ids):6d}  {title}" + (f"  (+{confident[title]} confident kept)" if confident[title] else ""))
        for example in examples[title]:
            print(f"            - {example}")
    if random_samples:
        print(f"\n{random_samples} random flagged captions:")
        for title, platform, caption in random.sample(flagged, min(random_samples, len(flagged))):
            print(f"  [{platform}] {title}: {caption}")
    if not write:
        print("dry run - pass --write to set relevance_label='not_related'")
        return
    now = datetime.now(tz=UTC).isoformat()
    await _hide([post_id for ids in hits.values() for post_id in ids], now)
    print(f"done - hid {total} posts")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--write", action="store_true", help="áp dụng (mặc định chỉ dry run)")
    parser.add_argument("--samples", type=int, default=2, help="số caption ví dụ in ra cho mỗi phim")
    parser.add_argument(
        "--random", type=int, default=0, help="in thêm N caption ngẫu nhiên bị gắn cờ (soi độ chính xác)"
    )
    args = parser.parse_args()
    asyncio.run(run(args.days, args.write, args.samples, args.random))


if __name__ == "__main__":
    main()
