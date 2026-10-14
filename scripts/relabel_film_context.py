"""Nâng lại các bài đang bị ẩn (keyword_match=0, relevance_label 'uncertain' hoặc NULL) trong N ngày gần nhất mà giờ
có tín hiệu phim theo relevance_rules.film_context_reason - dùng sau khi thêm tín hiệu mới (vd. "keyword_hashtag",
14/10/2026) để bài cũ không kẹt ở tab "Không liên quan". In số bài theo từng phim; với --write thì đặt keyword_match=1
và nâng 'uncertain' -> 'related' (đúng như ingest consumer làm với bài mới, xem resolve_relevance).

Không đụng bài 'not_related' (bộ phân loại hoặc hashtag_only đã quyết) và bài 'related'. Chỉ nâng nên chạy lại an toàn.
Phim "chặt" (settings.strict_relevance_movies) dùng đúng các cờ như ingest: không tín hiệu hashtag/tên người.

Cách dùng:
    python -m scripts.relabel_film_context               # dry run, 14 ngày
    python -m scripts.relabel_film_context --days 30 --random 30
    python -m scripts.relabel_film_context --write --reason keyword_hashtag --backup surfaced.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import settings
from app.services.d1 import d1_query
from app.services.relevance_rules import film_context_reason
from app.workers.ingest_consumer.main import _tracked_movies

PLATFORMS = ("tiktok", "facebook", "threads")
PAGE_SIZE = 500
ROWS_PER_UPDATE = 50


async def _surface(ids: list[str], now: str) -> None:
    for i in range(0, len(ids), ROWS_PER_UPDATE):
        batch = ids[i : i + ROWS_PER_UPDATE]
        placeholders = ", ".join("?" for _ in batch)
        await d1_query(
            "UPDATE posts SET keyword_match = 1, relevance_labeled_at = ?, "
            "relevance_label = CASE WHEN relevance_label = 'uncertain' THEN 'related' ELSE relevance_label END "
            f"WHERE keyword_match = 0 AND (relevance_label = 'uncertain' OR relevance_label IS NULL) "
            f"AND id IN ({placeholders})",
            [now, *batch],
        )


async def run(
    days: int, write: bool, samples: int, random_samples: int, only: set[str] | None, backup: str | None
) -> None:
    movies = await _tracked_movies()
    since = (datetime.now(tz=UTC) - timedelta(days=days)).isoformat()
    platforms = ", ".join("?" for _ in PLATFORMS)
    scanned = 0
    hits: dict[str, list[str]] = defaultdict(list)
    reasons: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    flagged: list[tuple[str, str, str, str]] = []
    cursor = "9999"
    while True:
        rows: list[dict[str, Any]] | None = await d1_query(
            "SELECT id, movie_id, platform, content, scraped_at FROM posts "
            "WHERE keyword_match = 0 AND (relevance_label = 'uncertain' OR relevance_label IS NULL) "
            f"AND platform IN ({platforms}) AND scraped_at >= ? AND scraped_at < ? "
            "ORDER BY scraped_at DESC LIMIT ?",
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
            if movie is None:
                continue
            strict = movie.get("slug") in settings.strict_relevance_movies
            reason = film_context_reason(
                row.get("content"), movie, allow_title_hashtag=not strict, allow_names=not strict
            )
            if not reason or (only and reason not in only):
                continue
            title = movie["title"]
            caption = " ".join(str(row.get("content") or "").split())
            hits[title].append(row["id"])
            reasons[reason] += 1
            flagged.append((title, row["platform"], reason, caption[:160]))
            if len(examples[title]) < samples:
                examples[title].append(f"[{reason}] {caption[:130]}")
        if len(rows) < PAGE_SIZE:
            break

    total = sum(len(ids) for ids in hits.values())
    print(f"scanned {scanned} hidden posts ({', '.join(PLATFORMS)}) scraped since {since[:10]}")
    print(f"to surface: {total} | by reason: {dict(reasons)}")
    for title, ids in sorted(hits.items(), key=lambda item: -len(item[1])):
        print(f"  {len(ids):6d}  {title}")
        for example in examples[title]:
            print(f"            - {example}")
    if random_samples:
        print(f"\n{random_samples} random captions to surface:")
        for title, platform, reason, caption in random.sample(flagged, min(random_samples, len(flagged))):
            print(f"  [{platform}] {title} ({reason}): {caption}")
    if not write:
        print("dry run - pass --write to set keyword_match=1")
        return
    surfaced = [post_id for post_ids in hits.values() for post_id in post_ids]
    if backup:
        payload = {"surfaced_ids": surfaced, "reasons": dict(reasons), "since": since}
        with open(backup, "w") as fh:  # noqa: ASYNC230 - script chạy tay, ghi một lần trước khi UPDATE
            json.dump(payload, fh)
        print(f"backup of ids -> {backup}")
    now = datetime.now(tz=UTC).isoformat()
    await _surface(surfaced, now)
    print(f"done - surfaced {total} posts")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--write", action="store_true", help="áp dụng (mặc định chỉ dry run)")
    parser.add_argument("--samples", type=int, default=2, help="số caption ví dụ in ra cho mỗi phim")
    parser.add_argument(
        "--random", type=int, default=0, help="in thêm N caption ngẫu nhiên sẽ được nâng (soi độ chính xác)"
    )
    parser.add_argument("--reason", action="append", help="chỉ nâng bài có lý do này (lặp được), vd. keyword_hashtag")
    parser.add_argument("--backup", help="ghi danh sách id sẽ nâng ra file JSON trước khi --write (để hoàn tác)")
    args = parser.parse_args()
    asyncio.run(run(args.days, args.write, args.samples, args.random, set(args.reason or ()), args.backup))


if __name__ == "__main__":
    main()
