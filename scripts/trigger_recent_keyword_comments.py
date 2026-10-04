"""Kích hoạt hàng loạt một lần: xếp hàng các request crawl comment cho các bài tương tác
cao nhất của những từ khoá đang bật được tạo gần đây nhất, trên cả 3 nền tảng.

Khác với lượt quét theo lịch hằng ngày (list_posts_needing_comments, vẫn chặn theo
"chưa có comment nào được lưu ở local"), script này còn bỏ qua bài có reply_count bằng
0 - số comment do chính nền tảng báo - để một lượt chạy hàng loạt không tốn một
session proxy/trình duyệt cho bài về cấu trúc không thể trả về gì. reply_count==0
không phân biệt được "thật sự bằng 0" với "lúc crawl không lấy được" (xem lịch sử của
platform_scraper.py với sự mơ hồ này), nhưng với một lần *chọn* hàng loạt chạy một
lần thì cái giá của việc bỏ nhầm một bài chỉ là "không phải lượt này" - rẻ hơn nhiều
so với cổng chặn cứng cho từng bài trước đây, thứ mà chính sự mơ hồ này đã là lý do
để bỏ đi.

Cách dùng:
  .venv/bin/python -m scripts.trigger_recent_keyword_comments
  .venv/bin/python -m scripts.trigger_recent_keyword_comments --keywords 15 --top-n 100
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

TOP_POSTS_SCAN = 5000


async def recent_enabled_keywords(limit: int) -> list[dict]:
    from app.services.d1 import d1_query

    rows = await d1_query(
        """
        SELECT k.id, k.platform, k.keyword, m.title AS movie_title
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.enabled = 1 AND m.enabled = 1
        ORDER BY k.created_at DESC
        LIMIT ?
        """,
        [limit],
    )
    return rows or []


async def posts_to_trigger(platform: str, keyword_id: str, top_n: int) -> list[dict]:
    from app.repositories.d1.posts import _ENGAGEMENT_SCORE_SQL, post_mentions_movie
    from app.services.d1 import d1_query

    rows = await d1_query(
        f"""
        SELECT p.id, p.external_id, p.url, p.content, p.reply_count, m.title AS movie_title,
               COALESCE(c.n, 0) AS comment_n
        FROM posts p
        LEFT JOIN movies m ON m.id = p.movie_id
        LEFT JOIN (SELECT post_id, COUNT(*) AS n FROM comments GROUP BY post_id) c ON c.post_id = p.id
        WHERE p.platform = ? AND p.keyword_id = ?
        ORDER BY {_ENGAGEMENT_SCORE_SQL} DESC
        LIMIT ?
        """,
        [platform, keyword_id, TOP_POSTS_SCAN],
    )
    mentioned = [row for row in (rows or []) if post_mentions_movie(row.get("content"), row.get("movie_title"))]
    selected = []
    for row in mentioned:
        if len(selected) >= top_n:
            break
        if row.get("comment_n"):
            continue  # đã có comment được lưu - không phải việc của lượt chạy này
        if not (row.get("reply_count") or 0):
            continue  # nền tảng báo 0 reply - bỏ qua, theo yêu cầu của lượt chạy này
        if not row.get("url"):
            continue
        selected.append(row)
    return selected


async def main(num_keywords: int, top_n: int) -> None:
    from app.clients.kafka import publish_comments_crawl_request, start_kafka_producer, stop_kafka_producer

    keywords = await recent_enabled_keywords(num_keywords)
    print(f"Recent enabled keywords: {len(keywords)}")
    for kw in keywords:
        print(f"  [{kw['platform']}] {kw['movie_title']} - {kw['keyword']}")

    await start_kafka_producer()
    try:
        totals = Counter()
        for kw in keywords:
            posts = await posts_to_trigger(kw["platform"], kw["id"], top_n)
            queued = 0
            for post in posts:
                ok = await publish_comments_crawl_request(
                    platform=kw["platform"],
                    post_external_id=post["external_id"],
                    post_url=post["url"],
                    bypass_drain=False,
                )
                if ok:
                    queued += 1
            totals[kw["platform"]] += queued
            print(f"  [{kw['platform']}] {kw['keyword']}: candidates={len(posts)} queued={queued}")
        print(f"\nTotal queued per platform: {dict(totals)}  (sum={sum(totals.values())})")
    finally:
        await stop_kafka_producer()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keywords", type=int, default=15, help="How many most-recent enabled keywords to cover")
    parser.add_argument("--top-n", type=int, default=100, help="Top-N posts by engagement per keyword")
    args = parser.parse_args()
    asyncio.run(main(args.keywords, args.top_n))
