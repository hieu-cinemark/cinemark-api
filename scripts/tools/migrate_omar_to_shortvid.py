"""Chuyển dữ liệu cũ của Omar sang tên ShortVid (16/11/2026) - chạy MỘT lần trên VPS, lúc deploy bản ShortVid.

- D1 (cinemark-scraper): omar_video_stats, omar_search_queries, omar_comment_keywords, omar_comment_options -> shortvid_*.
  Chưa có bảng mới thì ALTER TABLE ... RENAME (giữ nguyên dữ liệu, tức thì). Bảng mới đã có (bản mới chạy trước, tự tạo bảng
  rỗng) thì chép dữ liệu sang rồi xoá bảng cũ. Index cũ idx_omar_* bị xoá (bản mới tự tạo idx_shortvid_*).
- Redis: mọi khoá <prefix>omar:* -> <prefix>shortvid:* (con trỏ hashtag, video đã gửi, hàng đợi bình luận, hiệu quả nguồn...).
  Khoá mới đã tồn tại thì giữ khoá mới, bỏ khoá cũ.
- Supabase omar_api_settings: KHÔNG chép (key Omar không dùng được cho ShortVid) - nhập key ShortVid ở dashboard › Settings.
- posts.comments_crawl_reason 'omar_*' (nhãn lịch sử): chỉ đổi khi có --reasons, vì UPDATE quét cả bảng posts trên D1.

Mặc định chỉ in việc sẽ làm; thêm --apply để làm thật:

    python -m scripts.tools.migrate_omar_to_shortvid
    python -m scripts.tools.migrate_omar_to_shortvid --apply
    python -m scripts.tools.migrate_omar_to_shortvid --apply --reasons
"""

from __future__ import annotations

import argparse
import asyncio

from app.clients.d1 import d1_query
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client

TABLES = ("video_stats", "search_queries", "comment_keywords", "comment_options")


async def _tables() -> set[str]:
    rows = await d1_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND (name LIKE 'omar_%' OR name LIKE 'shortvid_%')"
    )
    return {row["name"] for row in rows or []}


async def _count(table: str) -> int:
    rows = await d1_query(f'SELECT COUNT(*) AS n FROM "{table}"')
    return int((rows or [{}])[0].get("n") or 0)


async def migrate_d1(apply: bool) -> None:
    existing = await _tables()
    for suffix in TABLES:
        old, new = f"omar_{suffix}", f"shortvid_{suffix}"
        if old not in existing:
            print(f"D1  {old}: không có, bỏ qua")
            continue
        rows = await _count(old)
        if new not in existing:
            print(f"D1  {old} ({rows} dòng) -> RENAME TO {new}")
            if apply:
                await d1_query(f'ALTER TABLE "{old}" RENAME TO "{new}"')
        else:
            print(f"D1  {old} ({rows} dòng) -> chép vào {new} đã có ({await _count(new)} dòng), xoá {old}")
            if apply:
                await d1_query(f'INSERT OR IGNORE INTO "{new}" SELECT * FROM "{old}"')
                await d1_query(f'DROP TABLE "{old}"')
    indexes = await d1_query("SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_omar_%'") or []
    for row in indexes:
        print(f"D1  DROP INDEX {row['name']} (bản mới tự tạo idx_shortvid_*)")
        if apply:
            await d1_query(f'DROP INDEX IF EXISTS "{row["name"]}"')


async def migrate_redis(apply: bool) -> None:
    redis = get_redis_client()
    old_prefix, new_prefix = f"{REDIS_KEY_PREFIX}omar:", f"{REDIS_KEY_PREFIX}shortvid:"
    moved = skipped = 0
    async for raw in redis.scan_iter(match=f"{old_prefix}*", count=500):
        key = raw.decode() if isinstance(raw, bytes) else raw
        target = new_prefix + key[len(old_prefix) :]
        if apply:
            if await redis.renamenx(key, target):
                moved += 1
            else:
                await redis.delete(key)
                skipped += 1
        else:
            moved += 1
    verb = "đã chuyển" if apply else "sẽ chuyển"
    print(
        f"Redis {verb} {moved} khoá {old_prefix}* -> {new_prefix}*"
        + (f", bỏ {skipped} khoá cũ trùng khoá mới" if skipped else "")
    )


async def migrate_reasons(apply: bool) -> None:
    rows = await d1_query("SELECT COUNT(*) AS n FROM posts WHERE comments_crawl_reason LIKE 'omar\\_%' ESCAPE '\\'")
    print(f"D1  posts.comments_crawl_reason omar_* -> shortvid_*: {(rows or [{}])[0].get('n')} dòng")
    if apply:
        await d1_query(
            "UPDATE posts SET comments_crawl_reason = 'shortvid_' || substr(comments_crawl_reason, 6) "
            "WHERE comments_crawl_reason LIKE 'omar\\_%' ESCAPE '\\'"
        )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="làm thật (mặc định chỉ in việc sẽ làm)")
    parser.add_argument(
        "--reasons", action="store_true", help="đổi cả nhãn posts.comments_crawl_reason (quét cả bảng posts)"
    )
    args = parser.parse_args()
    await migrate_d1(args.apply)
    await migrate_redis(args.apply)
    if args.reasons:
        await migrate_reasons(args.apply)
    if not args.apply:
        print("Chạy thử - chưa đổi gì. Thêm --apply để làm thật.")


if __name__ == "__main__":
    asyncio.run(main())
