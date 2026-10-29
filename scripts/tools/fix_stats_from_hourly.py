"""Sửa một lần stats_platform_daily cho các ngày mà lượt đối soát 28/10/2026 đã ghi đè bằng số lượt CẬP NHẬT (đếm theo
scraped_at) thay vì số bài/comment MỚI. Lấy số đúng từ bộ đếm theo giờ trong Redis (chỉ cộng dòng mới, giữ 72 giờ) - nên
phải chạy khi các giờ của những ngày đó còn trong Redis. Chỉ sửa bảng theo nền tảng; bộ đếm Redis không có chiều từ khoá.

    cd /opt/cinemark-api && .venv/bin/python -m scripts.tools.fix_stats_from_hourly --days 2026-10-27 2026-10-28
    (thêm --dry-run để chỉ in số, không ghi)
"""

from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict

from app.clients.redis import get_redis_client
from app.services.reports.stats_summary import _HOURLY_KEY_PREFIX, _as_int, _q, ensure_stats_tables


async def daily_from_hourly(day: str) -> tuple[dict[str, dict[str, int]], int]:
    """Tổng bài/comment mới theo nền tảng của một ngày UTC, và số giờ có dữ liệu trong Redis."""
    client = get_redis_client()
    pipe = client.pipeline()
    for hour in range(24):
        pipe.hgetall(f"{_HOURLY_KEY_PREFIX}{day}T{hour:02d}")
    totals: dict[str, dict[str, int]] = defaultdict(lambda: {"posts": 0, "comments": 0})
    hours_found = 0
    for fields in await pipe.execute():
        if fields:
            hours_found += 1
        for raw_field, raw_value in (fields or {}).items():
            field = raw_field.decode() if isinstance(raw_field, bytes) else str(raw_field)
            platform, _, metric = field.partition(":")
            if metric in ("posts", "comments"):
                totals[platform][metric] += _as_int(raw_value)
    return dict(totals), hours_found


async def main(days: list[str], dry_run: bool) -> None:
    await ensure_stats_tables()
    for day in days:
        totals, hours_found = await daily_from_hourly(day)
        if hours_found < 24:
            print(f"{day}: chỉ còn {hours_found}/24 giờ trong Redis - bỏ qua, số sẽ thiếu")
            continue
        for platform, counts in sorted(totals.items()):
            print(f"{day} {platform}: posts={counts['posts']} comments={counts['comments']}")
            if dry_run:
                continue
            await _q(
                """
                INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
                VALUES (?, ?, ?, ?, NULL)
                ON CONFLICT(day, platform) DO UPDATE SET posts = excluded.posts, comments = excluded.comments
                """,
                [day, platform, counts["posts"], counts["comments"]],
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", nargs="+", required=True, help="Ngày UTC dạng YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.days, args.dry_run))
