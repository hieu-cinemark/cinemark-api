"""Chụp ngay quy mô (tổng video / lượt xem) của mọi hashtag TikTok đang theo dõi, không chờ job hằng ngày lúc 06:00.

    python -m scripts.jobs.snapshot_hashtag_stats

Mỗi hashtag một request Omar. Tạo bảng tiktok_hashtag_stats nếu chưa có; chạy lại trong ngày thì ghi đè bản chụp của ngày đó."""

import asyncio
import json

from app.services.omar.omar_hashtag_stats import snapshot_all


async def main() -> None:
    print(json.dumps(await snapshot_all(), ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
