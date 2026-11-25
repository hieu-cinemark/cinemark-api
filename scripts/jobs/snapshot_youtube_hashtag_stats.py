"""Chụp ngay quy mô YouTube (tổng video / số kênh) của mọi hashtag phim, không chờ job hằng ngày lúc 06:00.

    python -m scripts.jobs.snapshot_youtube_hashtag_stats

Mỗi hashtag một GET trang youtube.com/hashtag/<tên>. Tạo bảng youtube_hashtag_stats nếu chưa có; chạy lại trong ngày thì ghi đè."""

import asyncio
import json

from app.services.youtube_hashtag_stats import snapshot_all


async def main() -> None:
    print(json.dumps(await snapshot_all(), ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
