"""Chạy ngay một lượt chọn bài + xếp crawl comment (app/services/comment_planner.py) thay vì chờ lượt mỗi giờ của
scheduler - gồm lớp AI chọn bài đáng cào (app/services/comment_value.py).

Cách dùng:
    python -m scripts.run_comment_round                     # mọi nền tảng có comment crawl
    python -m scripts.run_comment_round --platform facebook --sample
"""

from __future__ import annotations

import argparse
import asyncio

from app.services.comment_planner import run_comment_round
from app.services.platforms import COMMENT_CRAWL_PLATFORMS


async def main(platforms: list[str], hot_per_movie: int, sample: bool) -> None:
    for platform in platforms:
        print(platform, await run_comment_round(platform, hot_per_movie=hot_per_movie, with_sample=sample))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=sorted(COMMENT_CRAWL_PLATFORMS), help="Only this platform")
    parser.add_argument("--hot-per-movie", type=int, default=15)
    parser.add_argument("--sample", action="store_true", help="Also run the stratified sample layer")
    args = parser.parse_args()
    asyncio.run(
        main([args.platform] if args.platform else sorted(COMMENT_CRAWL_PLATFORMS), args.hot_per_movie, args.sample)
    )
