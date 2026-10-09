"""Chạy tay lượt AI tìm thêm hashtag TikTok (app/services/hashtag_discovery.py) - bình thường scheduler chạy mỗi ngày
ở settings.hashtag_discovery_time.

Cách dùng:
    python -m scripts.discover_hashtags --dry-run            # hỏi Kira, in quyết định, không ghi gì
    python -m scripts.discover_hashtags --movie-id movie_...
    python -m scripts.discover_hashtags
"""

from __future__ import annotations

import argparse
import asyncio

from app.services.hashtag_discovery import run_discovery

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Ask Kira and log decisions, write nothing")
    parser.add_argument("--movie-id", help="Only this movie")
    args = parser.parse_args()
    print(asyncio.run(run_discovery(dry_run=args.dry_run, movie_id=args.movie_id)))
