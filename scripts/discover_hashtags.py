"""Chạy tay lượt AI tìm thêm hashtag TikTok (app/services/hashtag_discovery.py) và câu tìm kiếm Facebook/Threads
(app/services/query_discovery.py) - bình thường scheduler chạy mỗi ngày ở settings.hashtag_discovery_time.

Cách dùng:
    python -m scripts.discover_hashtags --dry-run            # hỏi Kira, in quyết định, không ghi gì
    python -m scripts.discover_hashtags --movie-id movie_...
    python -m scripts.discover_hashtags
    python -m scripts.discover_hashtags --only queries --dry-run   # chỉ câu tìm kiếm Facebook/Threads
"""

from __future__ import annotations

import argparse
import asyncio

from app.services.hashtag_discovery import run_discovery
from app.services.query_discovery import run_query_discovery

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Ask Kira and log decisions, write nothing")
    parser.add_argument("--movie-id", help="Only this movie")
    parser.add_argument("--only", choices=["hashtags", "queries"], help="Run just one of the two passes")
    args = parser.parse_args()
    if args.only != "queries":
        print("hashtags:", asyncio.run(run_discovery(dry_run=args.dry_run, movie_id=args.movie_id)))
    if args.only != "hashtags":
        print("queries:", asyncio.run(run_query_discovery(dry_run=args.dry_run, movie_id=args.movie_id)))
