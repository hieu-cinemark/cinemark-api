"""Chạy tay lượt AI tìm thêm câu tìm kiếm Facebook/Threads (app/services/discovery/query_discovery.py) - bình thường scheduler chạy
mỗi ngày ở settings.hashtag_discovery_time. (Phần AI duyệt hashtag TikTok đã bỏ 17/11/2026.)

Cách dùng:
    python -m scripts.jobs.discover_hashtags --dry-run            # hỏi AI, in quyết định, không ghi gì
    python -m scripts.jobs.discover_hashtags --movie-id movie_...
    python -m scripts.jobs.discover_hashtags
"""

from __future__ import annotations

import argparse
import asyncio

from app.services.discovery.query_discovery import run_query_discovery

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Ask AI and log decisions, write nothing")
    parser.add_argument("--movie-id", help="Only this movie")
    args = parser.parse_args()
    print("queries:", asyncio.run(run_query_discovery(dry_run=args.dry_run, movie_id=args.movie_id)))
