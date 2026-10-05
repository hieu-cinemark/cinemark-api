"""Dựng lại toàn bộ lake: dim_keywords (từ D1) -> silver (từ bronze) -> gold (từ silver).
Xem app/lake/dims.py, silver.py, gold.py.

.venv/bin/python -m scripts.build_silver            # ghi lên R2 silver/ + gold/
.venv/bin/python -m scripts.build_silver --local    # ghi ra ./.lake-local/{silver,gold} để xem trước
"""

from __future__ import annotations

import argparse
import asyncio
import time

from app.lake.constants import GOLD
from app.lake.dims import build_dim_keywords, fetch_keywords
from app.lake.gold import build_movie_daily
from app.lake.silver import SILVER, build_comments, build_posts, connect

LOCAL_OUT = ".lake-local/silver"
LOCAL_GOLD = ".lake-local/gold"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local", action="store_true", help=f"write to {LOCAL_OUT} / {LOCAL_GOLD} instead of R2")
    args = parser.parse_args()

    out = LOCAL_OUT if args.local else SILVER
    gold_out = LOCAL_GOLD if args.local else GOLD
    con = connect()

    # Dim trước: D1 lỗi thì dừng ngay, không phải chờ vài phút build posts rồi mới hỏng.
    rows = asyncio.run(fetch_keywords())
    print("dim_keywords:", build_dim_keywords(con, rows, out=out))

    started = time.perf_counter()
    counts = build_posts(con, out=out)
    print(f"silver posts -> {out}: {counts} in {time.perf_counter() - started:.1f}s")
    con.sql(
        """SELECT platform, count(*) AS posts, count(decision) AS with_decision,
                  count(*) FILTER (WHERE decision = 'dropped') AS dropped,
                  min(posted_at)::DATE AS oldest, max(scraped_at)::DATE AS last_crawl
           FROM posts GROUP BY platform ORDER BY platform"""
    ).show()

    # Sau posts: phần kiểm tra comment mồ côi đọc silver/posts vừa ghi.
    started = time.perf_counter()
    counts = build_comments(con, out=out)
    print(f"silver comments -> {out}: {counts} in {time.perf_counter() - started:.1f}s")
    con.sql(
        """SELECT platform, count(*) AS comments, count(parent_comment_id) AS replies,
                  min(posted_at)::DATE AS oldest, max(scraped_at)::DATE AS last_crawl
           FROM comments GROUP BY platform ORDER BY platform"""
    ).show()

    # Gold đọc lại silver vừa ghi.
    started = time.perf_counter()
    rows_written = build_movie_daily(con, silver=out, out=gold_out)
    print(f"gold movie_daily -> {gold_out}: {rows_written} rows in {time.perf_counter() - started:.1f}s")
    con.sql(
        """SELECT platform, count(DISTINCT movie_id) AS movies, min(day) AS first_day, max(day) AS last_day,
                  sum(new_posts) AS new_posts, sum(engagement_gained) AS engagement_gained,
                  sum(new_comments) AS new_comments
           FROM movie_daily GROUP BY platform ORDER BY platform"""
    ).show()


if __name__ == "__main__":
    main()
