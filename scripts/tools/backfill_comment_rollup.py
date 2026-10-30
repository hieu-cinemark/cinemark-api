"""Nạp bảng tổng hợp bình luận theo phim x ngày lần đầu (hoặc dựng lại) cho N ngày gần nhất - xem
app/repositories/d1/comment_rollup.py. Lần đầu cũng tạo index comments(posted_at) (có thể mất cả phút).

    .venv/bin/python -m scripts.tools.backfill_comment_rollup --days 180
"""

from __future__ import annotations

import argparse
import asyncio
import time

from app.services.reports.comment_rollup import rebuild_days, vn_days


async def main(days: int) -> None:
    started = time.monotonic()
    result = await rebuild_days(vn_days(days))
    print(f"{result} trong {time.monotonic() - started:.0f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=180)
    asyncio.run(main(parser.parse_args().days))
