"""Dựng bảng movie_comment_hourly cho các ngày cũ (Attack Detection, dấu hiệu A cần mức thường của 14 ngày trước).

Nhịp scheduler (scheduler/attack.py) chỉ tính lại 3 giờ gần nhất; chạy script này một lần cho quá khứ. Từng ngày một, nghỉ
--pause giây giữa các ngày - chạy ban đêm để D1 không quá tải (docs/review-angles-va-attack.md mục 3).

    python -m scripts.tools.backfill_comment_hourly --days 30
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta

from app.repositories.d1.comment_hourly import rebuild_hours


async def run(days: int, pause: float) -> None:
    end = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    for back in range(days, 0, -1):
        start = end - timedelta(days=back)
        ok = await rebuild_hours(start, start + timedelta(days=1))
        print(f"{start.date()} {'ok' if ok else 'LỖI'}", flush=True)
        await asyncio.sleep(pause)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--pause", type=float, default=5.0)
    args = parser.parse_args()
    asyncio.run(run(args.days, args.pause))
