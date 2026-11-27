"""Đối chiếu bảng tổng hợp bài do cinemark-api tính (movie_post_daily_shadow) với bảng của Worker (movie_post_daily) - bước kiểm
tra trước khi chuyển hẳn việc tính sang cinemark-api (app/services/stats/rollup/post_rollup.py).

    python -m scripts.jobs.compare_post_rollup               # 14 ngày đăng gần nhất, mọi seen_age
    python -m scripts.jobs.compare_post_rollup --days 30 --current-only
    python -m scripts.jobs.compare_post_rollup --compute 2026-11-20   # tính ngay một ngày vào bảng shadow rồi so

Hai bên tính ở hai thời điểm khác nhau, nên với ngày đăng gần đây chênh lệch nhỏ là bình thường (bài/bản chụp mới về giữa hai lần
tính). Ngày đăng cũ (đã ổn định) phải khớp đúng. Mỗi ngày in thời điểm tính của hai bên để đọc chênh lệch cho đúng.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime

from app.clients.d1 import d1_query
from app.repositories.d1 import post_rollup as repo
from app.repositories.d1.post_rollup import TABLES
from app.services.stats.rollup.helper import DAY_MS, day_label
from app.services.stats.rollup.post_rollup import compute_and_write

FIELDS = ("posts", "discussions", "shares", "views")


async def _rows(table: str, start: str, end: str, current_only: bool) -> dict[tuple, dict]:
    where = "day >= ?1 AND day <= ?2" + (" AND seen_age = -1" if current_only else "")
    rows = await d1_query(
        f"SELECT day, seen_age, movie_id, platform, organic, {', '.join(FIELDS)} FROM {table} WHERE {where}",
        [start, end],
        timeout=120.0,
    )
    if rows is None:
        raise SystemExit(f"Không đọc được {table}")
    return {(r["day"], r["seen_age"], r["movie_id"], r["platform"], r["organic"]): r for r in rows}


async def _computed(table: str, start: str, end: str) -> dict[str, int]:
    rows = await d1_query(f"SELECT day, computed_at FROM {table} WHERE day >= ?1 AND day <= ?2", [start, end]) or []
    return {r["day"]: int(r["computed_at"]) for r in rows}


def _at(ms: int | None) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%d/%m %H:%M UTC") if ms else "-"


async def main() -> None:
    """Tuỳ chọn tính ngay một ngày vào bảng shadow, rồi so từng dòng hai bảng và in tóm tắt theo ngày kèm ví dụ lệch."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--current-only", action="store_true", help="chỉ so seen_age = -1 (số hiện tại)")
    parser.add_argument("--compute", metavar="YYYY-MM-DD", help="tính ngay ngày này vào bảng shadow trước khi so")
    args = parser.parse_args()

    if args.compute:
        await repo.ensure_schema("shadow")
        written = await compute_and_write("shadow", args.compute, int(time.time() * 1000))
        print(f"Đã tính {args.compute} vào bảng shadow: {written} dòng")

    now = int(time.time() * 1000)
    end = args.compute or day_label(now)
    start = day_label(now - (args.days - 1) * DAY_MS) if not args.compute else args.compute
    shadow_daily, shadow_days = TABLES["shadow"]
    live_daily, live_days = TABLES["live"]
    shadow, live = await asyncio.gather(
        _rows(shadow_daily, start, end, args.current_only), _rows(live_daily, start, end, args.current_only)
    )
    at_shadow, at_live = await asyncio.gather(_computed(shadow_days, start, end), _computed(live_days, start, end))

    summary = {"exact": 0, "differ": 0, "only_python": 0, "only_worker": 0}
    per_day: dict[str, dict[str, int]] = {}
    samples: list[dict] = []
    for key in sorted(set(shadow) | set(live)):
        day = key[0]
        bucket = per_day.setdefault(day, {"exact": 0, "differ": 0, "only_python": 0, "only_worker": 0})
        a, b = shadow.get(key), live.get(key)
        if a is None or b is None:
            kind = "only_worker" if a is None else "only_python"
        else:
            kind = "exact" if all(int(a[f] or 0) == int(b[f] or 0) for f in FIELDS) else "differ"
        summary[kind] += 1
        bucket[kind] += 1
        if kind != "exact" and len(samples) < 15:
            samples.append({"key": key, "python": {f: (a or {}).get(f) for f in FIELDS}, "worker": {f: (b or {}).get(f) for f in FIELDS}})

    print(f"Khoảng ngày đăng {start} -> {end}" + (" (chỉ seen_age=-1)" if args.current_only else ""))
    for day in sorted(per_day, reverse=True):
        counts = per_day[day]
        print(f"  {day}  khớp={counts['exact']:4d}  lệch={counts['differ']:3d}  chỉ-Python={counts['only_python']:3d}  "
              f"chỉ-Worker={counts['only_worker']:3d}  | tính lúc Python {_at(at_shadow.get(day))}, Worker {_at(at_live.get(day))}")
    print("Tổng:", json.dumps(summary))
    for sample in samples:
        print("  ví dụ lệch:", json.dumps(sample, ensure_ascii=False, default=str))


if __name__ == "__main__":
    asyncio.run(main())
