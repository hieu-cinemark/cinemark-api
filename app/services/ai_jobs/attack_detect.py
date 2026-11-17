"""Attack Detection hằng giờ cho phim đang chiếu (docs/review-angles-va-attack.md mục 2).

Mỗi phim: đọc số chê theo giờ (16 ngày) + bình luận chê 48 giờ + embedding -> dấu hiệu A (spikes) và B (similar) ->
verdict.decide -> một dòng attack_signals. Không gọi AI (dấu hiệu E và câu tóm tắt chưa làm).

Lịch: scheduler/ai_jobs.py (AI_JOB_EVERY["attack_detect"]). Chạy tay:
    python -m app.services.ai_jobs.attack_detect --backfill-days 30 --step-hours 6   # tính lại quá khứ, GHI D1 (chạy lại không trùng)
    python -m app.services.ai_jobs.attack_detect --all                               # mọi phim đang chiếu, GHI D1 (như một nhịp)
    python -m app.services.ai_jobs.attack_detect --movie-id <id> --dry-run           # in kết quả, không ghi
    python -m app.services.ai_jobs.attack_detect --movie-id <id> --dry-run --at 2026-11-05T14:00:00+00:00   # thử mốc cũ
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.logging import get_logger
from app.services.attack import data, store
from app.services.attack.similar import similar_signal
from app.services.attack.spikes import spike_signal
from app.services.attack.verdict import decide

logger = get_logger(__name__)

WINDOW_HOURS = 48


async def detect(movie_id: str, now: datetime | None = None) -> dict[str, Any]:
    """Kết quả một lần tính cho phim tại `now` (mặc định bây giờ) - chưa ghi."""
    now = (now or datetime.now(tz=UTC)).astimezone(UTC)
    start = now - timedelta(hours=WINDOW_HOURS)
    hours, events, negatives = await asyncio.gather(
        data.load_hours(movie_id, now), data.load_events(movie_id), data.load_negative_comments(movie_id, now)
    )
    vectors = await asyncio.to_thread(data.load_vectors, [r["id"] for r in negatives])
    signals = [
        spike_signal(hours, events, since_hour=data.vn_hour(start)),
        similar_signal(negatives, vectors),
    ]
    status, confidence, reasons = decide(signals, data.coverage(hours, negatives))
    return {
        "movie_id": movie_id,
        "computed_at": now.isoformat(timespec="seconds"),
        "window_start": start.isoformat(timespec="seconds"),
        "window_end": now.isoformat(timespec="seconds"),
        "status": status,
        "confidence": confidence,
        "reasons": reasons,
        "signals": [s.as_dict() for s in signals],
        "embedded": 0 if vectors is None else int((abs(vectors).sum(axis=1) > 0).sum()),
    }


async def run_tick() -> dict[str, Any]:
    """Một lượt cho mọi phim đang chiếu; một phim lỗi không chặn phim khác."""
    from app.services.relevance.trend_posts import showing_movies

    now = datetime.now(tz=UTC)
    counts: dict[str, int] = {}
    for movie in await showing_movies(now):
        try:
            result = await detect(movie["id"], now)
            if await store.save(result):
                counts[result["status"]] = counts.get(result["status"], 0) + 1
            if result["status"] == "coordinated":
                logger.warning(
                    "attack_coordinated", movie_id=movie["id"], title=movie.get("title"), reasons=result["reasons"]
                )
        except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
            logger.warning("attack_detect_failed", movie_id=movie["id"], error=str(exc)[:300])
    await store.prune(now)
    return counts


def backfill_points(start: datetime, end: datetime, step: timedelta, done: list[datetime]) -> list[datetime]:
    """Các mốc tính lại từ `start` tới `end` mỗi `step`, bỏ mốc đã có dòng cách nó dưới nửa `step` (chạy lại không tạo trùng)."""
    points, at = [], start
    while at <= end:
        if all(abs(at - d) >= step / 2 for d in done):
            points.append(at)
        at += step
    return points


async def backfill(
    days: float, step_hours: float, movie_id: str | None = None, *, dry_run: bool = False, pause: float = 3.0
) -> dict[str, int]:
    """Tính lại Attack Detection cho quá khứ: mỗi `step_hours` giờ trong `days` ngày qua, cho mọi phim đang chiếu ở mốc đó
    (hoặc một `movie_id`), ghi như một nhịp thật với computed_at = mốc đó. Cần movie_comment_hourly đã dựng đủ `days` + 16 ngày
    (scripts/tools/backfill_comment_hourly.py); dấu hiệu B chỉ có khi máy chạy có embedding của bình luận cũ."""
    from sqlalchemy import select

    from app.repositories.d1.sql import d1_exec
    from app.services.relevance.trend_posts import showing_movies

    end = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    step = timedelta(hours=step_hours)
    await store.ensure_table()
    t = store.attack_signals
    rows = (
        await d1_exec(select(t.c.movie_id, t.c.computed_at).where(t.c.computed_at >= start.isoformat()), timeout=60.0)
        or []
    )
    done: dict[str, list[datetime]] = {}
    for row in rows:
        done.setdefault(row["movie_id"], []).append(datetime.fromisoformat(row["computed_at"]))
    counts: dict[str, int] = {}
    at = start
    while at <= end:
        movies = [{"id": movie_id}] if movie_id else await showing_movies(at)
        for movie in movies:
            if not backfill_points(at, at, step, done.get(movie["id"], [])):
                counts["skipped"] = counts.get("skipped", 0) + 1
                continue
            try:
                result = await detect(movie["id"], at)
                if dry_run or await store.save(result):
                    counts[result["status"]] = counts.get(result["status"], 0) + 1
            except Exception as exc:  # noqa: BLE001 - một mốc lỗi không dừng cả lượt
                counts["failed"] = counts.get("failed", 0) + 1
                logger.warning("attack_backfill_failed", movie_id=movie["id"], at=at.isoformat(), error=str(exc)[:300])
            await asyncio.sleep(pause)  # D1 báo 7429 (quá tải) khi tính liền nhiều phim - nhường chỗ cho ingest
        print(at.isoformat(timespec="minutes"), counts, flush=True)
        at += step
    return counts


async def _cli(args: argparse.Namespace) -> None:
    if args.backfill_days:
        print(
            "xong:",
            await backfill(args.backfill_days, args.step_hours, args.movie_id, dry_run=args.dry_run, pause=args.pause),
        )
        return
    if args.all:
        print("đã ghi theo trạng thái:", await run_tick())  # đúng một lượt của scheduler: mọi phim đang chiếu, ghi D1
        return
    now = datetime.fromisoformat(args.at) if args.at else None
    result = await detect(args.movie_id, now)
    if not args.dry_run:
        print("ghi:", await store.save(result))
    for signal in result["signals"]:
        signal.get("detail", {}).pop("series", None)  # chuỗi 72 giờ dài, không in
    print(json.dumps(result, ensure_ascii=False, indent=2)[:6000])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", help="một phim (bắt buộc nếu không dùng --all)")
    parser.add_argument("--all", action="store_true", help="chạy ngay một lượt cho MỌI phim đang chiếu và GHI D1")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--backfill-days", type=float, help="tính lại cho N ngày qua (mọi phim đang chiếu ở từng mốc, hoặc --movie-id)"
    )
    parser.add_argument(
        "--step-hours", type=float, default=6.0, help="khoảng cách giữa các mốc khi --backfill-days (mặc định 6)"
    )
    parser.add_argument("--pause", type=float, default=3.0, help="giây nghỉ giữa hai lần tính khi --backfill-days")
    parser.add_argument("--at", help="thời điểm tính (ISO, mặc định bây giờ) - để thử trên dữ liệu cũ")
    parsed = parser.parse_args()
    if not parsed.all and not parsed.movie_id and not parsed.backfill_days:
        parser.error("cần --movie-id hoặc --all")
    asyncio.run(_cli(parsed))
