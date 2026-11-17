"""Attack Detection hằng giờ cho phim đang chiếu (docs/review-angles-va-attack.md mục 2).

Mỗi phim: đọc số chê theo giờ (16 ngày) + bình luận chê 48 giờ + embedding -> dấu hiệu A (spikes) và B (similar) ->
verdict.decide -> một dòng attack_signals. Không gọi AI (dấu hiệu E và câu tóm tắt chưa làm).

Lịch: scheduler/ai_jobs.py (AI_JOB_EVERY["attack_detect"]). Chạy tay:
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


async def _cli(args: argparse.Namespace) -> None:
    now = datetime.fromisoformat(args.at) if args.at else None
    result = await detect(args.movie_id, now)
    if not args.dry_run:
        print("ghi:", await store.save(result))
    for signal in result["signals"]:
        signal.get("detail", {}).pop("series", None)  # chuỗi 72 giờ dài, không in
    print(json.dumps(result, ensure_ascii=False, indent=2)[:6000])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--at", help="thời điểm tính (ISO, mặc định bây giờ) - để thử trên dữ liệu cũ")
    asyncio.run(_cli(parser.parse_args()))
