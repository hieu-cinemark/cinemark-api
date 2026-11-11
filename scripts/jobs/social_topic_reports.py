"""Tự động tạo báo cáo nhóm bình luận AI + tên cảnh cho các phim đang / vừa chiếu (2026-11-09), chạy hằng đêm trên VPS
(deploy/systemd/cinemark-reports.timer). Mỗi phim chạy đủ chuỗi:

    embed_comments --movie-id -> cluster_film_comments -> name_clusters -> summarize_clusters -> publish_clusters -> name_scenes

Chọn phim: đang bật, đã khởi chiếu trong WINDOW_DAYS ngày; phim chưa có báo cáo trước, rồi tới báo cáo cũ nhất. Báo cáo
được làm lại sau FRESH_DAYS_NEW ngày với phim ra rạp chưa quá NEW_FILM_DAYS ngày, sau FRESH_DAYS_OLD ngày với phim cũ hơn.
Mỗi lượt tối đa --max-films phim (lần đầu trên VPS phải embed toàn bộ bình luận của phim - phim lớn mất cả giờ trên CPU).

    python -m scripts.jobs.social_topic_reports --dry-run
    python -m scripts.jobs.social_topic_reports --max-films 2
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select

from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import comment_cluster_topics
from app.repositories.d1.tables import movies as movies_table

WINDOW_DAYS = 60
NEW_FILM_DAYS = 21
FRESH_DAYS_NEW = 3
FRESH_DAYS_OLD = 7
STEP_TIMEOUT_SECONDS = 2 * 3600


async def pick_films(max_films: int) -> list[dict]:
    today = datetime.now(tz=UTC).date()
    m = movies_table.alias("m")
    films = await d1_exec(
        select(m.c.id, m.c.title, func.substr(m.c.released_at, 1, 10).label("released")).where(
            m.c.enabled == 1,
            m.c.released_at.is_not(None),
            func.substr(m.c.released_at, 1, 10).between(
                (today - timedelta(days=WINDOW_DAYS)).isoformat(), today.isoformat()
            ),
        ),
        quiet=True,
    )
    done = (
        await d1_exec(
            select(
                comment_cluster_topics.c.movie_id,
                func.max(comment_cluster_topics.c.generated_at).label("g"),
            ).group_by(comment_cluster_topics.c.movie_id),
            quiet=True,
        )
        or []
    )
    generated = {row["movie_id"]: str(row["g"])[:10] for row in done}
    due = []
    for film in films or []:
        age = (today - date.fromisoformat(film["released"])).days
        fresh = FRESH_DAYS_NEW if age <= NEW_FILM_DAYS else FRESH_DAYS_OLD
        last = generated.get(film["id"])
        if last and (today - date.fromisoformat(last)).days < fresh:
            continue
        due.append({**film, "last": last})
    due.sort(key=lambda film: (film["last"] is not None, film["last"] or ""))
    return due[:max_films]


def run_step(module: str, *args: str) -> bool:
    started = time.monotonic()
    result = subprocess.run([sys.executable, "-m", module, *args], timeout=STEP_TIMEOUT_SECONDS, check=False)
    print(
        f"  {module.rsplit('.', 1)[-1]}: {'ok' if result.returncode == 0 else f'lỗi {result.returncode}'} ({time.monotonic() - started:.0f}s)",
        flush=True,
    )
    return result.returncode == 0


async def main(args: argparse.Namespace) -> None:
    films = await pick_films(args.max_films)
    print(f"{len(films)} phim cần báo cáo: {', '.join(f['title'] for f in films) or '-'}", flush=True)
    if args.dry_run:
        return
    for film in films:
        print(f"=== {film['title']}", flush=True)
        steps = [
            ("scripts.jobs.embed_comments", "--movie-id", film["id"]),
            ("scripts.tools.cluster_film_comments", "--movie-id", film["id"], "--k", "30"),
            ("scripts.tools.name_clusters", "--movie-id", film["id"], "--title", film["title"]),
            ("scripts.tools.summarize_clusters", "--movie-id", film["id"], "--title", film["title"]),
            ("scripts.tools.publish_clusters", "--movie-id", film["id"]),
        ]
        for step in steps:
            if not run_step(*step):
                break
        # Tên cảnh độc lập với nhóm bình luận: chỉ gọi AI cho cảnh mới (cảnh đã có tên thì bỏ qua).
        run_step("scripts.tools.name_scenes", "--movie-id", film["id"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-films", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    asyncio.run(main(parser.parse_args()))
