# scripts/tools/review_angles_posted_at.py
"""Bù postedAt cho ref cũ trong state.json của Review Angles (đọc D1 theo id, lô 40). --dry-run chỉ đếm."""

import argparse
import asyncio

from sqlalchemy import select

from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import movies, posts
from app.services.ai_jobs.review_angles import _read_json, _root, _write_json


async def fix(movie_id: str, dry_run: bool) -> tuple[int, int]:
    key = f"{_root(movie_id)}/state.json"
    state = await _read_json(key)
    if not state:
        return 0, 0
    missing = sorted({r["id"] for a in state["angles"] for r in a["refs"] if not r.get("postedAt")})
    if dry_run or not missing:
        return len(missing), 0
    found: dict[str, str] = {}
    for i in range(0, len(missing), 40):
        rows = await d1_exec(select(posts.c.id, posts.c.posted_at).where(posts.c.id.in_(missing[i : i + 40]))) or []
        found.update({r["id"]: r["posted_at"] for r in rows if r.get("posted_at")})
        await asyncio.sleep(0.5)
    for angle in state["angles"]:
        for ref in angle["refs"]:
            if not ref.get("postedAt") and ref["id"] in found:
                ref["postedAt"] = found[ref["id"]]
    await _write_json(key, state)
    return len(missing), len(found)


async def main(dry_run: bool, movie_id: str | None) -> None:
    # Không dùng _active_movies(): hàm đó cache bằng Redis mà Mac không có. Phim nào chưa có state.json thì fix() trả 0, 0.
    ids = (
        [movie_id]
        if movie_id
        else [r["id"] for r in await d1_exec(select(movies.c.id).where(movies.c.enabled == 1)) or []]
    )
    total_missing = total_found = 0
    for mid in ids:
        missing, found = await fix(mid, dry_run)
        print(f"{mid}: thiếu {missing}, bù được {found}")
        total_missing += missing
        total_found += found
    print(f"TỔNG: thiếu {total_missing}, bù được {total_found}" + (" (dry-run, chưa ghi)" if dry_run else ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="chỉ đếm ref thiếu postedAt, không ghi")
    parser.add_argument("--movie-id", help="chỉ một phim (mặc định: mọi phim đang hoạt động)")
    args = parser.parse_args()
    asyncio.run(main(args.dry_run, args.movie_id))
