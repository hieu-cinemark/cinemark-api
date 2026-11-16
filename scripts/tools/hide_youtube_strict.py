"""Ẩn bài YouTube CŨ của phim "chặt trên YouTube" (settings.youtube_strict_movie_slugs) không có tín hiệu gắn đúng phim.

Ingest từ 16/11/2026 tự làm việc này cho bài mới (app/workers/ingest_consumer/main.py, settings.is_strict_movie). Bài đã
lưu trước đó vẫn hiện, vd. video "mở con mắt thứ ba" dưới phim Con Mắt Thứ Ba. Cùng luật relevance_rules.has_film_context:
bài không có tín hiệu phim -> relevance_label = 'uncertain' (ẩn, không xoá - đổi lại 'related' là hiện lại).

Mặc định chỉ in kế hoạch + vài mẫu (không ghi). --apply mới ghi.

    python -m scripts.tools.hide_youtube_strict
    python -m scripts.tools.hide_youtube_strict --apply

Sau khi ghi: bảng tổng hợp theo ngày tự tính lại (reconcile mỗi 10 phút); cache Social Topic/Buzz làm mới ở lượt sau.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter

from sqlalchemy import and_, or_, select

from app.core.config import settings
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.store import d1_query
from app.repositories.d1.tables import movies
from app.repositories.d1.tables import posts as posts_table
from app.services.relevance.relevance_rules import has_film_context

UPDATE_CHUNK = 90  # D1 tối đa 100 tham số mỗi câu


async def run(args: argparse.Namespace) -> None:
    slugs = sorted(settings.youtube_strict_movies)
    film_rows = await d1_exec(select(movies).where(movies.c.slug.in_(slugs)), quiet=True) or []
    films = {row["id"]: dict(row) for row in film_rows}
    p = posts_table
    rows = (
        await d1_exec(
            select(p.c.id, p.c.movie_id, p.c.content).where(
                p.c.platform == "youtube",
                p.c.movie_id.in_(list(films)),
                or_(p.c.relevance_label == "related", and_(p.c.relevance_label.is_(None), p.c.keyword_match > 0)),
            ),
            timeout=90.0,
        )
        or []
    )
    hide = [row for row in rows if not has_film_context(row["content"], films[row["movie_id"]])]
    by_film = Counter(films[row["movie_id"]]["title"] for row in hide)
    kept = Counter(films[row["movie_id"]]["title"] for row in rows) - by_film
    for title in sorted(set(by_film) | set(kept)):
        print(f"{title}: ẩn {by_film[title]}, giữ {kept[title]}")
    for row in hide[: args.samples]:
        print("  ẩn:", " ".join(str(row["content"] or "").split())[:110])
    if not args.apply:
        print(f"Chạy thử - sẽ ẩn {len(hide)} / {len(rows)} bài. Thêm --apply để ghi.")
        return
    ids = [row["id"] for row in hide]
    for start in range(0, len(ids), UPDATE_CHUNK):
        chunk = ids[start : start + UPDATE_CHUNK]
        result = await d1_query(
            "UPDATE posts SET relevance_label = 'uncertain' WHERE id IN (SELECT value FROM json_each(?1))",
            [json.dumps(chunk)],
            timeout=60.0,
        )
        if result is None:
            raise SystemExit(f"ghi lỗi sau {start} bài")
    print(f"Đã ẩn {len(ids)} bài.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--samples", type=int, default=15)
    asyncio.run(run(parser.parse_args()))
