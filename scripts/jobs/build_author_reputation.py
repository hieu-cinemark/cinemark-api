"""Dựng/làm mới bảng author_reputation: với mỗi bài có relevance_label='related', kiểm
tra lại bằng CHỈ các tín hiệu MẠNH của movie_hashtag_present (is_reputable_author mặc
định False - xem docstring của hàm đó) và đếm, theo từng (platform, author), số phim
KHÁC NHAU mà họ được xác nhận theo cách đó.

Tác giả được xác nhận trên >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR phim khác nhau thì được
dùng để củng cố tín hiệu yếu nhất của movie_hashtag_present (chỉ khớp tên phim nguyên
văn, không có hashtag hỗ trợ) cho các phim KHÁC có tên trùng là từ vựng thông thường -
xem docstring module của hàm đó, sự cố "Huyết Thống" mà cả bảng này sinh ra để xử lý.
Không có vòng lặp: uy tín chỉ có được qua các tín hiệu dựa trên hashtag, không bao giờ
qua tín hiệu chỉ-khớp-tên-phim mà nó dùng để củng cố.

Dựng lại toàn bộ mỗi lần chạy (không tăng dần) - đủ rẻ (một lượt qua các bài
relevance_label='related', có phân trang) để cứ suy ra lại từ đầu thay vì theo dõi
phần thay đổi. Chạy lại định kỳ khi có thêm bài; đây là job theo lô, không phải thứ
mà các đường request gọi.

Cách dùng:
  .venv/bin/python -m scripts.jobs.build_author_reputation
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import delete, insert, select

from app.repositories.d1.posts import (
    MIN_MOVIES_FOR_REPUTABLE_AUTHOR,
    author_reputation,
    ensure_author_reputation_table,
    movie_hashtag_present,
)
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import keywords as keywords_table
from app.repositories.d1.tables import movies as movies_table
from app.repositories.d1.tables import posts as posts_table

PAGE_SIZE = 2000
# 7 tham số/dòng (platform, author, distinct_movies, confirmed_posts, updated_at - xem
# comment ROWS_PER_BATCH trong push_relevance_labels.py cho cùng lý do về trần tham số
# bind của D1) - 15 dòng/lô vẫn nằm thoải mái dưới mức đó.
ROWS_PER_INSERT_BATCH = 15


async def main() -> None:
    confirmed_movies: dict[tuple[str, str], set[str]] = defaultdict(set)
    confirmed_posts: dict[tuple[str, str], int] = defaultdict(int)

    last_id = ""
    total_scanned = 0
    while True:
        p, k, m = posts_table.alias("p"), keywords_table.alias("k"), movies_table.alias("m")
        rows = await d1_exec(
            select(
                p.c.id,
                p.c.platform,
                p.c.author,
                p.c.movie_id,
                p.c.content,
                k.c.keyword,
                m.c.title.label("movie_title"),
            )
            .select_from(p.outerjoin(k, k.c.id == p.c.keyword_id).outerjoin(m, m.c.id == p.c.movie_id))
            .where(
                p.c.relevance_label == "related",
                p.c.author.is_not(None),
                p.c.author != "",
                p.c.id > last_id,
            )
            .order_by(p.c.id)
            .limit(PAGE_SIZE),
            timeout=30.0,
        )
        if not rows:
            break
        for row in rows:
            if movie_hashtag_present(row.get("content"), row.get("movie_title"), row.get("keyword")):
                key = (row["platform"], row["author"])
                confirmed_movies[key].add(row["movie_id"])
                confirmed_posts[key] += 1
        total_scanned += len(rows)
        last_id = rows[-1]["id"]
        reputable_so_far = sum(1 for v in confirmed_movies.values() if len(v) >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR)
        print(f"scanned {total_scanned}, reputable-so-far {reputable_so_far}", flush=True)
        if len(rows) < PAGE_SIZE:
            break

    print(f"\ntotal scanned: {total_scanned}")
    print(f"authors with >=1 confirmed movie: {len(confirmed_movies)}")
    reputable_count = sum(1 for v in confirmed_movies.values() if len(v) >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR)
    print(f"authors reaching MIN_MOVIES_FOR_REPUTABLE_AUTHOR={MIN_MOVIES_FOR_REPUTABLE_AUTHOR}: {reputable_count}")

    await ensure_author_reputation_table()
    await d1_exec(delete(author_reputation))

    now = datetime.now(tz=timezone.utc).isoformat()
    all_rows = [
        (platform, author, len(movies), confirmed_posts[(platform, author)], now)
        for (platform, author), movies in confirmed_movies.items()
    ]
    for i in range(0, len(all_rows), ROWS_PER_INSERT_BATCH):
        batch = all_rows[i : i + ROWS_PER_INSERT_BATCH]
        await d1_exec(
            insert(author_reputation),
            values=[
                dict(zip(("platform", "author", "distinct_movies", "confirmed_posts", "updated_at"), row))
                for row in batch
            ],
        )

    print(f"wrote {len(all_rows)} author_reputation rows")


if __name__ == "__main__":
    asyncio.run(main())
