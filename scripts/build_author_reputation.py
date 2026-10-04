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
  .venv/bin/python -m scripts.build_author_reputation
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone

from app.repositories.d1.posts import (
    MIN_MOVIES_FOR_REPUTABLE_AUTHOR,
    ensure_author_reputation_table,
    movie_hashtag_present,
)
from app.services.d1 import d1_query

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
        rows = await d1_query(
            """
            SELECT p.id, p.platform, p.author, p.movie_id, p.content, k.keyword, m.title AS movie_title
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            WHERE p.relevance_label = 'related' AND p.author IS NOT NULL AND p.author != '' AND p.id > ?
            ORDER BY p.id
            LIMIT ?
            """,
            [last_id, PAGE_SIZE],
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
    await d1_query("DELETE FROM author_reputation")

    now = datetime.now(tz=timezone.utc).isoformat()
    all_rows = [
        (platform, author, len(movies), confirmed_posts[(platform, author)], now)
        for (platform, author), movies in confirmed_movies.items()
    ]
    for i in range(0, len(all_rows), ROWS_PER_INSERT_BATCH):
        batch = all_rows[i : i + ROWS_PER_INSERT_BATCH]
        placeholders = ", ".join("(?, ?, ?, ?, ?)" for _ in batch)
        params = [value for row in batch for value in row]
        await d1_query(
            f"INSERT INTO author_reputation (platform, author, distinct_movies, confirmed_posts, updated_at) "
            f"VALUES {placeholders}",
            params,
        )

    print(f"wrote {len(all_rows)} author_reputation rows")


if __name__ == "__main__":
    asyncio.run(main())
