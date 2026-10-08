"""Kiểm tra lại các bài relevance_label='related' của một phim bằng movie_hashtag_present
HIỆN TẠI (cổng hashtag/token chính xác + uy tín tác giả), và hạ cấp những bài không
còn qua được thành 'not_related'.

Tồn tại vì bản thân relevance_label là một snapshot tại một thời điểm: nó được đặt bởi
phán quyết nào (AI hoặc kiểm tra chuỗi con theo từ khoá cũ) đang chạy lúc ingest, và
không bao giờ được đánh giá lại khi logic của movie_hashtag_present được cải thiện.
list_posts(sort="engagement")/get_report_sample_for_movie vốn đã chạy lại
movie_hashtag_present lúc query nên output CỦA CHÚNG sạch, nhưng relevance_label trên
đĩa vẫn sai cho tới khi có gì đó ghi lại - và get_movie_sentiment_counts
(app/services/d1.py) đếm thẳng trên relevance_label mà không kiểm tra lại như vậy,
nên phim có tên là từ vựng thông thường (ví dụ "Huyết Thống") vẫn làm bẩn tỉ lệ cảm
xúc của chính nó ngay cả sau khi màn hình danh sách bài/report đã được sửa.

Phạm vi: mỗi lần chạy một phim, theo id. Chạy lại an toàn - mỗi dòng chỉ là hạ cấp
relevance_label='related' -> 'not_related' (không bao giờ ngược lại; script này chỉ
xoá dương tính giả, không thêm lại được các dương tính thật đã bỏ sót), nên chạy lại
chỉ xác nhận lại/không làm gì với các dòng đã hạ cấp.

Cách dùng:
  .venv/bin/python -m scripts.clean_stale_relevance_labels <movie_id>
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone

from app.core.logging import get_logger
from app.repositories.d1.posts import movie_hashtag_present, reputable_authors
from app.services.d1 import d1_query

logger = get_logger(__name__)

PAGE_SIZE = 1000
# id (CASE nhãn) + id (CASE confidence, literal 0.0) + id+now (CASE labeled_at) + id
# (mệnh đề IN) = 5 tham số/dòng - thấp xa so với trần tham số bind của D1 mà comment
# ROWS_PER_BATCH trong scripts/push_relevance_labels.py ghi lại; 10 dòng/lô (50 tham
# số) khớp với kích thước lô của script đó.
ROWS_PER_BATCH = 10


async def _downgrade_batch(ids: list[str], now: str) -> None:
    label_cases = " ".join("WHEN ? THEN 'not_related'" for _ in ids)
    conf_cases = " ".join("WHEN ? THEN 0.0" for _ in ids)
    time_cases = " ".join("WHEN ? THEN ?" for _ in ids)
    placeholders = ", ".join("?" for _ in ids)

    sql = f"""
        UPDATE posts SET
            relevance_label = CASE id {label_cases} END,
            relevance_confidence = CASE id {conf_cases} END,
            relevance_labeled_at = CASE id {time_cases} END
        WHERE id IN ({placeholders})
    """
    params: list[object] = list(ids)
    params.extend(ids)
    for post_id in ids:
        params.extend([post_id, now])
    params.extend(ids)

    await d1_query(sql, params)


async def main(movie_id: str) -> None:
    movie_rows = await d1_query("SELECT title FROM movies WHERE id = ?", [movie_id], timeout=30.0)
    if not movie_rows:
        raise RuntimeError(f"No movie with id={movie_id}")
    movie_title = movie_rows[0]["title"]
    reputable = await reputable_authors()
    print(f"movie: {movie_title!r}, reputable authors cached: {len(reputable)}")

    last_id = ""
    total_scanned = 0
    to_downgrade: list[str] = []
    kept = 0
    while True:
        rows = await d1_query(
            """
            SELECT p.id, p.platform, p.author, p.content, k.keyword
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            WHERE p.movie_id = ? AND p.relevance_label = 'related' AND p.id > ?
            ORDER BY p.id
            LIMIT ?
            """,
            [movie_id, last_id, PAGE_SIZE],
            timeout=30.0,
        )
        if not rows:
            break
        for row in rows:
            is_reputable = (row["platform"], row["author"]) in reputable
            if movie_hashtag_present(
                row.get("content"), movie_title, row.get("keyword"), is_reputable_author=is_reputable
            ):
                kept += 1
            else:
                to_downgrade.append(row["id"])
        total_scanned += len(rows)
        last_id = rows[-1]["id"]
        print(f"scanned {total_scanned}, kept {kept}, to_downgrade {len(to_downgrade)}", flush=True)
        if len(rows) < PAGE_SIZE:
            break

    print(f"\ntotal scanned: {total_scanned}, kept: {kept}, downgrading: {len(to_downgrade)}")

    now = datetime.now(tz=timezone.utc).isoformat()
    for i in range(0, len(to_downgrade), ROWS_PER_BATCH):
        batch = to_downgrade[i : i + ROWS_PER_BATCH]
        await _downgrade_batch(batch, now)
        if (i // ROWS_PER_BATCH) % 20 == 0:
            print(f"downgraded {i + len(batch)}/{len(to_downgrade)}", flush=True)

    print(f"done - downgraded {len(to_downgrade)} posts to not_related")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m scripts.clean_stale_relevance_labels <movie_id>")
    asyncio.run(main(sys.argv[1]))
