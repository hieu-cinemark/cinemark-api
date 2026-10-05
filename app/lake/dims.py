"""Bảng chiều (dimension) của lake, kéo từ D1 vì bronze không có: dim_keywords cho biết mỗi
keyword_id thuộc phim nào, để gold gom bài theo phim. Ghi ra silver/dim_keywords/."""

from __future__ import annotations

import duckdb

from app.clients.d1 import d1_query
from app.lake.constants import SILVER
from app.lake.silver import _write


async def fetch_keywords() -> list[dict]:
    """Mọi keyword (kể cả đã tắt - bài cũ vẫn trỏ tới) kèm phim của nó, đọc từ D1."""
    rows = await d1_query(
        """SELECT k.id AS keyword_id, k.movie_id, k.platform, k.keyword,
                  m.title AS movie_title, m.slug AS movie_slug
           FROM keywords k JOIN movies m ON m.id = k.movie_id"""
    )
    if not rows:  # None = D1 lỗi; [] = chắc chắn có vấn đề
        raise RuntimeError("could not read keywords from D1")
    return rows


def build_dim_keywords(con: duckdb.DuckDBPyConnection, rows: list[dict], *, out: str = SILVER) -> int:
    con.execute(
        """CREATE OR REPLACE TABLE dim_keywords (
               keyword_id VARCHAR, movie_id VARCHAR, platform VARCHAR,
               keyword VARCHAR, movie_title VARCHAR, movie_slug VARCHAR)"""
    )
    con.executemany(
        "INSERT INTO dim_keywords VALUES (?, ?, ?, ?, ?, ?)",
        [
            [r["keyword_id"], r["movie_id"], r["platform"], r["keyword"], r["movie_title"], r["movie_slug"]]
            for r in rows
        ],
    )
    dupes = con.sql(
        "SELECT count(*) FROM (SELECT keyword_id FROM dim_keywords GROUP BY 1 HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        raise ValueError(f"dim_keywords: {dupes} duplicate keyword_id")
    _write(con, "dim_keywords", f"{out}/dim_keywords")  # không partition
    return con.sql("SELECT count(*) FROM dim_keywords").fetchone()[0]
