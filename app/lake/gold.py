"""Tầng gold: bảng tổng hợp đọc từ silver, dùng trực tiếp cho dashboard.

    gold/movie_daily/platform=<p>/   mỗi phim × nền tảng × ngày (giờ VN) một dòng

Các bước (mỗi bước là một view/table DuckDB có tên, kiểm tra xong mới ghi - như silver):

    s_posts/s_snapshots/s_comments/s_dim  đầu vào silver
    post_movie   mỗi bài được giữ -> phim của nó (qua dim_keywords) + ngày đăng
    daily_last   bản chụp cuối mỗi ngày của mỗi bài
    post_delta   tương tác tăng thêm so với bản chụp ngày trước
    movie_daily  gom theo phim × nền tảng × ngày

Chạy bằng scripts/build_silver.py (sau silver)."""

from __future__ import annotations

import duckdb

from app.core.logging import get_logger
from app.lake.constants import GOLD, SILVER
from app.lake.silver import _write

logger = get_logger(__name__)

VN = "Asia/Ho_Chi_Minh"


def _vn_day(col: str) -> str:
    return f"timezone('{VN}', {col})::DATE"


def _inputs(con: duckdb.DuckDBPyConnection, silver: str) -> None:
    """Đặt tên cho các bảng silver đầu vào. Tiền tố s_ để không trùng các bảng posts/comments mà
    silver đã tạo trong cùng connection."""

    def read(path: str) -> str:
        return f"read_parquet('{silver}/{path}', hive_partitioning=true)"

    con.execute(f"CREATE OR REPLACE VIEW s_posts AS SELECT * FROM {read('posts/*/*.parquet')}")
    con.execute(f"CREATE OR REPLACE VIEW s_snapshots AS SELECT * FROM {read('posts_snapshots/*/*/*.parquet')}")
    con.execute(f"CREATE OR REPLACE VIEW s_comments AS SELECT * FROM {read('comments/*/*.parquet')}")
    con.execute(f"CREATE OR REPLACE VIEW s_dim AS SELECT * FROM read_parquet('{silver}/dim_keywords/*.parquet')")


def build_movie_daily(con: duckdb.DuckDBPyConnection, *, silver: str = SILVER, out: str = GOLD) -> int:
    """Dựng lại gold/movie_daily từ silver. `silver`/`out` có thể là thư mục local hoặc r2://."""
    _inputs(con, silver)

    # 1. post_movie: chỉ bài được giữ và hiện trên dashboard. Bài chưa có quyết định (từ trước khi có topic
    #    ingest_decisions) coi như được giữ. Mỗi bài một keyword_id nên join không nhân dòng.
    con.execute(
        f"""CREATE OR REPLACE TABLE post_movie AS
        SELECT p.platform, p.post_id, d.movie_id, {_vn_day("p.posted_at")} AS posted_day
        FROM s_posts p JOIN s_dim d USING (keyword_id)
        WHERE coalesce(p.decision, 'kept') = 'kept'
          -- Lưu vào D1 nhưng ẩn khỏi dashboard: Kira không kết luận và bài không có tín hiệu phim
          -- (xem ingest_consumer.handle_post / relevance_rules.has_film_context).
          AND coalesce(p.reason, '') <> 'no_film_context'"""
    )

    # 2. daily_last: một bài có thể được crawl nhiều lần trong ngày - giữ lần cuối.
    con.execute(
        f"""CREATE OR REPLACE TABLE daily_last AS
        SELECT platform, post_id, {_vn_day("scraped_at")} AS day,
               coalesce(likes, 0) + coalesce(comments, 0) + coalesce(shares, 0) AS engagement,
               coalesce(views, 0) AS views
        FROM s_snapshots
        QUALIFY row_number() OVER (
            PARTITION BY platform, post_id, {_vn_day("scraped_at")} ORDER BY scraped_at DESC) = 1"""
    )

    # 3. post_delta: so với bản chụp của lần thấy trước. NULL ở ngày đầu tiên thấy bài - phần đó
    #    không phải "tăng thêm" (cộng vào sẽ tạo đỉnh giả rất cao ở ngày lake bắt đầu ghi), nên
    #    tách riêng thành engagement_first_seen ở bước 4. Bỏ trống một ngày thì delta ngày sau gồm
    #    cả ngày bị bỏ.
    con.execute(
        """CREATE OR REPLACE TABLE post_delta AS
        SELECT *,
               engagement - lag(engagement) OVER w AS raw_delta,
               views - lag(views) OVER w AS raw_views_delta
        FROM daily_last
        WINDOW w AS (PARTITION BY platform, post_id ORDER BY day)"""
    )

    # 4. movie_daily: ba nguồn có "ngày" khác nhau (ngày đăng bài, ngày crawl, ngày đăng
    #    comment) nên gom riêng rồi FULL JOIN. Số đếm âm (nền tảng trả số thấp hơn) tính là 0.
    con.execute(
        f"""CREATE OR REPLACE TABLE movie_daily AS
        WITH posts_by_day AS (
            SELECT movie_id, platform, posted_day AS day, count(*) AS new_posts
            FROM post_movie WHERE posted_day IS NOT NULL
            GROUP BY ALL
        ),
        engagement_by_day AS (
            SELECT m.movie_id, d.platform, d.day,
                   count(*) AS posts_tracked,
                   sum(greatest(d.raw_delta, 0)) AS engagement_gained,
                   sum(d.engagement) FILTER (WHERE d.raw_delta IS NULL) AS engagement_first_seen,
                   sum(greatest(d.raw_views_delta, 0)) AS views_gained
            FROM post_delta d JOIN post_movie m USING (platform, post_id)
            GROUP BY ALL
        ),
        comments_by_day AS (
            SELECT m.movie_id, c.platform, {_vn_day("c.posted_at")} AS day, count(*) AS new_comments
            FROM s_comments c JOIN post_movie m USING (platform, post_id)
            WHERE c.posted_at IS NOT NULL
            GROUP BY ALL
        )
        SELECT movie_id, platform, day,
               coalesce(new_posts, 0)::BIGINT AS new_posts,
               coalesce(posts_tracked, 0)::BIGINT AS posts_tracked,
               coalesce(engagement_gained, 0)::BIGINT AS engagement_gained,
               coalesce(engagement_first_seen, 0)::BIGINT AS engagement_first_seen,
               coalesce(views_gained, 0)::BIGINT AS views_gained,
               coalesce(new_comments, 0)::BIGINT AS new_comments
        FROM posts_by_day
        FULL JOIN engagement_by_day USING (movie_id, platform, day)
        FULL JOIN comments_by_day USING (movie_id, platform, day)"""
    )

    # 5. kiểm tra xong mới ghi.
    _check_movie_daily(con)

    # 6. ghi.
    _write(con, "movie_daily", f"{out}/movie_daily", partition_by="platform")
    return con.sql("SELECT count(*) FROM movie_daily").fetchone()[0]


def _check_movie_daily(con: duckdb.DuckDBPyConnection) -> None:
    problems = []
    dupes = con.sql(
        "SELECT count(*) FROM (SELECT movie_id, platform, day FROM movie_daily GROUP BY ALL HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        problems.append(f"{dupes} duplicate (movie, platform, day) rows")
    # +1: ngày giờ VN có thể đi trước ngày UTC của máy chạy.
    future = con.sql("SELECT count(*) FROM movie_daily WHERE day > current_date + 1").fetchone()[0]
    if future:
        problems.append(f"{future} rows dated in the future")
    expected, got = con.sql(
        """SELECT (SELECT count(*) FROM post_movie WHERE posted_day IS NOT NULL),
                  (SELECT coalesce(sum(new_posts), 0) FROM movie_daily)"""
    ).fetchone()
    if expected != got:
        # Join làm nhân hoặc mất dòng.
        problems.append(f"new_posts total {got} != {expected} kept posts")
    if problems:
        raise ValueError("gold movie_daily check failed: " + "; ".join(problems))

    # Chỉ log: tỉ lệ cao là dấu hiệu đọc sai cột, nhưng không phải lý do để dừng build.
    posts, unmapped = con.sql(
        """SELECT count(*), count(*) FILTER (WHERE keyword_id IS NULL OR keyword_id NOT IN (SELECT keyword_id FROM s_dim))
           FROM s_posts"""
    ).fetchone()
    comments, unjoined = con.sql(
        """SELECT count(*), count(*) FILTER (WHERE m.post_id IS NULL)
           FROM s_comments c LEFT JOIN post_movie m USING (platform, post_id)"""
    ).fetchone()
    logger.info(
        "gold_movie_daily_coverage",
        posts=posts,
        posts_without_movie=unmapped,
        comments=comments,
        comments_without_kept_post=unjoined,
    )
