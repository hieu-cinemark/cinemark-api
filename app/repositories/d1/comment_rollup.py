"""Bảng tổng hợp bình luận theo phim x ngày (30/10/2026) - Social Topic (insights) và Social Buzz của cinemark đọc bảng này
thay vì đếm lại hàng triệu bình luận mỗi lần tính cache (17-44 giây, 1-5 triệu dòng mỗi câu, ~4 giờ D1 bận mỗi ngày - D1
chạy các câu của một database gần như lần lượt, nên request của người dùng xếp hàng sau).

    movie_comment_daily  (movie_id, day, platform, stage, sentiment, meaningful) -> n
    movie_aspect_daily   (movie_id, day, aspect) -> n
    movie_comment_group_daily (movie_id, day, platform, sentiment, after_premiere, grp) -> n

- day: ngày giờ Việt Nam của c.posted_at (chỉ ~20/2 triệu bình luận thiếu posted_at - bỏ qua);
- chỉ bình luận thuộc bài liên quan (RELEVANT_POST_SQL, cùng điều kiện RELEVANT_POST_FILTER của cinemark);
- stage = audience_stage ('' khi chưa gán), sentiment ('' khi chưa chấm), meaningful = sentiment_source khác 'rule'
  (MEANINGFUL_COMMENT_FILTER của cinemark); bảng khía cạnh chỉ đếm bình luận meaningful.

movie_comment_group_daily là số của "Bình luận đã thu thập theo chủ đề" trên Social Topic (cinemark api/lib/topicComments.ts):
chỉ bình luận gốc có chữ; grp = 'total' / 'icon' (bình luận gắn nhãn 'rule') / 'trend' (bài hưởng ứng trend) / 'none' (chưa có
chủ đề) / nhãn chủ đề như 'dien_vien:+'. after_premiere = bài đăng từ ngày chiếu sớm (hoặc ngày khởi chiếu) của phim trở đi,
cho giai đoạn Chiếu sớm / Chính thức. Đếm trực tiếp trên bình luận mất tới 107 giây với khoảng ngày rộng (09/11/2026).

Tính lại theo NGUYÊN NGÀY (app/services/stats/comment_rollup.py): upsert số mới của ngày rồi xoá các dòng của ngày đó
không còn xuất hiện (đánh dấu bằng `run`) - không có lúc nào ngày đó trống. Đọc một ngày đi qua index comments(posted_at)
nên chỉ chạm bình luận của đúng ngày đó.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

from app.clients.d1 import d1_query
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL

logger = get_logger(__name__)

_ready = False


async def ensure_rollup_schema() -> bool:
    """Tạo bảng + index (một lần mỗi tiến trình). Index comments(posted_at) dựng trên ~2 triệu dòng có thể mất cả phút -
    timeout dài, và chỉ chạy một lần (IF NOT EXISTS)."""
    global _ready
    if _ready:
        return True
    statements = (
        """CREATE TABLE IF NOT EXISTS movie_comment_daily (
            movie_id TEXT NOT NULL,
            day TEXT NOT NULL,
            platform TEXT NOT NULL,
            stage TEXT NOT NULL,
            sentiment TEXT NOT NULL,
            meaningful INTEGER NOT NULL,
            n INTEGER NOT NULL,
            run TEXT NOT NULL,
            PRIMARY KEY (movie_id, day, platform, stage, sentiment, meaningful)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_movie_comment_daily_day ON movie_comment_daily(day)",
        """CREATE TABLE IF NOT EXISTS movie_aspect_daily (
            movie_id TEXT NOT NULL,
            day TEXT NOT NULL,
            aspect TEXT NOT NULL,
            n INTEGER NOT NULL,
            run TEXT NOT NULL,
            PRIMARY KEY (movie_id, day, aspect)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_movie_aspect_daily_day ON movie_aspect_daily(day)",
        """CREATE TABLE IF NOT EXISTS movie_comment_group_daily (
            movie_id TEXT NOT NULL,
            day TEXT NOT NULL,
            platform TEXT NOT NULL,
            sentiment TEXT NOT NULL,
            after_premiere INTEGER NOT NULL,
            grp TEXT NOT NULL,
            n INTEGER NOT NULL,
            run TEXT NOT NULL,
            PRIMARY KEY (movie_id, day, platform, sentiment, after_premiere, grp)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_movie_comment_group_daily_day ON movie_comment_group_daily(day)",
        "CREATE INDEX IF NOT EXISTS idx_comments_posted_at ON comments(posted_at)",
    )
    for sql in statements:
        if await d1_query(sql, timeout=600.0) is None:
            logger.warning("comment_rollup_schema_failed", sql=sql.split("(")[0][:80])
            return False
    _ready = True
    return True


def _utc_bounds(day: str) -> tuple[str, str]:
    """Ngày giờ VN -> [00:00 VN, 00:00 VN hôm sau) dạng chuỗi UTC so được với posted_at ('YYYY-MM-DDTHH:MM:SS+00:00')."""
    start = date.fromisoformat(day) - timedelta(days=1)
    return f"{start.isoformat()}T17:00:00", f"{day}T17:00:00"


async def rebuild_day(day: str) -> bool:
    """Tính lại mọi dòng của một ngày VN cho mọi phim. False khi D1 lỗi (giữ số cũ của ngày đó)."""
    if not await ensure_rollup_schema():
        return False
    start, end = _utc_bounds(day)
    run = uuid.uuid4().hex[:12]
    steps = (
        (
            f"""INSERT INTO movie_comment_daily (movie_id, day, platform, stage, sentiment, meaningful, n, run)
                SELECT p.movie_id, ?3, p.platform, COALESCE(c.audience_stage, ''), COALESCE(c.sentiment, ''),
                       COALESCE(c.sentiment_source, '') != 'rule', COUNT(*), ?4
                FROM comments c JOIN posts p ON p.id = c.post_id
                WHERE c.posted_at >= ?1 AND c.posted_at < ?2 AND p.movie_id IS NOT NULL AND {RELEVANT_POST_SQL}
                GROUP BY 1, 3, 4, 5, 6
                ON CONFLICT (movie_id, day, platform, stage, sentiment, meaningful) DO UPDATE SET
                    n = excluded.n, run = excluded.run""",
            [start, end, day, run],
        ),
        ("DELETE FROM movie_comment_daily WHERE day = ?1 AND run != ?2", [day, run]),
        (
            f"""INSERT INTO movie_aspect_daily (movie_id, day, aspect, n, run)
                SELECT p.movie_id, ?3, je.value, COUNT(*), ?4
                FROM comments c JOIN posts p ON p.id = c.post_id, json_each(c.aspects) je
                WHERE c.posted_at >= ?1 AND c.posted_at < ?2 AND p.movie_id IS NOT NULL AND {RELEVANT_POST_SQL}
                  AND COALESCE(p.trend_post, 0) = 0 AND COALESCE(c.sentiment_source, '') != 'rule' AND c.aspects IS NOT NULL AND c.aspects != '[]'
                GROUP BY 1, 3
                ON CONFLICT (movie_id, day, aspect) DO UPDATE SET n = excluded.n, run = excluded.run""",
            [start, end, day, run],
        ),
        ("DELETE FROM movie_aspect_daily WHERE day = ?1 AND run != ?2", [day, run]),
        (
            f"""INSERT INTO movie_comment_group_daily (movie_id, day, platform, sentiment, after_premiere, grp, n, run)
                WITH base AS (
                    SELECT p.movie_id, p.platform, COALESCE(c.sentiment, '') AS sentiment, c.aspects,
                           COALESCE(p.posted_at >= strftime('%Y-%m-%dT%H:%M:%S', substr(COALESCE(s.premiere_date, m.released_at), 1, 10), '-7 hours'), 0)
                               AS after_premiere,
                           COALESCE(c.sentiment_source, '') != 'rule' AS meaningful, COALESCE(p.trend_post, 0) = 1 AS trend
                    FROM comments c JOIN posts p ON p.id = c.post_id
                    LEFT JOIN movies m ON m.id = p.movie_id LEFT JOIN movie_measure_settings s ON s.movie_id = p.movie_id
                    WHERE c.posted_at >= ?1 AND c.posted_at < ?2 AND p.movie_id IS NOT NULL AND {RELEVANT_POST_SQL}
                      AND c.parent_external_id IS NULL AND TRIM(COALESCE(c.message, '')) != ''
                )
                SELECT movie_id, ?3, platform, sentiment, after_premiere, grp, COUNT(*), ?4 FROM (
                    SELECT movie_id, platform, sentiment, after_premiere, CASE WHEN meaningful THEN 'total' ELSE 'icon' END AS grp FROM base
                    UNION ALL SELECT movie_id, platform, sentiment, after_premiere, 'trend' FROM base WHERE meaningful AND trend
                    UNION ALL SELECT movie_id, platform, sentiment, after_premiere, 'none' FROM base
                        WHERE meaningful AND NOT trend AND (aspects IS NULL OR aspects IN ('', '[]'))
                    UNION ALL SELECT b.movie_id, b.platform, b.sentiment, b.after_premiere, je.value FROM base b, json_each(b.aspects) je
                        WHERE b.meaningful AND NOT b.trend AND b.aspects IS NOT NULL AND b.aspects NOT IN ('', '[]')
                )
                GROUP BY 1, 3, 4, 5, 6
                ON CONFLICT (movie_id, day, platform, sentiment, after_premiere, grp) DO UPDATE SET n = excluded.n, run = excluded.run""",
            [start, end, day, run],
        ),
        ("DELETE FROM movie_comment_group_daily WHERE day = ?1 AND run != ?2", [day, run]),
    )
    for sql, params in steps:
        if await d1_query(sql, params, timeout=120.0) is None:
            logger.warning("comment_rollup_day_failed", day=day, step=sql.split()[0])
            return False
    return True
