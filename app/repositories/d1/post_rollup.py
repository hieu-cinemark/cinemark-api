"""Đọc/ghi D1 của bảng tổng hợp bài theo phim x nền tảng x ngày đăng (movie_post_daily) - phần tính ở
app/services/stats/rollup/post_rollup_core.py, lịch ở app/services/stats/rollup/post_rollup.py.

Hai bộ bảng (27/11/2026, lúc chuyển từ Worker sang cinemark-api):
- "shadow": movie_post_daily_shadow + movie_post_rollup_days_shadow - chạy song song với Worker để đối chiếu, không ai đọc;
- "live": movie_post_daily + movie_post_rollup_days - bảng Social Buzz của Worker đọc. Chỉ bật khi đã gỡ processPostRollup khỏi
  worker.ts trong cùng lần deploy (hai bên không được cùng ghi một bảng).

Ghi một ngày không có lúc nào trống (Worker xoá rồi chèn trong một batch; HTTP API không batch được): upsert các dòng mới kèm
`run`, rồi xoá các dòng của ngày đó có `run` khác - như comment_rollup.
"""

from __future__ import annotations

import uuid
from typing import Any

from app.clients.d1 import d1_query
from app.core.logging import get_logger
from app.repositories.d1.post_filters import RELEVANT_POST_SQL
from app.services.stats.rollup.helper import DAY_MS, day_start, utc_iso
from app.services.stats.rollup.post_rollup_core import RollupRow, rollup_handle

logger = get_logger(__name__)

TABLES = {
    "shadow": ("movie_post_daily_shadow", "movie_post_rollup_days_shadow"),
    "live": ("movie_post_daily", "movie_post_rollup_days"),
}
_COLUMNS = ("day", "seen_age", "movie_id", "platform", "organic", "posts", "discussions", "shares", "views", "run")
# D1 cho tối đa 100 tham số mỗi câu, mỗi dòng 10 tham số.
ROWS_PER_INSERT = 9
TIER_CHUNK = 90

_ready: set[str] = set()


async def ensure_schema(mode: str) -> bool:
    """Bảng shadow tạo mới; bảng live (Worker đã tạo) chỉ thêm cột `run` (lỗi "duplicate column" khi đã có - bỏ qua)."""
    if mode in _ready:
        return True
    daily, days = TABLES[mode]
    if mode == "shadow":
        statements = (
            f"""CREATE TABLE IF NOT EXISTS {daily} (
                day TEXT NOT NULL, seen_age INTEGER NOT NULL, movie_id TEXT NOT NULL, platform TEXT NOT NULL,
                organic INTEGER NOT NULL, posts INTEGER NOT NULL, discussions INTEGER NOT NULL, shares INTEGER NOT NULL,
                views INTEGER NOT NULL, run TEXT,
                PRIMARY KEY (day, seen_age, movie_id, platform, organic)
            )""",
            f"CREATE TABLE IF NOT EXISTS {days} (day TEXT PRIMARY KEY, computed_at INTEGER NOT NULL)",
        )
        for sql in statements:
            if await d1_query(sql, timeout=60.0) is None:
                logger.warning("post_rollup_schema_failed", mode=mode, sql=sql.split("(")[0][:80])
                return False
    else:
        await d1_query(f"ALTER TABLE {daily} ADD COLUMN run TEXT", quiet=True, timeout=60.0)
    _ready.add(mode)
    return True


async def load_computed(mode: str) -> dict[str, int] | None:
    """Ngày đăng -> lúc tính gần nhất (mili giây). None khi D1 lỗi."""
    rows = await d1_query(f"SELECT day, computed_at FROM {TABLES[mode][1]}", timeout=30.0)
    if rows is None:
        return None
    return {row["day"]: int(row["computed_at"]) for row in rows}


async def _load_tiers(handles: list[str]) -> dict[str, str] | None:
    """Hạng tác giả (cinemark-db) theo handle - như loadTiers của Worker: không lọc theo nền tảng."""
    tiers: dict[str, str] = {}
    for i in range(0, len(handles), TIER_CHUNK):
        chunk = handles[i : i + TIER_CHUNK]
        marks = ", ".join("?" for _ in chunk)
        rows = await d1_query(
            f"SELECT handle, tier FROM author_classifications WHERE handle IN ({marks})", chunk, database="app", timeout=30.0
        )
        if rows is None:
            return None
        for row in rows:
            tiers[row["handle"]] = row["tier"]
    return tiers


async def load_day(day: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]] | None:
    """Bài liên quan có ngày đăng (giờ VN) là `day`, các bản chụp của chúng và hạng tác giả. None khi D1 lỗi.

    Cận so chuỗi với posted_at dạng toISOString ('...T17:00:00.000Z') giống hệt Worker - để hai bên cùng tính một bài ở sát mốc."""
    start = utc_iso(day_start(day))
    end = utc_iso(day_start(day) + DAY_MS)
    in_day = f"p.movie_id IS NOT NULL AND p.posted_at >= ?1 AND p.posted_at < ?2 AND {RELEVANT_POST_SQL}"
    posts = await d1_query(
        f"""SELECT p.id, p.movie_id, p.platform, p.author, p.url, p.scraped_at, p.reply_count AS replies,
                   COALESCE(p.repost_count, 0) + COALESCE(p.reshare_count, 0) + COALESCE(p.quote_count, 0) AS shares,
                   p.view_count AS views
            FROM posts p WHERE {in_day}""",
        [start, end],
        timeout=120.0,
    )
    if posts is None:
        return None
    snapshots = await d1_query(
        f"""SELECT s.post_id, s.recorded_at, s.reply_count AS replies,
                   s.repost_count + s.reshare_count + s.quote_count AS shares, s.view_count AS views
            FROM posts p JOIN post_engagement_snapshots s ON s.post_id = p.id WHERE {in_day}""",
        [start, end],
        timeout=120.0,
    )
    if snapshots is None:
        return None
    handles = sorted({h for h in (rollup_handle(p["platform"], p.get("author"), p.get("url")) for p in posts) if h})
    tiers = await _load_tiers(handles)
    if tiers is None:
        return None
    return posts, snapshots, tiers


async def write_day(mode: str, day: str, rows: list[RollupRow], computed_at: int) -> bool:
    """Ghi các dòng của một ngày rồi đánh dấu đã tính. False khi D1 lỗi (giữ số cũ của ngày đó, lượt sau tính lại)."""
    daily, days = TABLES[mode]
    run = uuid.uuid4().hex[:12]
    columns = ", ".join(_COLUMNS)
    for i in range(0, len(rows), ROWS_PER_INSERT):
        chunk = rows[i : i + ROWS_PER_INSERT]
        params: list[Any] = []
        for row in chunk:
            params += [day, row.seen_age, row.movie_id, row.platform, row.organic, row.posts, row.discussions, row.shares,
                       row.views, run]
        values = ", ".join("(" + ", ".join("?" for _ in _COLUMNS) + ")" for _ in chunk)
        sql = (
            f"INSERT INTO {daily} ({columns}) VALUES {values} "
            "ON CONFLICT (day, seen_age, movie_id, platform, organic) DO UPDATE SET posts = excluded.posts, "
            "discussions = excluded.discussions, shares = excluded.shares, views = excluded.views, run = excluded.run"
        )
        if await d1_query(sql, params, timeout=60.0) is None:
            logger.warning("post_rollup_day_failed", mode=mode, day=day, step="insert")
            return False
    if await d1_query(f"DELETE FROM {daily} WHERE day = ?1 AND (run IS NULL OR run != ?2)", [day, run], timeout=60.0) is None:
        logger.warning("post_rollup_day_failed", mode=mode, day=day, step="delete")
        return False
    marked = await d1_query(
        f"INSERT INTO {days} (day, computed_at) VALUES (?1, ?2) ON CONFLICT (day) DO UPDATE SET computed_at = excluded.computed_at",
        [day, computed_at],
        timeout=30.0,
    )
    if marked is None:
        logger.warning("post_rollup_day_failed", mode=mode, day=day, step="mark")
        return False
    return True


# --- đọc chuỗi theo ngày của một phim (prefix sum, app/services/stats/rollup/movie_series.py) ---

POST_METRICS = ("posts", "replies", "shares", "views", "organic_posts")
COMMENT_METRICS = ("comments", "meaningful", "positive", "negative", "neutral", "watched", "anticipating")

_read_index_ready = False


async def _ensure_read_index() -> None:
    """Bảng của Worker chỉ có khoá (day, seen_age, movie_id, ...): đọc theo phim sẽ quét cả bảng - thêm index theo phim."""
    global _read_index_ready
    if not _read_index_ready:
        await d1_query(
            "CREATE INDEX IF NOT EXISTS idx_movie_post_daily_movie ON movie_post_daily(movie_id, seen_age, day)", timeout=120.0
        )
        _read_index_ready = True


async def load_movie_post_days(movie_id: str) -> list[dict[str, Any]] | None:
    """Số hiện tại (seen_age = -1) theo ngày đăng của một phim, cộng mọi nền tảng. replies = discussions - posts (bảng tính mỗi
    bài 1 + số trả lời)."""
    await _ensure_read_index()
    return await d1_query(
        """SELECT day, SUM(posts) AS posts, SUM(discussions) - SUM(posts) AS replies, SUM(shares) AS shares,
                  SUM(views) AS views, SUM(CASE WHEN organic = 1 THEN posts ELSE 0 END) AS organic_posts
           FROM movie_post_daily WHERE movie_id = ?1 AND seen_age = -1 GROUP BY day""",
        [movie_id],
        timeout=30.0,
    )


async def load_movie_comment_days(movie_id: str) -> list[dict[str, Any]] | None:
    """Bình luận theo ngày của một phim (movie_comment_daily): tổng, có nghĩa (không phải nhãn quy tắc), theo cảm xúc (chỉ bình
    luận có nghĩa), theo giai đoạn khán giả (đã xem / đang hóng)."""
    return await d1_query(
        """SELECT day, SUM(n) AS comments, SUM(CASE WHEN meaningful = 1 THEN n ELSE 0 END) AS meaningful,
                  SUM(CASE WHEN meaningful = 1 AND sentiment = 'positive' THEN n ELSE 0 END) AS positive,
                  SUM(CASE WHEN meaningful = 1 AND sentiment = 'negative' THEN n ELSE 0 END) AS negative,
                  SUM(CASE WHEN meaningful = 1 AND sentiment = 'neutral' THEN n ELSE 0 END) AS neutral,
                  SUM(CASE WHEN stage = 'da_xem' THEN n ELSE 0 END) AS watched,
                  SUM(CASE WHEN stage = 'hong' THEN n ELSE 0 END) AS anticipating
           FROM movie_comment_daily WHERE movie_id = ?1 GROUP BY day""",
        [movie_id],
        timeout=30.0,
    )


async def load_computed_post_days(mode: str = "live") -> list[str] | None:
    """Các ngày đăng đã có trong bảng tổng hợp bài - để biết một khoảng ngày đã đủ dữ liệu chưa."""
    rows = await d1_query(f"SELECT day FROM {TABLES[mode][1]}", timeout=30.0)
    return None if rows is None else [row["day"] for row in rows]
