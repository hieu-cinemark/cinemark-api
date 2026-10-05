"""Thống kê dashboard được tổng hợp sẵn cho D1 remote.

COUNT(*) toàn bảng posts/comments qua HTTP API của Cloudflare quá chậm cho trang
Overview (vài giây mỗi endpoint). Hai bảng tổng hợp theo ngày này đủ nhỏ để đọc trong
vài chục mili giây, và được cộng thêm mỗi lần insert bài/comment *mới* (update chỉ
làm mới last_scraped_at).

    stats_platform_daily  (day, platform) -> posts, comments, last_scraped_at
    stats_keyword_daily   (day, keyword_id) -> platform, posts, comments, last_scraped_at

Dựng lại từ các dòng hiện có bằng:

    python -m scripts.rebuild_stats_summaries

Ngoài ra có bộ đếm theo giờ (biểu đồ "24 giờ gần nhất" của dashboard) nằm trong Redis
chứ không phải D1: mỗi bài/comment mới thêm một lượt HINCRBY, giữ HOURLY_RETENTION_HOURS
rồi tự hết hạn - không tốn thêm lượt ghi D1 nào. Không dựng lại được từ D1; bị mất thì
biểu đồ chỉ trống phần giờ đó.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.clients.telegram import send_telegram_message
from app.core.logging import get_logger

logger = get_logger(__name__)

_ready = False
_ready_lock = asyncio.Lock()

# Cùng kiểu với _note_drop trong app/workers/ingest_consumer/main.py. Ghi bảng tổng hợp
# lỗi ở đây không làm mất chính bài/comment (persist_post/persist_comment đã commit dòng
# đó rồi) nhưng âm thầm để số liệu trên trang Overview bị thiếu cho ngày/nền tảng đó mãi
# mãi - đây là các bộ đếm cộng dồn, không có cách đối soát nào trừ khi có người nhận ra
# và tự chạy `python -m scripts.rebuild_stats_summaries`. Cùng dạng cửa sổ trượt rồi
# cảnh báo một lần như bộ đếm bài bị loại của ingest, để D1 sập kéo dài không spam kênh
# sau khi đã báo một lần.
_ROLLUP_FAILURE_ALERT_THRESHOLD = 5
_ROLLUP_FAILURE_WINDOW_SECONDS = 3600


async def _note_rollup_failure(table: str, **context: Any) -> None:
    logger.error("stats_rollup_write_failed", table=table, **context)
    client = get_redis_client()
    key = f"{REDIS_KEY_PREFIX}stats_rollup_failed:{table}"
    count = await client.incr(key)
    if count == 1:
        await client.expire(key, _ROLLUP_FAILURE_WINDOW_SECONDS)
    if count == _ROLLUP_FAILURE_ALERT_THRESHOLD:
        details = " | ".join(f"{k}={v}" for k, v in context.items())
        await send_telegram_message(
            f"🚨 Stats rollup write failing: {table}\n"
            f"{count} failures in the last {_ROLLUP_FAILURE_WINDOW_SECONDS // 60}m - dashboard counts are "
            f"drifting.\nRun `python -m scripts.rebuild_stats_summaries` to reconcile.\n{details}"
        )


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except TypeError, ValueError:
        return 0


async def _q(
    sql: str, params: list[Any] | None = None, *, quiet: bool = False, timeout: float = 10.0
) -> list[dict[str, Any]] | None:
    # Import lười: d1.py gọi vào module này từ persist_*/get_*.
    from app.services.d1 import d1_query

    return await d1_query(sql, params, quiet=quiet, timeout=timeout)


async def ensure_stats_tables() -> None:
    """Tạo các bảng tổng hợp một lần mỗi tiến trình nếu còn thiếu."""
    global _ready
    if _ready:
        return
    async with _ready_lock:
        if _ready:
            return
        await _q(
            """
            CREATE TABLE IF NOT EXISTS stats_platform_daily (
                day TEXT NOT NULL,
                platform TEXT NOT NULL,
                posts INTEGER NOT NULL DEFAULT 0,
                comments INTEGER NOT NULL DEFAULT 0,
                last_scraped_at TEXT,
                PRIMARY KEY (day, platform)
            )
            """,
            quiet=True,
        )
        await _q(
            """
            CREATE TABLE IF NOT EXISTS stats_keyword_daily (
                day TEXT NOT NULL,
                keyword_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                posts INTEGER NOT NULL DEFAULT 0,
                comments INTEGER NOT NULL DEFAULT 0,
                last_scraped_at TEXT,
                PRIMARY KEY (day, keyword_id)
            )
            """,
            quiet=True,
        )
        _ready = True


def _day_from_iso(scraped_at: str) -> str:
    return scraped_at[:10] if scraped_at else ""


HOURLY_RETENTION_HOURS = 72
_HOURLY_KEY_PREFIX = f"{REDIS_KEY_PREFIX}stats_hourly:"


def _hour_bucket(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H")


async def _bump_hourly(platform: str, scraped_at: str, metric: str) -> None:
    try:
        when = datetime.fromisoformat(scraped_at)
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        key = f"{_HOURLY_KEY_PREFIX}{_hour_bucket(when)}"
        client = get_redis_client()
        await client.hincrby(key, f"{platform}:{metric}", 1)
        await client.expire(key, HOURLY_RETENTION_HOURS * 3600)
    except Exception as exc:
        # Chỉ là số liệu biểu đồ - không bao giờ được làm hỏng việc ingest.
        logger.warning("stats_hourly_bump_failed", platform=platform, error=str(exc) or repr(exc))


async def bump_ingest_decision(platform: str | None, decision: str, reason: str) -> None:
    """Phễu ingest theo giờ (cùng hash Redis với bộ đếm bài/comment): mọi bài vào ingest
    tăng `received`; bài bị loại tăng thêm `dropped.<lý do>`. Bài được giữ được tách thành
    mới/cập nhật bởi record_post (`posts` / `updated`)."""
    if not platform:
        return
    try:
        key = f"{_HOURLY_KEY_PREFIX}{_hour_bucket(datetime.now(UTC))}"
        client = get_redis_client()
        pipe = client.pipeline()
        pipe.hincrby(key, f"{platform}:received", 1)
        if decision == "dropped":
            pipe.hincrby(key, f"{platform}:dropped.{reason}", 1)
        pipe.expire(key, HOURLY_RETENTION_HOURS * 3600)
        await pipe.execute()
    except Exception as exc:
        logger.warning("stats_funnel_bump_failed", platform=platform, error=str(exc) or repr(exc))


async def get_ingest_funnel(hours: int) -> list[dict[str, Any]]:
    """Tổng phễu ingest bài viết của từng nền tảng trong `hours` giờ gần nhất."""
    client = get_redis_client()
    now = datetime.now(UTC)
    pipe = client.pipeline()
    for i in range(hours):
        pipe.hgetall(f"{_HOURLY_KEY_PREFIX}{_hour_bucket(now - timedelta(hours=i))}")
    totals: dict[str, dict[str, Any]] = {}
    for fields in await pipe.execute():
        for raw_field, raw_value in (fields or {}).items():
            platform, _, metric = str(raw_field).partition(":")
            row = totals.setdefault(
                platform, {"platform": platform, "received": 0, "new": 0, "updated": 0, "dropped": {}}
            )
            value = _as_int(raw_value)
            if metric == "received":
                row["received"] += value
            elif metric == "posts":
                row["new"] += value
            elif metric == "updated":
                row["updated"] += value
            elif metric.startswith("dropped."):
                reason = metric.removeprefix("dropped.")
                row["dropped"][reason] = row["dropped"].get(reason, 0) + value
    return sorted(totals.values(), key=lambda r: r["platform"])


async def get_hourly_counts(hours: int) -> list[dict[str, Any]]:
    """Số bài/comment mới theo giờ (UTC) của từng nền tảng trong `hours` giờ gần nhất,
    kể cả giờ hiện tại. Giờ không có dữ liệu không có dòng nào."""
    client = get_redis_client()
    now = datetime.now(UTC)
    buckets = [_hour_bucket(now - timedelta(hours=i)) for i in range(hours - 1, -1, -1)]
    pipe = client.pipeline()
    for bucket in buckets:
        pipe.hgetall(f"{_HOURLY_KEY_PREFIX}{bucket}")
    results = await pipe.execute()
    rows: list[dict[str, Any]] = []
    for bucket, fields in zip(buckets, results, strict=True):
        per_platform: dict[str, dict[str, int]] = {}
        for raw_field, raw_value in (fields or {}).items():
            field = raw_field.decode() if isinstance(raw_field, bytes) else str(raw_field)
            platform, _, metric = field.partition(":")
            if metric in ("posts", "comments"):
                per_platform.setdefault(platform, {"posts": 0, "comments": 0})[metric] = _as_int(raw_value)
        for platform, counts in sorted(per_platform.items()):
            rows.append({"hour": f"{bucket}:00:00Z", "platform": platform, **counts})
    return rows


async def record_post(
    *,
    platform: str,
    keyword_id: str | None,
    scraped_at: str,
    is_new: bool,
) -> None:
    """Cộng bảng tổng hợp ngày theo nền tảng (+ từ khoá) sau khi ghi một bài."""
    day = _day_from_iso(scraped_at)
    if not day or not platform:
        return
    await ensure_stats_tables()
    post_delta = 1 if is_new else 0
    await _bump_hourly(platform, scraped_at, "posts" if is_new else "updated")
    ok = await _q(
        """
        INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
        VALUES (?, ?, ?, 0, ?)
        ON CONFLICT(day, platform) DO UPDATE SET
            posts = posts + excluded.posts,
            last_scraped_at = excluded.last_scraped_at
        """,
        [day, platform, post_delta, scraped_at],
    )
    if ok is None:
        await _note_rollup_failure("stats_platform_daily", day=day, platform=platform)
    if keyword_id:
        ok = await _q(
            """
            INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
            VALUES (?, ?, ?, ?, 0, ?)
            ON CONFLICT(day, keyword_id) DO UPDATE SET
                posts = posts + excluded.posts,
                platform = excluded.platform,
                last_scraped_at = excluded.last_scraped_at
            """,
            [day, keyword_id, platform, post_delta, scraped_at],
        )
        if ok is None:
            await _note_rollup_failure("stats_keyword_daily", day=day, platform=platform, keyword_id=keyword_id)


async def record_comment(
    *,
    platform: str,
    keyword_id: str | None,
    scraped_at: str,
    is_new: bool,
) -> None:
    """Cộng bảng tổng hợp ngày theo nền tảng (+ từ khoá) sau khi ghi một comment."""
    day = _day_from_iso(scraped_at)
    if not day or not platform:
        return
    await ensure_stats_tables()
    comment_delta = 1 if is_new else 0
    if is_new:
        await _bump_hourly(platform, scraped_at, "comments")
    ok = await _q(
        """
        INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
        VALUES (?, ?, 0, ?, ?)
        ON CONFLICT(day, platform) DO UPDATE SET
            comments = comments + excluded.comments,
            last_scraped_at = CASE
                WHEN excluded.last_scraped_at > COALESCE(stats_platform_daily.last_scraped_at, '')
                THEN excluded.last_scraped_at
                ELSE stats_platform_daily.last_scraped_at
            END
        """,
        [day, platform, comment_delta, scraped_at],
    )
    if ok is None:
        await _note_rollup_failure("stats_platform_daily", day=day, platform=platform)
    if keyword_id:
        ok = await _q(
            """
            INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
            VALUES (?, ?, ?, 0, ?, ?)
            ON CONFLICT(day, keyword_id) DO UPDATE SET
                comments = comments + excluded.comments,
                platform = excluded.platform,
                last_scraped_at = CASE
                    WHEN excluded.last_scraped_at > COALESCE(stats_keyword_daily.last_scraped_at, '')
                    THEN excluded.last_scraped_at
                    ELSE stats_keyword_daily.last_scraped_at
                END
            """,
            [day, keyword_id, platform, comment_delta, scraped_at],
        )
        if ok is None:
            await _note_rollup_failure("stats_keyword_daily", day=day, platform=platform, keyword_id=keyword_id)


async def get_post_counts_by_platform() -> list[dict[str, Any]]:
    await ensure_stats_tables()
    rows = await _q(
        """
        SELECT platform,
               SUM(posts) AS count,
               MAX(last_scraped_at) AS last_scraped_at,
               SUM(CASE WHEN day = date('now') THEN posts ELSE 0 END) AS count_today,
               SUM(CASE WHEN day = date('now', '-1 day') THEN posts ELSE 0 END) AS count_prev
        FROM stats_platform_daily
        GROUP BY platform
        """
    )
    return [
        {
            **row,
            "count": _as_int(row.get("count")),
            "count_today": _as_int(row.get("count_today")),
            "count_prev": _as_int(row.get("count_prev")),
        }
        for row in (rows or [])
    ]


async def get_comment_counts_by_platform() -> list[dict[str, Any]]:
    await ensure_stats_tables()
    rows = await _q(
        """
        SELECT platform,
               SUM(comments) AS count,
               MAX(last_scraped_at) AS last_scraped_at,
               SUM(CASE WHEN day = date('now') THEN comments ELSE 0 END) AS count_today,
               SUM(CASE WHEN day = date('now', '-1 day') THEN comments ELSE 0 END) AS count_prev
        FROM stats_platform_daily
        GROUP BY platform
        """
    )
    return [
        {
            **row,
            "count": _as_int(row.get("count")),
            "count_today": _as_int(row.get("count_today")),
            "count_prev": _as_int(row.get("count_prev")),
        }
        for row in (rows or [])
    ]


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    await ensure_stats_tables()
    rows = await _q(
        """
        SELECT day, platform, posts AS count
        FROM stats_platform_daily
        WHERE day >= date('now', ?)
        ORDER BY day ASC
        """,
        [f"-{days} days"],
    )
    return rows or []


async def get_comment_timeseries(days: int) -> list[dict[str, Any]]:
    await ensure_stats_tables()
    rows = await _q(
        """
        SELECT day, platform, comments AS count
        FROM stats_platform_daily
        WHERE day >= date('now', ?)
        ORDER BY day ASC
        """,
        [f"-{days} days"],
    )
    return rows or []


async def get_keyword_volume(platform: str | None = None) -> list[dict[str, Any]]:
    """Cùng dạng response với phép join toàn bảng cũ, lấy dữ liệu từ bảng tổng hợp theo ngày."""
    from app.services.d1 import _related_hashtags_for_keywords

    await ensure_stats_tables()
    platform_sql = "AND k.platform = ?" if platform else ""
    platform_params: list[Any] = [platform] if platform else []
    rows = await _q(
        f"""
        SELECT
            k.id AS keyword_id,
            k.movie_id AS movie_id,
            k.keyword AS keyword,
            k.platform AS platform,
            k.enabled AS enabled,
            m.title AS movie_title,
            COALESCE(tot.posts_total, 0) AS posts_total,
            COALESCE(today.posts, 0) AS posts_today,
            COALESCE(prev.posts, 0) AS posts_prev,
            tot.last_scraped_at AS last_scraped_at,
            COALESCE(tot.comments_total, 0) AS comments_total,
            COALESCE(today.comments, 0) AS comments_today,
            COALESCE(prev.comments, 0) AS comments_prev
        FROM keywords k
        LEFT JOIN movies m ON m.id = k.movie_id
        LEFT JOIN (
            SELECT keyword_id,
                   SUM(posts) AS posts_total,
                   SUM(comments) AS comments_total,
                   MAX(last_scraped_at) AS last_scraped_at
            FROM stats_keyword_daily
            GROUP BY keyword_id
        ) tot ON tot.keyword_id = k.id
        LEFT JOIN stats_keyword_daily today
            ON today.keyword_id = k.id AND today.day = date('now')
        LEFT JOIN stats_keyword_daily prev
            ON prev.keyword_id = k.id AND prev.day = date('now', '-1 day')
        WHERE 1=1 {platform_sql}
        """,
        platform_params,
    )
    out: list[dict[str, Any]] = []
    for row in rows or []:
        posts_total = _as_int(row.get("posts_total"))
        enabled = bool(row.get("enabled"))
        if posts_total == 0 and not enabled:
            continue
        out.append(
            {
                "keyword_id": row["keyword_id"],
                "movie_id": row.get("movie_id") or "",
                "keyword": row.get("keyword") or "",
                "platform": row.get("platform") or "",
                "enabled": enabled,
                "movie_title": row.get("movie_title"),
                "posts_total": posts_total,
                "posts_today": _as_int(row.get("posts_today")),
                "posts_prev": _as_int(row.get("posts_prev")),
                "comments_total": _as_int(row.get("comments_total")),
                "comments_today": _as_int(row.get("comments_today")),
                "comments_prev": _as_int(row.get("comments_prev")),
                "last_scraped_at": row.get("last_scraped_at"),
                "related_hashtags": [],
            }
        )
    related_by_kw = await _related_hashtags_for_keywords(
        [row["keyword_id"] for row in out if row.get("platform") == "tiktok"]
    )
    for row in out:
        row["related_hashtags"] = related_by_kw.get(row["keyword_id"], [])
    out.sort(key=lambda r: (r["posts_today"], r["posts_total"]), reverse=True)
    return out


async def rebuild_from_source() -> dict[str, int]:
    """Xoá sạch + điền lại cả hai bảng tổng hợp từ posts/comments. Chạy bằng tay
    (scripts/rebuild_stats_summaries.py) và sau mỗi lượt dọn bài không liên quan có xoá
    dòng (app/services/cleanup.py). Các lệnh INSERT cho posts dùng upsert vì ingest
    consumer có thể tạo lại một dòng (day, platform) giữa lúc DELETE và lúc điền lại."""
    await ensure_stats_tables()
    # Phép tổng hợp toàn bảng có thể vượt timeout HTTP mặc định 10s của D1.
    slow = 120.0
    await _q("DELETE FROM stats_platform_daily", timeout=slow)
    await _q("DELETE FROM stats_keyword_daily", timeout=slow)

    # Bài theo nền tảng
    ok = await _q(
        """
        INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
        SELECT substr(scraped_at, 1, 10), platform, COUNT(*), 0, MAX(scraped_at)
        FROM posts
        WHERE scraped_at IS NOT NULL AND length(scraped_at) >= 10
        GROUP BY substr(scraped_at, 1, 10), platform
        ON CONFLICT(day, platform) DO UPDATE SET
            posts = excluded.posts,
            last_scraped_at = excluded.last_scraped_at
        """,
        timeout=slow,
    )
    if ok is None:
        raise RuntimeError("rebuild failed: platform posts aggregate")
    # Comment theo nền tảng (cộng vào các dòng day/platform đã có)
    ok = await _q(
        """
        INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
        SELECT substr(scraped_at, 1, 10), platform, 0, COUNT(*), MAX(scraped_at)
        FROM comments
        WHERE scraped_at IS NOT NULL AND length(scraped_at) >= 10
        GROUP BY substr(scraped_at, 1, 10), platform
        ON CONFLICT(day, platform) DO UPDATE SET
            comments = stats_platform_daily.comments + excluded.comments,
            last_scraped_at = CASE
                WHEN excluded.last_scraped_at > COALESCE(stats_platform_daily.last_scraped_at, '')
                THEN excluded.last_scraped_at
                ELSE stats_platform_daily.last_scraped_at
            END
        """,
        timeout=slow,
    )
    if ok is None:
        raise RuntimeError("rebuild failed: platform comments aggregate")
    # Bài theo từ khoá
    ok = await _q(
        """
        INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
        SELECT substr(scraped_at, 1, 10), keyword_id, platform, COUNT(*), 0, MAX(scraped_at)
        FROM posts
        WHERE keyword_id IS NOT NULL AND scraped_at IS NOT NULL AND length(scraped_at) >= 10
        GROUP BY substr(scraped_at, 1, 10), keyword_id, platform
        ON CONFLICT(day, keyword_id) DO UPDATE SET
            posts = excluded.posts,
            last_scraped_at = excluded.last_scraped_at
        """,
        timeout=slow,
    )
    if ok is None:
        raise RuntimeError("rebuild failed: keyword posts aggregate")
    # Comment theo từ khoá, qua bài cha
    ok = await _q(
        """
        INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
        SELECT substr(c.scraped_at, 1, 10), p.keyword_id, c.platform, 0, COUNT(*), MAX(c.scraped_at)
        FROM comments c
        INNER JOIN posts p ON p.id = c.post_id
        WHERE p.keyword_id IS NOT NULL AND c.scraped_at IS NOT NULL AND length(c.scraped_at) >= 10
        GROUP BY substr(c.scraped_at, 1, 10), p.keyword_id, c.platform
        ON CONFLICT(day, keyword_id) DO UPDATE SET
            comments = stats_keyword_daily.comments + excluded.comments,
            last_scraped_at = CASE
                WHEN excluded.last_scraped_at > COALESCE(stats_keyword_daily.last_scraped_at, '')
                THEN excluded.last_scraped_at
                ELSE stats_keyword_daily.last_scraped_at
            END
        """,
        timeout=slow,
    )
    if ok is None:
        raise RuntimeError("rebuild failed: keyword comments aggregate")

    platform_rows = await _q("SELECT COUNT(*) AS n FROM stats_platform_daily")
    keyword_rows = await _q("SELECT COUNT(*) AS n FROM stats_keyword_daily")
    return {
        "platform_days": _as_int((platform_rows or [{}])[0].get("n")),
        "keyword_days": _as_int((keyword_rows or [{}])[0].get("n")),
    }
