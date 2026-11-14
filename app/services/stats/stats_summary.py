"""Thống kê dashboard được tổng hợp sẵn cho D1 remote.

COUNT(*) toàn bảng posts/comments qua HTTP API của Cloudflare quá chậm cho trang
Overview (vài giây mỗi endpoint). Hai bảng tổng hợp theo ngày này đủ nhỏ để đọc trong
vài chục mili giây. Từ 30/10/2026 chúng KHÔNG còn được cộng theo từng bài/comment (mỗi lần
lưu tốn thêm 2 lần ghi D1 - hơn 200k lần ghi phụ mỗi ngày, góp phần làm D1 quá tải 7429):
reconcile_recent_days() đếm lại từ bảng gốc theo first_seen_at mỗi 10 phút (scheduler), nên
số trong ngày trễ tối đa ~10 phút.

    stats_platform_daily  (day, platform) -> posts, comments, last_scraped_at
    stats_keyword_daily   (day, keyword_id) -> platform, posts, comments, last_scraped_at

Dựng lại từ các dòng hiện có bằng:

    python -m scripts.jobs.rebuild_stats_summaries

Ngoài ra có bộ đếm theo giờ (biểu đồ "24 giờ gần nhất" của dashboard) nằm trong Redis
chứ không phải D1: mỗi bài/comment mới thêm một lượt HINCRBY, giữ HOURLY_RETENTION_HOURS
rồi tự hết hạn - không tốn thêm lượt ghi D1 nào. Không dựng lại được từ D1; bị mất thì
biểu đồ chỉ trống phần giờ đó.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    Column,
    Integer,
    Table,
    Text,
    and_,
    bindparam,
    case,
    delete,
    func,
    literal_column,
    select,
    text,
    update,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import ClauseElement

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.clients.telegram import send_telegram_message
from app.core.logging import get_logger
from app.repositories.d1.sql import compile_sql, metadata

# Bảng nguồn của phần tổng hợp - import với tên khác để không trùng với tham số
# `posts` / `comments` của subtract_deleted (biến trùng tên bảng làm dùng nhầm, lỗi bị
# `except` nuốt im lặng - xem docs/review-angles-va-attack.md mục 3).
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import posts as posts_table

logger = get_logger(__name__)

_ready = False
_ready_lock = asyncio.Lock()

# Lượt đối soát lỗi (D1 quá tải) để số liệu trên trang Overview đứng yên - báo Telegram một lần khi lỗi lặp lại trong cửa
# sổ trượt, để D1 sập kéo dài không spam kênh sau khi đã báo một lần.
_ROLLUP_FAILURE_ALERT_THRESHOLD = 5
_ROLLUP_FAILURE_WINDOW_SECONDS = 3600


async def _note_rollup_failure(table: str, **context: Any) -> None:
    logger.error("stats_rollup_write_failed", table=table, **context)
    try:
        client = get_redis_client()
        key = f"{REDIS_KEY_PREFIX}stats_rollup_failed:{table}"
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, _ROLLUP_FAILURE_WINDOW_SECONDS)
    except Exception as exc:  # noqa: BLE001 - chỉ là bộ đếm cảnh báo, không được làm hỏng lượt đối soát
        logger.warning("stats_rollup_failure_count_failed", error=str(exc) or repr(exc))
        return
    if count == _ROLLUP_FAILURE_ALERT_THRESHOLD:
        details = " | ".join(f"{k}={v}" for k, v in context.items())
        await send_telegram_message(
            f"🚨 Stats rollup write failing: {table}\n"
            f"{count} failures in the last {_ROLLUP_FAILURE_WINDOW_SECONDS // 60}m - dashboard counts are "
            f"drifting.\nNext reconcile retries in 10 minutes.\n{details}"
        )


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except TypeError, ValueError:
        return 0


async def _q(
    sql: str | ClauseElement, params: list[Any] | None = None, *, quiet: bool = False, timeout: float = 10.0
) -> list[dict[str, Any]] | None:
    """Mọi câu của module đi qua đây (test vá hàm này). Nhận chuỗi SQL hoặc câu dựng bằng SQLAlchemy."""
    # Import lười: repositories gọi vào module này từ persist_*/get_*.
    from app.repositories.d1.sql import compile_sql
    from app.repositories.d1.store import d1_query

    if not isinstance(sql, str):
        sql, params = compile_sql(sql)
    return await d1_query(sql, params, quiet=quiet, timeout=timeout)


def _stats_table(name: str, key: str) -> Table:
    key_columns = [Column("day", Text, primary_key=True), Column(key, Text, primary_key=True)]
    if key == "keyword_id":
        key_columns.append(Column("platform", Text, nullable=False))
    return Table(
        name,
        metadata,
        *key_columns,
        Column("posts", Integer, nullable=False, server_default=text("0")),
        Column("comments", Integer, nullable=False, server_default=text("0")),
        Column("last_scraped_at", Text),
    )


# Bảng tổng hợp theo ngày (UTC) cho dashboard: theo nền tảng và theo từ khoá.
platform_daily = _stats_table("stats_platform_daily", "platform")
keyword_daily = _stats_table("stats_keyword_daily", "keyword_id")
_TODAY = func.date("now")
_YESTERDAY = func.date("now", "-1 day")


async def ensure_stats_tables() -> None:
    """Tạo các bảng tổng hợp một lần mỗi tiến trình nếu còn thiếu."""
    global _ready
    if _ready:
        return
    async with _ready_lock:
        if _ready:
            return
        for table in (platform_daily, keyword_daily):
            await _q(CreateTable(table, if_not_exists=True), quiet=True)
        _ready = True


HOURLY_RETENTION_HOURS = 72
# Ngày đầu tiên mọi bài/comment mới đều có first_seen_at (cột thêm ngày 28/10/2026, deploy giữa ngày).
FIRST_SEEN_COMPLETE_SINCE = "2026-10-29"
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
    except Exception as exc:  # noqa: BLE001 - chỉ là số liệu biểu đồ, không được làm hỏng việc ingest
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
    except Exception as exc:  # noqa: BLE001 - chỉ là số liệu phễu, không được làm hỏng việc ingest
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
    """Đếm bài vào bộ đếm theo giờ (Redis) sau khi ghi - bảng tổng hợp ngày do reconcile_recent_days lo (xem docstring
    module). keyword_id giữ trong chữ ký cho bên gọi, không dùng nữa."""
    if not scraped_at or not platform:
        return
    await _bump_hourly(platform, scraped_at, "posts" if is_new else "updated")


async def record_comment(
    *,
    platform: str,
    keyword_id: str | None,
    scraped_at: str,
    is_new: bool,
) -> None:
    """Đếm comment MỚI vào bộ đếm theo giờ (Redis) - xem record_post."""
    if not scraped_at or not platform or not is_new:
        return
    await _bump_hourly(platform, scraped_at, "comments")


async def _counts_by_platform(metric: str) -> list[dict[str, Any]]:
    """Tổng `metric` (posts/comments) theo nền tảng, kèm hôm nay và hôm qua."""
    await ensure_stats_tables()
    t = platform_daily.c
    value = t[metric]
    rows = await _q(
        select(
            t.platform,
            func.sum(value).label("count"),
            func.max(t.last_scraped_at).label("last_scraped_at"),
            func.sum(case((t.day == _TODAY, value), else_=0)).label("count_today"),
            func.sum(case((t.day == _YESTERDAY, value), else_=0)).label("count_prev"),
        ).group_by(t.platform)
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


async def get_post_counts_by_platform() -> list[dict[str, Any]]:
    return await _counts_by_platform("posts")


async def get_comment_counts_by_platform() -> list[dict[str, Any]]:
    return await _counts_by_platform("comments")


async def _timeseries(metric: str, days: int) -> list[dict[str, Any]]:
    await ensure_stats_tables()
    t = platform_daily.c
    rows = await _q(
        select(t.day, t.platform, t[metric].label("count"))
        .where(t.day >= func.date("now", f"-{days} days"))
        .order_by(t.day)
    )
    return rows or []


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    return await _timeseries("posts", days)


async def get_comment_timeseries(days: int) -> list[dict[str, Any]]:
    return await _timeseries("comments", days)


async def get_keyword_volume(platform: str | None = None) -> list[dict[str, Any]]:
    """Cùng dạng response với phép join toàn bảng cũ, lấy dữ liệu từ bảng tổng hợp theo ngày."""
    from app.repositories.d1.store import related_hashtags_for_keywords

    await ensure_stats_tables()
    from app.repositories.d1.tables import keywords, movies

    kd = keyword_daily.c
    totals = (
        select(
            kd.keyword_id,
            func.sum(kd.posts).label("posts_total"),
            func.sum(kd.comments).label("comments_total"),
            func.max(kd.last_scraped_at).label("last_scraped_at"),
        )
        .group_by(kd.keyword_id)
        .subquery("tot")
    )
    today, prev = keyword_daily.alias("today"), keyword_daily.alias("prev")
    k, m = keywords.alias("k"), movies.alias("m")
    query = select(
        k.c.id.label("keyword_id"),
        k.c.movie_id,
        k.c.keyword,
        k.c.platform,
        k.c.enabled,
        m.c.title.label("movie_title"),
        func.coalesce(totals.c.posts_total, 0).label("posts_total"),
        func.coalesce(today.c.posts, 0).label("posts_today"),
        func.coalesce(prev.c.posts, 0).label("posts_prev"),
        totals.c.last_scraped_at,
        func.coalesce(totals.c.comments_total, 0).label("comments_total"),
        func.coalesce(today.c.comments, 0).label("comments_today"),
        func.coalesce(prev.c.comments, 0).label("comments_prev"),
    ).select_from(
        k.outerjoin(m, m.c.id == k.c.movie_id)
        .outerjoin(totals, totals.c.keyword_id == k.c.id)
        .outerjoin(today, and_(today.c.keyword_id == k.c.id, today.c.day == _TODAY))
        .outerjoin(prev, and_(prev.c.keyword_id == k.c.id, prev.c.day == _YESTERDAY))
    )
    if platform:
        query = query.where(k.c.platform == platform)
    rows = await _q(query)
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
    related_by_kw = await related_hashtags_for_keywords(
        [row["keyword_id"] for row in out if row.get("platform") == "tiktok"]
    )
    for row in out:
        row["related_hashtags"] = related_by_kw.get(row["keyword_id"], [])
    out.sort(key=lambda r: (r["posts_today"], r["posts_total"]), reverse=True)
    return out


# --- các câu tổng hợp (INSERT ... SELECT ... ON CONFLICT) --------------------
# Ngày của một dòng = 10 ký tự đầu của first_seen_at, không có thì scraped_at (dòng cũ hơn
# cột first_seen_at). Ngày tính bằng SQL để INDEX (day, ...) dùng được.
_DAY = literal_column("1")
_DAY_LEN = literal_column("10")
_ZERO = literal_column("0")


def _day_of(first_seen: Any, scraped: Any) -> Any:
    """substr(COALESCE(first_seen_at, scraped_at), 1, 10) - ngày (UTC) của một dòng."""
    return func.substr(func.coalesce(first_seen, scraped), _DAY, _DAY_LEN)


def _platform_posts_insert() -> Any:
    """Bài theo (day, platform) - upsert, ghi đè posts/last_scraped_at (dòng cũ có thể đã có)."""
    day = _day_of(posts_table.c.first_seen_at, posts_table.c.scraped_at)
    source = (
        select(
            day.label("day"),
            posts_table.c.platform.label("platform"),
            func.count().label("posts"),
            _ZERO.label("comments"),
            func.max(posts_table.c.scraped_at).label("last_scraped_at"),
        )
        .where(posts_table.c.scraped_at.is_not(None), func.length(posts_table.c.scraped_at) >= 10)
        .group_by(day, posts_table.c.platform)
    )
    stmt = sqlite_insert(platform_daily).from_select(
        ["day", "platform", "posts", "comments", "last_scraped_at"], source
    )
    return stmt.on_conflict_do_update(
        index_elements=["day", "platform"],
        set_={"posts": stmt.excluded.posts, "last_scraped_at": stmt.excluded.last_scraped_at},
    )


def _platform_comments_insert() -> Any:
    """Comment theo (day, platform) - CỘNG DỒN vào dòng bài đã có (comments = cũ + excluded)."""
    day = _day_of(comments_table.c.first_seen_at, comments_table.c.scraped_at)
    source = (
        select(
            day.label("day"),
            comments_table.c.platform.label("platform"),
            _ZERO.label("posts"),
            func.count().label("comments"),
            func.max(comments_table.c.scraped_at).label("last_scraped_at"),
        )
        .where(comments_table.c.scraped_at.is_not(None), func.length(comments_table.c.scraped_at) >= 10)
        .group_by(day, comments_table.c.platform)
    )
    stmt = sqlite_insert(platform_daily).from_select(
        ["day", "platform", "posts", "comments", "last_scraped_at"], source
    )
    newer = stmt.excluded.last_scraped_at > func.coalesce(platform_daily.c.last_scraped_at, "")
    return stmt.on_conflict_do_update(
        index_elements=["day", "platform"],
        set_={
            "comments": platform_daily.c.comments + stmt.excluded.comments,
            "last_scraped_at": case((newer, stmt.excluded.last_scraped_at), else_=platform_daily.c.last_scraped_at),
        },
    )


def _keyword_posts_insert() -> Any:
    """Bài theo (day, keyword_id) - upsert, ghi đè posts/last_scraped_at."""
    day = _day_of(posts_table.c.first_seen_at, posts_table.c.scraped_at)
    source = (
        select(
            day.label("day"),
            posts_table.c.keyword_id.label("keyword_id"),
            posts_table.c.platform.label("platform"),
            func.count().label("posts"),
            _ZERO.label("comments"),
            func.max(posts_table.c.scraped_at).label("last_scraped_at"),
        )
        .where(
            posts_table.c.keyword_id.is_not(None),
            posts_table.c.scraped_at.is_not(None),
            func.length(posts_table.c.scraped_at) >= 10,
        )
        .group_by(day, posts_table.c.keyword_id, posts_table.c.platform)
    )
    stmt = sqlite_insert(keyword_daily).from_select(
        ["day", "keyword_id", "platform", "posts", "comments", "last_scraped_at"], source
    )
    return stmt.on_conflict_do_update(
        index_elements=["day", "keyword_id"],
        set_={"posts": stmt.excluded.posts, "last_scraped_at": stmt.excluded.last_scraped_at},
    )


def _keyword_comments_insert() -> Any:
    """Comment theo (day, keyword_id), qua bài cha - CỘNG DỒN vào dòng bài đã có."""
    c, p = comments_table.alias("c"), posts_table.alias("p")
    day = _day_of(c.c.first_seen_at, c.c.scraped_at)
    source = (
        select(
            day.label("day"),
            p.c.keyword_id.label("keyword_id"),
            c.c.platform.label("platform"),
            _ZERO.label("posts"),
            func.count().label("comments"),
            func.max(c.c.scraped_at).label("last_scraped_at"),
        )
        .select_from(c.join(p, p.c.id == c.c.post_id))
        .where(
            p.c.keyword_id.is_not(None),
            c.c.scraped_at.is_not(None),
            func.length(c.c.scraped_at) >= 10,
        )
        .group_by(day, p.c.keyword_id, c.c.platform)
    )
    stmt = sqlite_insert(keyword_daily).from_select(
        ["day", "keyword_id", "platform", "posts", "comments", "last_scraped_at"], source
    )
    newer = stmt.excluded.last_scraped_at > func.coalesce(keyword_daily.c.last_scraped_at, "")
    return stmt.on_conflict_do_update(
        index_elements=["day", "keyword_id"],
        set_={
            "comments": keyword_daily.c.comments + stmt.excluded.comments,
            "last_scraped_at": case((newer, stmt.excluded.last_scraped_at), else_=keyword_daily.c.last_scraped_at),
        },
    )


async def rebuild_from_source() -> dict[str, int]:
    """Xoá sạch + điền lại cả hai bảng tổng hợp từ posts/comments. Chạy bằng tay
    (scripts/jobs/rebuild_stats_summaries.py) và sau mỗi lượt dọn bài không liên quan có xoá
    dòng (app/services/relevance/cleanup.py). Các lệnh INSERT cho posts dùng upsert vì ingest
    consumer có thể tạo lại một dòng (day, platform) giữa lúc DELETE và lúc điền lại.

    Ngày = first_seen_at (ngày thấy lần đầu). Dòng cũ hơn cột đó không có first_seen_at nên rơi
    về scraped_at - tức ngày cào lại gần nhất, không phải ngày thu thập thật; số các ngày đó chỉ là xấp xỉ."""
    await ensure_stats_tables()
    # Phép tổng hợp toàn bảng có thể vượt timeout HTTP mặc định 10s của D1.
    slow = 120.0
    await _q(delete(platform_daily), timeout=slow)
    await _q(delete(keyword_daily), timeout=slow)

    # Bài theo nền tảng
    ok = await _q(_platform_posts_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: platform posts aggregate")
    # Comment theo nền tảng (cộng vào các dòng day/platform đã có)
    ok = await _q(_platform_comments_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: platform comments aggregate")
    # Bài theo từ khoá
    ok = await _q(_keyword_posts_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: keyword posts aggregate")
    # Comment theo từ khoá, qua bài cha
    ok = await _q(_keyword_comments_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: keyword comments aggregate")

    platform_rows = await _q(select(func.count().label("n")).select_from(platform_daily))
    keyword_rows = await _q(select(func.count().label("n")).select_from(keyword_daily))
    return {
        "platform_days": _as_int((platform_rows or [{}])[0].get("n")),
        "keyword_days": _as_int((keyword_rows or [{}])[0].get("n")),
    }


async def subtract_deleted(posts: list[dict[str, Any]], comments: list[dict[str, Any]]) -> None:
    """Trừ các bài/comment vừa bị xoá khỏi hai bảng tổng hợp (theo ngày first_seen_at, không có thì scraped_at - cùng cách
    rebuild_from_source xếp ngày), không bao giờ xuống dưới 0. Rẻ hơn hẳn dựng lại toàn bộ sau mỗi lượt dọn."""

    def day(row: dict[str, Any]) -> str:
        return str(row.get("first_seen_at") or row.get("scraped_at") or "")[:10]

    keyword_of = {p["id"]: p.get("keyword_id") for p in posts}
    platform_rows: dict[tuple[str, str, str], int] = {}
    keyword_rows: dict[tuple[str, str, str], int] = {}
    for kind, rows in (("posts", posts), ("comments", comments)):
        for row in rows:
            platform_rows[(kind, day(row), row.get("platform") or "")] = (
                platform_rows.get((kind, day(row), row.get("platform") or ""), 0) + 1
            )
            keyword_id = row.get("keyword_id") if kind == "posts" else keyword_of.get(row.get("post_id"))
            if keyword_id:
                keyword_rows[(kind, day(row), keyword_id)] = keyword_rows.get((kind, day(row), keyword_id), 0) + 1
    for (kind, d, platform), n in platform_rows.items():
        table = platform_daily
        sql, params = compile_sql(
            update(table)
            .where(table.c.day == d, table.c.platform == platform)
            .values(**{kind: func.max(_ZERO, table.c[kind] - n)})
        )
        await _q(sql, params)
    for (kind, d, keyword_id), n in keyword_rows.items():
        table = keyword_daily
        sql, params = compile_sql(
            update(table)
            .where(table.c.day == d, table.c.keyword_id == keyword_id)
            .values(**{kind: func.max(_ZERO, table.c[kind] - n)})
        )
        await _q(sql, params)


def _reconcile_statements(since: str) -> tuple[tuple[Any, str], ...]:
    """Bốn câu upsert GHI ĐÈ (không cộng dồn, không DELETE) cho các ngày gần nhất.

    Mỗi câu dùng CHUNG một tham số `since` (ngày bắt đầu) - bind param tên "since" để cả bốn câu
    có cùng một bộ tham số, đối chiếu được khi D1 quá tải giữa chừng."""
    since_param = bindparam("since", value=since)

    def platform(source: Any, metric: str) -> Any:
        stmt = sqlite_insert(platform_daily).from_select(
            ["day", "platform", "posts", "comments", "last_scraped_at"], source
        )
        return stmt.on_conflict_do_update(
            index_elements=["day", "platform"],
            set_={
                metric: stmt.excluded[metric],
                "last_scraped_at": func.max(
                    func.coalesce(platform_daily.c.last_scraped_at, ""), stmt.excluded.last_scraped_at
                ),
            },
        )

    def keyword(source: Any, metric: str) -> Any:
        stmt = sqlite_insert(keyword_daily).from_select(
            ["day", "keyword_id", "platform", "posts", "comments", "last_scraped_at"], source
        )
        return stmt.on_conflict_do_update(
            index_elements=["day", "keyword_id"],
            set_={
                metric: stmt.excluded[metric],
                "last_scraped_at": func.max(
                    func.coalesce(keyword_daily.c.last_scraped_at, ""), stmt.excluded.last_scraped_at
                ),
            },
        )

    p, c = posts_table, comments_table
    p_day = func.substr(p.c.first_seen_at, _DAY, _DAY_LEN)
    c_day = func.substr(c.c.first_seen_at, _DAY, _DAY_LEN)

    platform_posts = (
        select(
            p_day.label("day"),
            p.c.platform.label("platform"),
            func.count().label("posts"),
            _ZERO.label("comments"),
            func.max(p.c.scraped_at).label("last_scraped_at"),
        )
        .where(p.c.scraped_at >= since_param, p.c.first_seen_at >= since_param)
        .group_by(p_day, p.c.platform)
    )
    platform_comments = (
        select(
            c_day.label("day"),
            c.c.platform.label("platform"),
            _ZERO.label("posts"),
            func.count().label("comments"),
            func.max(c.c.scraped_at).label("last_scraped_at"),
        )
        .where(c.c.scraped_at >= since_param, c.c.first_seen_at >= since_param)
        .group_by(c_day, c.c.platform)
    )
    keyword_posts = (
        select(
            p_day.label("day"),
            p.c.keyword_id.label("keyword_id"),
            p.c.platform.label("platform"),
            func.count().label("posts"),
            _ZERO.label("comments"),
            func.max(p.c.scraped_at).label("last_scraped_at"),
        )
        .where(
            p.c.keyword_id.is_not(None),
            p.c.scraped_at >= since_param,
            p.c.first_seen_at >= since_param,
        )
        .group_by(p_day, p.c.keyword_id, p.c.platform)
    )
    ca, pa = c.alias("c"), p.alias("p")
    ca_day = func.substr(ca.c.first_seen_at, _DAY, _DAY_LEN)
    keyword_comments = (
        select(
            ca_day.label("day"),
            pa.c.keyword_id.label("keyword_id"),
            ca.c.platform.label("platform"),
            _ZERO.label("posts"),
            func.count().label("comments"),
            func.max(ca.c.scraped_at).label("last_scraped_at"),
        )
        .select_from(ca.join(pa, pa.c.id == ca.c.post_id))
        .where(
            pa.c.keyword_id.is_not(None),
            ca.c.scraped_at >= since_param,
            ca.c.first_seen_at >= since_param,
        )
        .group_by(ca_day, pa.c.keyword_id, ca.c.platform)
    )
    return (
        (platform(platform_posts, "posts"), "platform posts"),
        (platform(platform_comments, "comments"), "platform comments"),
        (keyword(keyword_posts, "posts"), "keyword posts"),
        (keyword(keyword_comments, "comments"), "keyword comments"),
    )


async def reconcile_recent_days(days: int = 2) -> bool:
    """Tính lại hai bảng tổng hợp cho `days` ngày gần nhất (UTC, giống cột day) từ posts/comments - bản nhẹ của
    rebuild_from_source để chạy mỗi giờ (D1 quá tải "7429 overloaded" làm các lần cộng dồn của ingest thất
    bại -> bảng tổng hợp đếm thiếu, dashboard hiện số sai). Chỉ quét các ngày đó qua index scraped_at, không đụng ngày cũ.
    Không DELETE: mọi upsert GHI ĐÈ giá trị bằng số đếm từ bảng gốc, nên một bước lỗi giữa chừng (đúng lúc D1 quá tải)
    chỉ để lại số cũ, không xoá trắng số liệu hôm nay.

    Đếm theo NGÀY THẤY LẦN ĐẦU (first_seen_at), giống record_post/record_comment chỉ cộng dòng mới. scraped_at bị
    ghi lại mỗi lần cào lại bài/comment cũ, nên đếm theo nó (bản 28/10) biến cả chục nghìn lượt cập nhật TikTok thành
    "bài mới trong ngày". Lọc thêm scraped_at >= since chỉ để dùng index (first_seen_at <= scraped_at luôn đúng)."""
    await ensure_stats_tables()
    since = (datetime.now(tz=UTC).date() - timedelta(days=max(days, 1) - 1)).isoformat()
    # Trước ngày này first_seen_at chưa ghi đủ cho mọi dòng mới - đừng ghi đè các ngày đó bằng số đếm thiếu.
    since = max(since, FIRST_SEEN_COMPLETE_SINCE)
    slow = 60.0
    for stmt, label in _reconcile_statements(since):
        # Biến câu thành chuỗi + tham số ngay ở đây: cả bốn câu dùng CHUNG một bộ tham số
        # (đối chiếu được khi D1 quá tải giữa chừng), và _q nhận cả hai kiểu.
        sql, params = compile_sql(stmt)
        if await _q(sql, params, timeout=slow) is None:
            await _note_rollup_failure("stats_reconcile", step=label, since=since)
            return False
    logger.info("stats_reconciled", since=since)
    return True
