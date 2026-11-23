"""Dựng lại và đối soát bảng thống kê theo ngày từ dữ liệu nguồn: dựng toàn bộ, trừ phần đã xoá, đối soát các ngày gần đây."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    bindparam,
    case,
    delete,
    func,
    literal_column,
    select,
    update,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.core.logging import get_logger
from app.repositories.d1.sql import compile_sql
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import posts as posts_table
from app.services.stats.stats_hourly import FIRST_SEEN_COMPLETE_SINCE
from app.services.stats.stats_tables import (
    _as_int,
    ensure_stats_tables,
    keyword_daily,
    note_rollup_failure,
    platform_daily,
    run_stats_query,
)

logger = get_logger(__name__)

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
    await run_stats_query(delete(platform_daily), timeout=slow)
    await run_stats_query(delete(keyword_daily), timeout=slow)

    # Bài theo nền tảng
    ok = await run_stats_query(_platform_posts_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: platform posts aggregate")
    # Comment theo nền tảng (cộng vào các dòng day/platform đã có)
    ok = await run_stats_query(_platform_comments_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: platform comments aggregate")
    # Bài theo từ khoá
    ok = await run_stats_query(_keyword_posts_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: keyword posts aggregate")
    # Comment theo từ khoá, qua bài cha
    ok = await run_stats_query(_keyword_comments_insert(), timeout=slow)
    if ok is None:
        raise RuntimeError("rebuild failed: keyword comments aggregate")

    platform_rows = await run_stats_query(select(func.count().label("n")).select_from(platform_daily))
    keyword_rows = await run_stats_query(select(func.count().label("n")).select_from(keyword_daily))
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
        await run_stats_query(sql, params)
    for (kind, d, keyword_id), n in keyword_rows.items():
        table = keyword_daily
        sql, params = compile_sql(
            update(table)
            .where(table.c.day == d, table.c.keyword_id == keyword_id)
            .values(**{kind: func.max(_ZERO, table.c[kind] - n)})
        )
        await run_stats_query(sql, params)


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
        # (đối chiếu được khi D1 quá tải giữa chừng), và run_stats_query nhận cả hai kiểu.
        sql, params = compile_sql(stmt)
        if await run_stats_query(sql, params, timeout=slow) is None:
            await note_rollup_failure("stats_reconcile", step=label, since=since)
            return False
    logger.info("stats_reconciled", since=since)
    return True
