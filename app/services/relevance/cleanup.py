"""Dọn dữ liệu bài không liên quan khỏi D1 hằng ngày.

Xoá các bài mà pipeline phân loại độ liên quan đã gắn nhãn not_related (cùng comment
và snapshot tương tác của chúng) sau khi mang nhãn đó đủ grace_hours. Mọi đường đọc
vốn đã lọc bỏ những bài này (RELEVANT_POST_SQL trong app/repositories/d1/posts.py,
query social-topic của worker), nên chúng chỉ tốn chỗ lưu và lượt đọc dòng trên D1.

Mỗi lượt chạy quét một lần để chọn ra các id cần xoá (tối đa MAX_BATCHES *
BATCH_SIZE, id cũ nhất trước); sau đó xoá theo từng lô trên đúng các id đó, bảng con
trước bài, để không bao giờ còn sót comment của một bài đã bị xoá, và nếu crash giữa
chừng thì lượt sau chỉ việc làm tiếp. Hàng tồn lớn được xử lý dần qua nhiều ngày thay
vì trong một lượt dài - mỗi câu lệnh nằm gọn trong giới hạn thời gian mỗi câu lệnh
của D1, và cả lượt chạy luôn thấp xa so với giới hạn rate của Cloudflare API mà
dashboard và ingest dùng chung.

Việc dọn `dropped_posts` lịch sử đã bỏ - lake writer (app/workers/lake_writer/main.py)
giờ giữ kho lưu trữ mọi quyết định loại bài qua topic Kafka ingest_decisions (xem
app/clients/kafka.py:publish_ingest_decision), nên trong D1 không còn gì để xoá theo
tuổi nữa."""

from __future__ import annotations

import gzip
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import bindparam, delete, func, literal_column, select, text, update

from app.clients.lake import lake_configured, put_object
from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.sql import compile_sql, json_values
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import movies, post_comment_value, post_engagement_snapshots, post_visuals
from app.repositories.d1.tables import posts as posts_table
from app.services.accounts import platform_config_db as db
from app.services.relevance.relevance_rules import film_context_reason
from app.services.stats.stats_summary import subtract_deleted

logger = get_logger(__name__)

BATCH_SIZE = 500
MAX_BATCHES = 60

# Bảng post_comment_value và post_visuals khai ở app/repositories/d1/tables.py (dùng chung với
# crawl/comment_value.py và scripts/jobs/post_visuals.py).

# Thời gian ân hạn tính từ lúc bài bị gắn nhãn not_related, không phải lúc được crawl -
# một lượt gắn nhãn lại hạ cấp các bài cũ vẫn phải chừa cho người vận hành đủ
# grace_hours để phát hiện và hoàn tác.
# Mốc ân hạn lấy bằng strftime('now', ?) - giữ nguyên dạng SQL thuần vì đây là điều kiện
# dùng lại ở nhiều câu; :grace là tham số do SQLAlchemy bind khi dựng câu.
_ELIGIBLE_SQL = (
    "{prefix}relevance_label = 'not_related' "
    "AND COALESCE({prefix}relevance_labeled_at, {prefix}scraped_at) "
    "< strftime('%Y-%m-%dT%H:%M:%S', 'now', :grace)"
)

# Dùng chung giữa scheduler của API và scripts/tools/purge_irrelevant_posts.py, để một lượt
# chạy tay không chạy chồng lên lượt theo lịch ở tiến trình khác.
_LOCK_KEY = "cinemark_api:cleanup:irrelevant_posts:lock"
# Bài trước, rồi các bảng con (xoá theo thứ tự ngược lại: con trước bài).
_BACKUP_TABLES = (
    ("posts", "id"),
    ("comments", "post_id"),
    ("post_engagement_snapshots", "post_id"),
    ("post_comment_value", "post_id"),
    ("post_visuals", "post_id"),
)
# Bảng SQLAlchemy tương ứng từng bảng trong _BACKUP_TABLES - cùng thứ tự.
_BACKUP_TABLE_OBJECTS = {
    "posts": posts_table,
    "comments": comments_table,
    "post_engagement_snapshots": post_engagement_snapshots,
    "post_comment_value": post_comment_value,
    "post_visuals": post_visuals,
}
_LOCK_TTL_SECONDS = 2 * 60 * 60


async def _run(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    """Chạy câu đã dựng bằng SQLAlchemy (compile_sql ở _run_stmt) - đây là ngõ duy nhất của module này
    gọi D1, nên timeout và cách báo lỗi giống nhau cho mọi câu."""
    from app.repositories.d1.store import d1_query

    rows = await d1_query(sql, params, timeout=60.0)
    if rows is None:
        raise RuntimeError(f"d1 statement failed: {sql[:80]}")
    return rows


async def _run_stmt(stmt: Any) -> list[dict[str, Any]]:
    """Chạy câu dựng bằng SQLAlchemy (dùng _run để có cùng timeout và cùng cách báo lỗi)."""
    sql, params = compile_sql(stmt)
    return await _run(sql, params)


def _eligible_posts(alias: Any, grace: str) -> Any:
    """Điều kiện "bài đã gắn nhãn not_related đủ lâu" cho bảng posts (alias p). Mốc ân hạn lấy bằng
    strftime('now', ?) - giữ dạng SQL thuần vì đây là điều kiện dùng lại ở nhiều câu."""
    return text(_ELIGIBLE_SQL.format(prefix=f"{alias.name}.")).bindparams(bindparam("grace", value=grace))


def _count_eligible(grace: str) -> Any:
    """Số bài đủ điều kiện xoá. select_from bắt buộc: điều kiện là text() nên SQLAlchemy không tự suy ra FROM, thiếu nó
    câu thành "SELECT count(*) WHERE posts...." - D1 báo no such column."""
    return select(func.count().label("n")).select_from(posts_table).where(_eligible_posts(posts_table, grace))


async def resolve_cleanup_settings() -> dict[str, Any]:
    """Các tham số dọn dẹp đang có hiệu lực: dòng cleanup_settings lưu trên dashboard cho
    những key nó có, còn lại lấy giá trị dự phòng từ env trong app/core/config.py. Luôn
    cùng một dạng, nên chỗ gọi không cần rẽ nhánh."""
    row = await db.get_cleanup_settings()
    stored = value if isinstance(value := row.get("settings"), dict) else {}
    grace = stored.get("grace_hours")
    return {
        "run_time": str(stored.get("run_time") or settings.irrelevant_post_purge_time),
        "enabled": bool(stored.get("enabled", settings.irrelevant_post_purge_enabled)),
        # Dùng `is None`, không dùng `or`: 0 là giá trị lưu hợp lệ ("xoá ngay").
        "grace_hours": int(grace if grace is not None else settings.irrelevant_post_grace_hours),
        "updated_at": row.get("updated_at"),
    }


async def purge_irrelevant_posts(
    *,
    dry_run: bool = False,
    grace_hours: int | None = None,
) -> dict[str, Any]:
    """Một lượt dọn. grace_hours mặc định lấy giá trị dự phòng từ env - những chỗ gọi cần
    tôn trọng dashboard (scheduler, nút Run-now, script) thì đi qua run_purge, nơi lấy
    giá trị đã lưu."""
    grace_value = max(0, grace_hours if grace_hours is not None else settings.irrelevant_post_grace_hours)
    grace = f"-{grace_value} hours"

    if dry_run:
        posts = await _run_stmt(_count_eligible(grace))
        # Một bảng comments, không alias: alias + điều kiện trên bảng gốc sinh "FROM comments AS c, comments"
        # (tích Descartes - quét cả bảng nhân số dòng khớp).
        comments = await _run_stmt(
            select(func.count().label("n"))
            .select_from(comments_table)
            .where(comments_table.c.post_id.in_(select(posts_table.c.id).where(_eligible_posts(posts_table, grace))))
        )
        result = {"dry_run": True, "grace_hours": grace_value, "posts": posts[0]["n"], "comments": comments[0]["n"]}
        logger.info("irrelevant_purge_dry_run", **result)
        return result

    p, m = posts_table.alias("p"), movies.alias("m")
    candidates = await _run_stmt(
        select(
            p.c.id,
            func.substr(p.c.content, 1, 1500).label("content"),
            m.c.title,
            m.c.director,
            m.c.cast.label("cast"),
        )
        .select_from(p.outerjoin(m, m.c.id == p.c.movie_id))
        .where(_eligible_posts(p, grace))
        .order_by(p.c.id)
        .limit(BATCH_SIZE * MAX_BATCHES)
    )
    # bài nhắc tên diễn viên/đạo diễn của chính phim đó thì KHÔNG xoá dù bộ phân loại nói không liên quan -
    # chuyển về 'uncertain' (vẫn ẩn, gán lại được) để không bị chọn lại mỗi đêm.
    named = [
        row["id"]
        for row in candidates
        if film_context_reason(row.get("content"), row, allow_title_hashtag=False) in ("cast", "director")
    ]
    if named:
        await _run_stmt(
            update(posts_table)
            .where(posts_table.c.id.in_(json_values(named)))
            .values(relevance_label="uncertain", keyword_match=0)
        )
    ids = [row["id"] for row in candidates if row["id"] not in set(named)]
    totals: dict[str, Any] = {"posts": 0, "comments": 0, "snapshots": 0, "batches": 0, "kept_named": len(named)}
    if ids and not lake_configured():
        # Không có chỗ sao lưu thì không xoá - dữ liệu xoá khỏi D1 là mất hẳn.
        raise RuntimeError("purge needs the R2 lake configured for backups")
    run_key = f"backups/purge/{datetime.now(UTC):%Y-%m-%d}/{uuid.uuid4().hex[:8]}"
    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start : start + BATCH_SIZE]
        # Sao lưu lô lên R2 TRƯỚC khi xoá: bài + mọi bảng con, mỗi bảng một file jsonl.gz.
        backup: dict[str, list[dict[str, Any]]] = {}
        for table, column in _BACKUP_TABLES:
            obj = _BACKUP_TABLE_OBJECTS[table]
            backup[table] = await _run_stmt(
                select(literal_column("*")).select_from(obj).where(obj.c[column].in_(json_values(batch)))
            )
            body = gzip.compress("\n".join(json.dumps(r, ensure_ascii=False) for r in backup[table]).encode())
            await put_object(f"{run_key}/{totals['batches']:03d}-{table}.jsonl.gz", body)
        for table, column in _BACKUP_TABLES[1:]:
            obj = _BACKUP_TABLE_OBJECTS[table]
            deleted = len(
                await _run_stmt(
                    delete(obj).where(obj.c[column].in_(json_values(batch))).returning(literal_column("1").label("x"))
                )
            )
            if table == "comments":
                totals["comments"] += deleted
            elif table == "post_engagement_snapshots":
                totals["snapshots"] += deleted
        totals["posts"] += len(
            await _run_stmt(
                delete(posts_table).where(posts_table.c.id.in_(json_values(batch))).returning(posts_table.c.id)
            )
        )
        totals["batches"] += 1
        # Trừ đúng phần đã xoá khỏi bảng thống kê ngày (trước 30/10/2026 là dựng lại toàn bộ - quét 2 triệu bình luận).
        try:
            await subtract_deleted(backup["posts"], backup["comments"])
        except Exception as exc:  # noqa: BLE001 - dữ liệu đã xoá; số thống kê lệch chỉ được báo
            logger.error("irrelevant_purge_stats_subtract_failed", error=str(exc))
    if ids:
        totals["backup"] = run_key

    backlog = await _run_stmt(_count_eligible(grace))
    totals["remaining_posts"] = backlog[0]["n"]
    totals["grace_hours"] = grace_value
    logger.info("irrelevant_purge_done", telegram=True, **totals)
    return totals


async def run_purge(*, triggered_by: str) -> dict[str, Any] | None:
    """Chạy trọn một lượt dọn theo đúng cách scheduler, nút Run-now trên dashboard và script
    chạy tay đều làm: lấy khoá liên tiến trình, lấy grace_hours của dashboard, ghi lượt
    chạy vào cleanup_run_history. Trả về bản tổng kết, hoặc None khi đang có lượt khác
    giữ khoá. Lỗi được ghi vào dòng lịch sử rồi raise lại."""
    redis = get_redis_client()
    token = uuid.uuid4().hex
    if not await redis.set(_LOCK_KEY, token, nx=True, ex=_LOCK_TTL_SECONDS):
        logger.warning("irrelevant_purge_already_running", triggered_by=triggered_by)
        return None
    run_id: int | None = None
    try:
        cfg = await resolve_cleanup_settings()
        run_id = await db.record_cleanup_run_start(dry_run=False, triggered_by=triggered_by)
        summary = await purge_irrelevant_posts(grace_hours=cfg["grace_hours"])
        await db.record_cleanup_run_finish(run_id, summary=summary)
        return summary
    except Exception as exc:
        if run_id is not None:
            try:
                await db.record_cleanup_run_finish(run_id, summary={}, error=str(exc))
            except Exception as history_exc:  # noqa: BLE001 - không được che mất lỗi gốc
                # Không được che mất lỗi gốc.
                logger.error("irrelevant_purge_history_write_failed", error=str(history_exc))
        raise
    finally:
        if await redis.get(_LOCK_KEY) == token:
            await redis.delete(_LOCK_KEY)
