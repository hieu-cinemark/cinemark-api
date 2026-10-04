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

import uuid
from typing import Any

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services import platform_config_db as db
from app.services.d1 import d1_query
from app.services.stats_summary import rebuild_from_source

logger = get_logger(__name__)

BATCH_SIZE = 500
MAX_BATCHES = 60

# Thời gian ân hạn tính từ lúc bài bị gắn nhãn not_related, không phải lúc được crawl -
# một lượt gắn nhãn lại hạ cấp các bài cũ vẫn phải chừa cho người vận hành đủ
# grace_hours để phát hiện và hoàn tác.
_ELIGIBLE = (
    "relevance_label = 'not_related' "
    "AND COALESCE(relevance_labeled_at, scraped_at) < strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)"
)

# Dùng chung giữa scheduler của API và scripts/purge_irrelevant_posts.py, để một lượt
# chạy tay không chạy chồng lên lượt theo lịch ở tiến trình khác.
_LOCK_KEY = "cinemark_api:cleanup:irrelevant_posts:lock"
_LOCK_TTL_SECONDS = 2 * 60 * 60


async def _run(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    rows = await d1_query(sql, params, timeout=60.0)
    if rows is None:
        raise RuntimeError(f"d1 statement failed: {sql[:80]}")
    return rows


def _id_list(ids: list[str]) -> str:
    # Nhúng thẳng thành literal trong dấu nháy thay vì bind tham số: D1 giới hạn một câu
    # lệnh tối đa 100 tham số bind, và mỗi lô một câu lệnh giúp một lượt chạy chỉ tốn vài
    # trăm lời gọi API. Các id lấy thẳng từ câu SELECT ở trên.
    return ",".join("'" + str(i).replace("'", "''") + "'" for i in ids)


async def resolve_cleanup_settings() -> dict[str, Any]:
    """Các tham số dọn dẹp đang có hiệu lực: dòng cleanup_settings lưu trên dashboard cho
    những key nó có, còn lại lấy giá trị dự phòng từ env trong app/core/config.py. Luôn
    cùng một dạng, nên chỗ gọi không cần rẽ nhánh."""
    row = await db.get_cleanup_settings()
    stored = row.get("settings") if isinstance(row.get("settings"), dict) else {}
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
        posts = await _run(f"SELECT count(*) AS n FROM posts WHERE {_ELIGIBLE}", [grace])
        comments = await _run(
            f"SELECT count(*) AS n FROM comments WHERE post_id IN (SELECT id FROM posts WHERE {_ELIGIBLE})", [grace]
        )
        result = {"dry_run": True, "grace_hours": grace_value, "posts": posts[0]["n"], "comments": comments[0]["n"]}
        logger.info("irrelevant_purge_dry_run", **result)
        return result

    ids = [
        row["id"]
        for row in await _run(
            f"SELECT id FROM posts WHERE {_ELIGIBLE} ORDER BY id LIMIT ?", [grace, BATCH_SIZE * MAX_BATCHES]
        )
    ]
    totals: dict[str, Any] = {"posts": 0, "comments": 0, "snapshots": 0, "batches": 0}
    for start in range(0, len(ids), BATCH_SIZE):
        id_list = _id_list(ids[start : start + BATCH_SIZE])
        totals["comments"] += len(await _run(f"DELETE FROM comments WHERE post_id IN ({id_list}) RETURNING id", []))
        totals["snapshots"] += len(
            await _run(f"DELETE FROM post_engagement_snapshots WHERE post_id IN ({id_list}) RETURNING id", [])
        )
        totals["posts"] += len(await _run(f"DELETE FROM posts WHERE id IN ({id_list}) RETURNING id", []))
        totals["batches"] += 1

    if totals["posts"]:
        # Tổng số trên trang Overview lấy từ các bảng tổng hợp stats_*_daily, vốn chỉ tăng khi
        # insert - không có bước này thì chúng vẫn tính cả mọi bài/comment đã bị xoá. Các dòng
        # đã bị xoá rồi, nên dựng lại thất bại chỉ được báo chứ không làm hỏng lượt dọn.
        try:
            await rebuild_from_source()
            totals["stats_rebuilt"] = True
        except Exception as exc:
            totals["stats_rebuilt"] = False
            logger.error("irrelevant_purge_stats_rebuild_failed", error=str(exc))

    backlog = await _run(f"SELECT count(*) AS n FROM posts WHERE {_ELIGIBLE}", [grace])
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
            except Exception as history_exc:
                # Không được che mất lỗi gốc.
                logger.error("irrelevant_purge_history_write_failed", error=str(history_exc))
        raise
    finally:
        if await redis.get(_LOCK_KEY) == token:
            await redis.delete(_LOCK_KEY)
