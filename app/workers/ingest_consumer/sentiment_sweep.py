"""Lượt quét cảm xúc comment chạy nền, bên trong ingest consumer.

handle_comment lưu comment với sentiment NULL; vòng lặp này cứ mỗi
SWEEP_INTERVAL_SECONDS lại lấy các comment gần đây chưa phân loại và phân loại theo
lô qua Kira (app/ai/tasks/sentiment.py), để một comment có nhãn trong vòng một hai
phút thay vì làm nghẽn việc ingest Kafka bằng mỗi comment một lời gọi LLM.

Lời gọi Kira chạy từng lô một, nên lượt quét không bao giờ chiếm quá một chỗ trong
giới hạn song song của Kira (app/ai/client.py) so với phân loại độ liên quan của bài
lúc ingest. Một khoá Redis ngăn hai consumer đang chạy cùng phân loại một dòng.

scripts/backfill_comment_sentiment.py dùng lại classify_pending() cho hàng tồn cũ
hơn, nằm ngoài cửa sổ thời gian gần đây của lượt quét.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from app.ai.tasks.sentiment import BATCH_SIZE, classify_sentiments
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.services.d1 import MIN_CONTENT_LENGTH, d1_query

logger = get_logger(__name__)

SWEEP_INTERVAL_SECONDS = 60
SWEEP_MAX_ROWS = 200
# Chỉ comment gần đây: các dòng NULL cũ hơn là việc của script backfill, và cửa sổ này
# giới hạn thời gian một comment mà Kira cứ lỗi mãi còn được thử lại.
SWEEP_WINDOW = timedelta(hours=48)
# Comment mà Kira không trả về nhãn hợp lệ quá chừng này lần thì để lại cho script
# backfill thay vì chiếm chỗ ở mọi lượt quét.
MAX_ATTEMPTS = 3
_LOCK_KEY = f"{REDIS_KEY_PREFIX}comment_sentiment_sweep_lock"
_LOCK_TTL_SECONDS = 600


async def _save(labels: dict[str, str]) -> int:
    """Mỗi giá trị nhãn một lệnh UPDATE (id trong IN (...)) - tối đa BATCH_SIZE+1 tham số
    bind, thấp xa so với giới hạn 100 mỗi câu lệnh của D1."""
    by_label: dict[str, list[str]] = {}
    for comment_id, label in labels.items():
        by_label.setdefault(label, []).append(comment_id)
    saved = 0
    for label, ids in by_label.items():
        placeholders = ",".join("?" * len(ids))
        result = await d1_query(
            f"UPDATE comments SET sentiment = ?, sentiment_classified_at = CURRENT_TIMESTAMP "
            f"WHERE id IN ({placeholders}) AND sentiment IS NULL",
            [label, *ids],
        )
        if result is None:
            logger.warning("comment_sentiment_save_failed", sentiment=label, count=len(ids))
        else:
            saved += len(ids)
    return saved


async def classify_pending(
    *,
    limit: int,
    since: datetime | None = None,
    before: datetime | None = None,
    platform: str | None = None,
    exclude: set[str] | frozenset[str] = frozenset(),
    dry_run: bool = False,
) -> dict[str, Any]:
    """Phân loại tối đa `limit` comment có sentiment NULL, mới nhất trước, từng lô Kira một,
    ghi mỗi lô ngay khi đã gắn nhãn. Trả về {"selected", "classified", "failed",
    "failed_ids", <label>: count...} - có failed_ids để chỗ gọi giới hạn số lần thử lại
    của từng comment."""
    sql = f"SELECT id, message FROM comments WHERE sentiment IS NULL AND length(trim(message)) >= {MIN_CONTENT_LENGTH}"
    params: list[str | int] = []
    if since is not None:
        sql += " AND scraped_at >= ?"
        params.append(since.astimezone(UTC).isoformat())  # scraped_at là ISO-8601 UTC
    if before is not None:
        sql += " AND scraped_at < ?"
        params.append(before.astimezone(UTC).isoformat())
    if platform:
        sql += " AND platform = ?"
        params.append(platform)
    sql += " ORDER BY scraped_at DESC LIMIT ?"
    params.append(limit + len(exclude))
    rows = await d1_query(sql, params)
    if rows is None:
        raise RuntimeError("comment_sentiment_select_failed")
    rows = [row for row in rows if row["id"] not in exclude][:limit]

    # Đặt sẵn classified từ đầu: khi không có dòng nào thì vòng lặp không chạm tới nó, và
    # dict thường được trả về (khác Counter) sẽ lỗi khi thiếu key.
    stats: Counter[str] = Counter(selected=len(rows), classified=0)
    failed_ids: list[str] = []
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        labels = await classify_sentiments([row["message"] for row in batch])
        done = {row["id"]: label for row, label in zip(batch, labels, strict=True) if label is not None}
        failed_ids.extend(row["id"] for row, label in zip(batch, labels, strict=True) if label is None)
        stats.update(done.values())
        stats["classified"] += len(done) if dry_run else await _save(done)
    stats["failed"] = len(failed_ids)
    return {**stats, "failed_ids": failed_ids}


async def _acquire_lock() -> bool:
    try:
        return bool(await get_redis_client().set(_LOCK_KEY, "1", nx=True, ex=_LOCK_TTL_SECONDS))
    except Exception as exc:  # noqa: BLE001 - không có Redis thì chạy không khoá (bình thường chỉ có một consumer)
        logger.warning("comment_sentiment_lock_unavailable", error=exc)
        return True


async def _release_lock() -> None:
    try:
        await get_redis_client().delete(_LOCK_KEY)
    except Exception as exc:  # noqa: BLE001 - đằng nào TTL cũng làm nó hết hạn
        logger.debug("comment_sentiment_lock_release_failed", error=exc)


async def sweep_forever() -> None:
    """Không bao giờ trả về; mọi lỗi đều được log và thử lại ở vòng sau."""
    attempts: Counter[str] = Counter()
    while True:
        full_batch = False
        try:
            if await _acquire_lock():
                try:
                    exclude = {cid for cid, n in attempts.items() if n >= MAX_ATTEMPTS}
                    result = await classify_pending(
                        limit=SWEEP_MAX_ROWS, since=datetime.now(tz=UTC) - SWEEP_WINDOW, exclude=exclude
                    )
                finally:
                    await _release_lock()
                failed_ids = result.pop("failed_ids")
                # Chỉ tính lỗi cho comment khi Kira đã gắn nhãn được các comment khác trong cùng vòng -
                # một vòng mà lô nào cũng lỗi nghĩa là Kira sập/bị giới hạn, và không được loại bỏ mọi
                # thứ.
                if result["classified"]:
                    attempts.update(failed_ids)
                if len(attempts) > 10_000:  # giới hạn bộ nhớ; tệ nhất là thử lại thêm vài lần
                    attempts.clear()
                if result["selected"]:
                    logger.info("comment_sentiment_sweep_finished", **result)
                full_batch = result["selected"] >= SWEEP_MAX_ROWS and result["classified"] > 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - tiếp tục quét; ingest không được chết vì cảm xúc
            logger.warning("comment_sentiment_sweep_failed", error=exc)
        # Đầy một trang nghĩa là còn hàng tồn - chạy tiếp ngay.
        await asyncio.sleep(1 if full_batch else SWEEP_INTERVAL_SECONDS)
