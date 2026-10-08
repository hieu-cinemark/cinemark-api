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

Từ 2026-10-08 lượt quét gắn nhãn cả NỘI DUNG BÀI liên quan tới phim (classify_pending_posts) - trên Threads cảm
nhận nằm ở chính bài nhiều hơn ở comment, nên bỏ qua bài là bỏ mất phần lớn tiếng nói khán giả ở đó.
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from app.ai.tasks.sentiment import BATCH_SIZE, CommentLabel, classify_comments
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.repositories.d1.comments import ensure_comment_insight_columns
from app.repositories.d1.posts import RELEVANT_POST_SQL, ensure_post_voice_columns
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
_FILM_FIELDS = ("title", "director", "cast", "distributor", "released_at", "description")
_LOCK_TTL_SECONDS = 600


async def _save(labels: dict[str, CommentLabel], *, insights_only: bool = False, table: str = "comments") -> int:
    """Một lệnh UPDATE cho cả lô bằng CASE id WHEN ... (tối đa BATCH_SIZE dòng x 3 cột x 2 tham số + id -
    thấp xa so với giới hạn 100 tham số mỗi câu lệnh của D1). insights_only: chỉ ghi khía cạnh/giai đoạn cho
    comment đã có sentiment (giữ nguyên nhãn cũ để tỉ lệ cảm xúc không đổi vì một lượt bổ sung)."""
    if not labels:
        return 0
    values = {
        "aspects": {cid: json.dumps(label["aspects"]) for cid, label in labels.items()},
        "audience_stage": {cid: label["stage"] for cid, label in labels.items()},
    }
    if not insights_only:
        values["sentiment"] = {cid: label["sentiment"] for cid, label in labels.items()}
    sets: list[str] = []
    params: list[str] = []
    for column, by_id in values.items():
        sets.append(f"{column} = CASE id {' '.join('WHEN ? THEN ?' for _ in by_id)} END")
        for cid, value in by_id.items():
            params.extend([cid, value])
    sets.append("insights_classified_at = CURRENT_TIMESTAMP")
    if not insights_only:
        sets.append("sentiment_classified_at = CURRENT_TIMESTAMP")
    ids = list(labels)
    guard = "insights_classified_at IS NULL" if insights_only else "sentiment IS NULL"
    result = await d1_query(
        f"UPDATE {table} SET {', '.join(sets)} WHERE id IN ({','.join('?' * len(ids))}) AND {guard}",
        [*params, *ids],
    )
    if result is None:
        logger.warning("comment_sentiment_save_failed", count=len(ids), insights_only=insights_only)
        return 0
    return len(ids)


async def classify_pending(
    *,
    limit: int,
    since: datetime | None = None,
    before: datetime | None = None,
    platform: str | None = None,
    exclude: set[str] | frozenset[str] = frozenset(),
    dry_run: bool = False,
    insights_only: bool = False,
    movie_id: str | None = None,
) -> dict[str, Any]:
    """Phân loại tối đa `limit` comment có sentiment NULL, mới nhất trước, từng lô Kira một,
    ghi mỗi lô ngay khi đã gắn nhãn. Trả về {"selected", "classified", "failed",
    "failed_ids", <label>: count...} - có failed_ids để chỗ gọi giới hạn số lần thử lại
    của từng comment.

    insights_only=True: thay vào đó lấy comment ĐÃ có sentiment nhưng chưa có khía cạnh/giai đoạn
    (insights_classified_at NULL - mọi comment trước 2026-10-07) và chỉ ghi khía cạnh/giai đoạn - xem
    scripts/backfill_comment_insights.py. movie_id giới hạn vào comment dưới bài của một phim."""
    await ensure_comment_insight_columns()
    pending = "c.sentiment IS NOT NULL AND c.insights_classified_at IS NULL" if insights_only else "c.sentiment IS NULL"
    # Kèm thông tin phim của bài cha để AI nhận ra diễn viên/nhân vật và biết phim đã ra rạp chưa.
    sql = (
        "SELECT c.id, c.message, m.title, m.director, m.`cast` AS `cast`, m.distributor, m.released_at, m.description "
        "FROM comments c LEFT JOIN posts p ON p.id = c.post_id LEFT JOIN movies m ON m.id = p.movie_id "
        f"WHERE {pending} AND length(trim(c.message)) >= {MIN_CONTENT_LENGTH}"
    )
    params: list[str | int] = []
    if movie_id:
        sql += " AND c.post_id IN (SELECT id FROM posts WHERE movie_id = ?)"
        params.append(movie_id)
    if since is not None:
        sql += " AND c.scraped_at >= ?"
        params.append(since.astimezone(UTC).isoformat())  # scraped_at là ISO-8601 UTC
    if before is not None:
        sql += " AND c.scraped_at < ?"
        params.append(before.astimezone(UTC).isoformat())
    if platform:
        sql += " AND c.platform = ?"
        params.append(platform)
    sql += " ORDER BY c.scraped_at DESC LIMIT ?"
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
        films = [{k: row.get(k) for k in _FILM_FIELDS} if row.get("title") else None for row in batch]
        labels = await classify_comments([row["message"] for row in batch], films)
        done = {row["id"]: label for row, label in zip(batch, labels, strict=True) if label is not None}
        failed_ids.extend(row["id"] for row, label in zip(batch, labels, strict=True) if label is None)
        stats.update(label["sentiment"] for label in done.values())
        stats["with_aspects"] += sum(1 for label in done.values() if label["aspects"])
        stats["classified"] += len(done) if dry_run else await _save(done, insights_only=insights_only)
    stats["failed"] = len(failed_ids)
    return {**stats, "failed_ids": failed_ids}


# Nội dung bài thường dài (caption + hashtag) - cắt bớt trước khi gửi Kira; phần đầu là phần cảm nhận.
POST_TEXT_LIMIT = 1200
SWEEP_MAX_POSTS = 100


async def classify_pending_posts(
    *,
    limit: int,
    since: datetime | None = None,
    exclude: set[str] | frozenset[str] = frozenset(),
    dry_run: bool = False,
    movie_id: str | None = None,
) -> dict[str, Any]:
    """Như classify_pending nhưng cho NỘI DUNG BÀI liên quan tới phim (RELEVANT_POST_SQL) chưa có nhãn - cùng bộ
    phân loại, cùng nhãn (sentiment/khía cạnh/giai đoạn), ghi vào các cột tương ứng của bảng posts."""
    await ensure_post_voice_columns()
    sql = (
        f"SELECT p.id, substr(p.content, 1, {POST_TEXT_LIMIT}) AS message, m.title, m.director, m.`cast` AS `cast`, "
        "m.distributor, m.released_at, m.description "
        "FROM posts p LEFT JOIN movies m ON m.id = p.movie_id "
        f"WHERE p.sentiment IS NULL AND {RELEVANT_POST_SQL} AND length(trim(p.content)) >= {MIN_CONTENT_LENGTH}"
    )
    params: list[str | int] = []
    if movie_id:
        sql += " AND p.movie_id = ?"
        params.append(movie_id)
    if since is not None:
        sql += " AND p.scraped_at >= ?"
        params.append(since.astimezone(UTC).isoformat())
    sql += " ORDER BY p.scraped_at DESC LIMIT ?"
    params.append(limit + len(exclude))
    rows = await d1_query(sql, params)
    if rows is None:
        raise RuntimeError("post_sentiment_select_failed")
    rows = [row for row in rows if row["id"] not in exclude][:limit]
    stats: Counter[str] = Counter(selected=len(rows), classified=0)
    failed_ids: list[str] = []
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        films = [{k: row.get(k) for k in _FILM_FIELDS} if row.get("title") else None for row in batch]
        labels = await classify_comments([row["message"] for row in batch], films)
        done = {row["id"]: label for row, label in zip(batch, labels, strict=True) if label is not None}
        failed_ids.extend(row["id"] for row, label in zip(batch, labels, strict=True) if label is None)
        stats.update(label["sentiment"] for label in done.values())
        stats["classified"] += len(done) if dry_run else await _save(done, table="posts")
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
    post_attempts: Counter[str] = Counter()
    while True:
        full_batch = False
        try:
            if await _acquire_lock():
                try:
                    exclude = {cid for cid, n in attempts.items() if n >= MAX_ATTEMPTS}
                    result = await classify_pending(
                        limit=SWEEP_MAX_ROWS, since=datetime.now(tz=UTC) - SWEEP_WINDOW, exclude=exclude
                    )
                    # Bài chỉ chạy khi comment không còn tồn (comment mới vẫn ưu tiên có nhãn trong một hai phút).
                    if result["selected"] < SWEEP_MAX_ROWS:
                        post_result = await classify_pending_posts(
                            limit=SWEEP_MAX_POSTS,
                            since=datetime.now(tz=UTC) - SWEEP_WINDOW,
                            exclude={pid for pid, n in post_attempts.items() if n >= MAX_ATTEMPTS},
                        )
                        post_failed = post_result.pop("failed_ids")
                        if post_result["classified"]:
                            post_attempts.update(post_failed)
                        if len(post_attempts) > 10_000:
                            post_attempts.clear()
                        if post_result["selected"]:
                            logger.info("post_sentiment_sweep_finished", **post_result)
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
