"""Bộ đếm theo giờ trong Redis: quyết định ingest (mới / cập nhật / bị loại) và số bài, comment mỗi giờ."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.services.stats.stats_tables import _as_int

logger = get_logger(__name__)

HOURLY_RETENTION_HOURS = 72

# Ngày đầu tiên mọi bài/comment mới đều có first_seen_at (cột thêm ngày 28/10/2026, deploy giữa ngày).
FIRST_SEEN_COMPLETE_SINCE = "2026-10-29"

_HOURLY_KEY_PREFIX = f"{REDIS_KEY_PREFIX}stats_hourly:"


def _hour_bucket(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H")


async def bump_hourly(platform: str, scraped_at: str, metric: str) -> None:
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
        logger.warning("stats_hourly_bump_failed", platform=platform, error=exc)


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
        logger.warning("stats_funnel_bump_failed", platform=platform, error=exc)


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
