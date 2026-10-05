"""Nạp lại một lần bộ đếm theo giờ trong Redis (stats_summary.get_hourly_counts) từ
posts + comments trên D1 trong HOURLY_RETENTION_HOURS giờ gần nhất, và phễu ingest
(stats_summary.get_ingest_funnel) từ topic Kafka ingest_decisions trong cùng cửa sổ. Bộ đếm chỉ bắt đầu
tăng từ lúc deploy code mới, nên chạy script này một lần để biểu đồ "24 giờ gần nhất"
của dashboard không trống ngay sau deploy. Ghi đè (HSET), chạy lại lúc nào cũng an toàn:

    cd cinemark-api && source .venv/bin/activate
    python -m scripts.backfill_hourly_stats
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

from aiokafka import AIOKafkaConsumer, TopicPartition

from app.clients.kafka import INGEST_DECISIONS_TOPIC
from app.clients.redis import get_redis_client
from app.core.config import settings
from app.services.d1 import d1_query
from app.services.stats_summary import _HOURLY_KEY_PREFIX, HOURLY_RETENTION_HOURS, _hour_bucket


async def _counts(table: str, since: str) -> list[dict]:
    rows = await d1_query(
        f"""
        SELECT substr(scraped_at, 1, 13) AS hour, platform, COUNT(*) AS n
        FROM {table}
        WHERE scraped_at >= ?
        GROUP BY substr(scraped_at, 1, 13), platform
        """,
        [since],
    )
    return rows or []


async def _decision_counts(since: datetime) -> Counter[tuple[str, str]]:
    """(bucket giờ, trường hash) -> số lượng, từ mọi quyết định ingest kể từ `since`."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers, enable_auto_commit=False, group_id=None
    )
    await consumer.start()
    counts: Counter[tuple[str, str]] = Counter()
    try:
        await consumer._client.force_metadata_update()
        parts = [
            TopicPartition(INGEST_DECISIONS_TOPIC, p)
            for p in (consumer._client.cluster.partitions_for_topic(INGEST_DECISIONS_TOPIC) or [])
        ]
        consumer.assign(parts)
        starts = await consumer.offsets_for_times({tp: int(since.timestamp() * 1000) for tp in parts})
        ends = await consumer.end_offsets(parts)
        remaining = {}
        for tp in parts:
            start = starts.get(tp)
            remaining[tp] = ends[tp] - start.offset if start else 0
            if start:
                consumer.seek(tp, start.offset)
        while sum(remaining.values()) > 0:
            batch = await consumer.getmany(timeout_ms=5000, max_records=5000)
            if not batch:
                break
            for tp, messages in batch.items():
                for message in messages:
                    remaining[tp] -= 1
                    try:
                        event = json.loads(message.value)
                    except TypeError, ValueError:
                        continue
                    platform = event.get("platform")
                    if not platform:
                        continue
                    bucket = _hour_bucket(datetime.fromtimestamp(message.timestamp / 1000, tz=UTC))
                    counts[(bucket, f"{platform}:received")] += 1
                    if event.get("decision") == "dropped":
                        counts[(bucket, f"{platform}:dropped.{event.get('reason')}")] += 1
                    else:
                        counts[(bucket, f"{platform}:kept")] += 1
    finally:
        await consumer.stop()
    return counts


async def main() -> None:
    print(f"db_mode={settings.db_mode}")
    since = (datetime.now(UTC) - timedelta(hours=HOURLY_RETENTION_HOURS)).strftime("%Y-%m-%dT%H")
    client = get_redis_client()
    written = 0
    for table, metric in (("posts", "posts"), ("comments", "comments")):
        for row in await _counts(table, since):
            key = f"{_HOURLY_KEY_PREFIX}{row['hour']}"
            await client.hset(key, f"{row['platform']}:{metric}", int(row["n"] or 0))
            await client.expire(key, HOURLY_RETENTION_HOURS * 3600)
            written += 1
    print(f"wrote {written} hour/platform counters since {since}")

    # Phễu: received/dropped.* lấy thẳng từ quyết định; "updated" = giữ lại - mới (posts).
    decisions = await _decision_counts(datetime.now(UTC) - timedelta(hours=HOURLY_RETENTION_HOURS))
    funnel_written = 0
    for (bucket, field), n in decisions.items():
        key = f"{_HOURLY_KEY_PREFIX}{bucket}"
        if field.endswith(":kept"):
            platform = field.removesuffix(":kept")
            new = int(await client.hget(key, f"{platform}:posts") or 0)
            await client.hset(key, f"{platform}:updated", max(0, n - new))
        else:
            await client.hset(key, field, n)
        await client.expire(key, HOURLY_RETENTION_HOURS * 3600)
        funnel_written += 1
    print(f"wrote {funnel_written} ingest-funnel counters from {INGEST_DECISIONS_TOPIC}")


if __name__ == "__main__":
    asyncio.run(main())
