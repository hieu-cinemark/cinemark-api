"""Archives every raw Kafka message to the R2 data lake (bronze layer):

    bronze/entity={posts|comments|decisions}/platform=<p>/dt=<YYYY-MM-DD>/p<partition>-<first>-<last>.ndjson.gz

Own consumer group, so it reads the same topics as the ingest consumer
without affecting it. Offsets are committed only after every object of a
batch is written: a crash re-reads from the last commit, so bronze can hold
a few duplicate rows - silver dedups on (topic, partition, offset).

    python -m app.workers.lake_writer.main
"""

from __future__ import annotations

import asyncio
import gzip
import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord

from app.clients.lake import lake_configured, put_object
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

TOPICS = {"raw_posts": "posts", "raw_comments": "comments", "ingest_decisions": "decisions"}
CONSUMER_GROUP = "cinemark-api.lake"
BATCH_MAX_RECORDS = 5000
FLUSH_EVERY_SECONDS = 300
SCHEMA_VERSION = 1


def _envelope(record: ConsumerRecord) -> tuple[str, str, str]:
    """(platform, dt, ndjson line) for one Kafka record."""
    try:
        payload: Any = json.loads(record.value)
    except TypeError, ValueError:
        payload = {"_unparsed": record.value.decode("utf-8", "replace") if record.value else None}
    platform = (payload.get("platform") if isinstance(payload, dict) else None) or "unknown"
    dt = datetime.fromtimestamp(record.timestamp / 1000, UTC).strftime("%Y-%m-%d")
    line = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "ingested_at": datetime.now(UTC).isoformat(),
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "kafka_ts": record.timestamp,
            "key": record.key.decode("utf-8", "replace") if record.key else None,
            "payload": payload,
        },
        ensure_ascii=False,
    )
    return platform, dt, line


async def _flush(consumer: AIOKafkaConsumer, buffers: dict[TopicPartition, list[ConsumerRecord]]) -> int:
    commits: dict[TopicPartition, int] = {}
    written = 0
    for tp, records in buffers.items():
        if not records:
            continue
        groups: dict[tuple[str, str], list[str]] = defaultdict(list)
        for record in records:
            platform, dt, line = _envelope(record)
            groups[(platform, dt)].append(line)
        first, last = records[0].offset, records[-1].offset
        for (platform, dt), lines in groups.items():
            key = f"bronze/entity={TOPICS[tp.topic]}/platform={platform}/dt={dt}/p{tp.partition}-{first:012d}-{last:012d}.ndjson.gz"
            await put_object(key, gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
            written += len(lines)
        commits[tp] = last + 1
    await consumer.commit(commits)
    return written


async def run() -> None:
    if not lake_configured():
        raise SystemExit("R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY are not set")
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("lake_writer_started", topics=list(TOPICS), group=CONSUMER_GROUP)
    buffers: dict[TopicPartition, list[ConsumerRecord]] = defaultdict(list)
    last_flush = time.monotonic()
    try:
        while True:
            for tp, records in (await consumer.getmany(timeout_ms=1000, max_records=1000)).items():
                buffers[tp].extend(records)
            pending = sum(len(records) for records in buffers.values())
            if pending and (pending >= BATCH_MAX_RECORDS or time.monotonic() - last_flush >= FLUSH_EVERY_SECONDS):
                started = time.monotonic()
                written = await _flush(consumer, buffers)
                logger.info("lake_flush", records=written, latency_ms=round((time.monotonic() - started) * 1000))
                buffers.clear()
                last_flush = time.monotonic()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())
