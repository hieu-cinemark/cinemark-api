"""Phát lại message trong topic ingest_dead_letters về topic gốc (raw_posts / raw_comments), sau khi đã xử lý sự cố khiến chúng
vào đó (D1 sập, bài cha thiếu...). Ingest consumer xử lý lại như message mới; upsert nên chạy lại vô hại. Message vẫn không
xử lý được thì lại rơi vào dead letter.

Offset chỉ commit sau khi broker đã nhận bản phát lại, nên dừng giữa chừng không mất gì.

    python -m scripts.tools.replay_ingest_dead_letters --dry-run
    python -m scripts.tools.replay_ingest_dead_letters --reason comment_parent_missing --max 500
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.clients.kafka import INGEST_DEAD_LETTER_TOPIC
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

CONSUMER_GROUP = "cinemark-api.ingest.dead-letter-replay"
ALLOWED_TOPICS = {"raw_posts", "raw_comments"}
_IDLE_TIMEOUT_MS = 5000


def _replayable(record: dict) -> bool:
    return record.get("topic") in ALLOWED_TOPICS and isinstance(record.get("payload"), dict)


async def replay(*, dry_run: bool, reason: str | None, max_messages: int | None) -> Counter:
    consumer = AIOKafkaConsumer(
        INGEST_DEAD_LETTER_TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=orjson.loads,
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, value_serializer=orjson.dumps)
    stats: Counter = Counter()
    await consumer.start()
    await producer.start()
    try:
        while max_messages is None or stats["replayed"] < max_messages:
            # Không lấy quá phần còn lại của --max: cả lô được commit cùng lúc bên dưới.
            room = 100 if max_messages is None else min(100, max_messages - stats["replayed"])
            batch = await consumer.getmany(timeout_ms=_IDLE_TIMEOUT_MS, max_records=room)
            if not any(batch.values()):
                break
            for messages in batch.values():
                for message in messages:
                    record = message.value
                    stats[f"seen.{record.get('reason')}"] += 1
                    # Bỏ qua (theo --reason) nghĩa là không phát lại trong lượt này nhưng offset vẫn đi qua; chạy lại từ đầu
                    # bằng cách đặt lại offset của group nếu cần.
                    if reason and record.get("reason") != reason:
                        stats["skipped_reason"] += 1
                    elif not _replayable(record):
                        logger.warning("dead_letter_not_replayable", offset=message.offset, topic=record.get("topic"))
                        stats["not_replayable"] += 1
                    elif not dry_run:
                        key = message.key
                        await producer.send_and_wait(record["topic"], key=key, value=record["payload"])
                        stats["replayed"] += 1
                    else:
                        stats["would_replay"] += 1
            if not dry_run:
                await consumer.commit()
    finally:
        await producer.stop()
        await consumer.stop()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="chỉ đếm, không phát lại, không commit offset")
    parser.add_argument(
        "--reason", help="chỉ phát lại message có lý do này (d1_write_failed, comment_parent_missing...)"
    )
    parser.add_argument("--max", type=int, default=None, dest="max_messages")
    args = parser.parse_args()
    stats = asyncio.run(replay(dry_run=args.dry_run, reason=args.reason, max_messages=args.max_messages))
    logger.info("dead_letter_replay_finished", dry_run=args.dry_run, **dict(stats))


if __name__ == "__main__":
    main()
