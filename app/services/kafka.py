"""Publishes crawl-trigger requests to Kafka - the producer side of
spider-hub's crawl_request_consumer.py, which listens for these and
launches the matching `scrapy crawl` subprocess. Both app/api/routes/scraper.py
(manual "run" button) and the daily scheduler script publish through the
same function, so a manual trigger and a scheduled one are indistinguishable
downstream - one code path, one contract.

Mirrors spider-hub's own social_crawler/services/kafka.py: fire-and-forget,
never blocks/fails the request over a Kafka outage - a crawl trigger that
can't be published just doesn't run, logged, not a 500."""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

CRAWL_REQUESTS_TOPIC = "crawl_requests"

_producer: AIOKafkaProducer | None = None


async def start_kafka_producer() -> None:
    global _producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    try:
        await producer.start()
    except KafkaError as exc:
        logger.warning("kafka_unavailable", error=str(exc))
        await producer.stop()
        return
    _producer = producer


async def stop_kafka_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def publish_crawl_request(
    *,
    platform: str,
    keyword: str,
    keyword_id: uuid.UUID,
    run_id: uuid.UUID | None = None,
    max_pages: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> bool:
    """Returns whether the request was actually published - callers decide
    what to tell the user if Kafka is down (e.g. still return 202 since the
    trigger endpoint's job is just to ask, not to guarantee delivery, or
    surface a warning - see app/api/routes/scraper.py)."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", keyword_id=str(keyword_id))
        return False
    value: dict[str, Any] = {"platform": platform, "keyword": keyword, "keyword_id": str(keyword_id)}
    if run_id is not None:
        value["run_id"] = str(run_id)
    if max_pages is not None:
        value["max_pages"] = max_pages
    if start_date is not None:
        value["start_date"] = start_date.isoformat()
    if end_date is not None:
        value["end_date"] = end_date.isoformat()
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{platform}:{keyword_id}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), keyword_id=str(keyword_id))
        return False
    return True


async def publish_token_refresh_request(*, run_id: uuid.UUID) -> bool:
    """Publishes to the same crawl_requests topic as publish_crawl_request,
    tagged type="refresh_token" so crawl_request_consumer.py runs the
    Facebook auth bootstrap script (see spider-hub's
    social_crawler/spiders/facebook/auth/bootstrap.py and
    scripts/refresh_token.sh, which already does this on a 4h cron) instead
    of a scrapy spider. Kept as its own function rather than overloading
    publish_crawl_request - the fields genuinely don't overlap (no
    platform/keyword/keyword_id here)."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", run_id=str(run_id))
        return False
    value: dict[str, Any] = {"type": "refresh_token", "run_id": str(run_id)}
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"refresh_token:{run_id}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), run_id=str(run_id))
        return False
    return True
