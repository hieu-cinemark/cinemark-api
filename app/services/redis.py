"""Single shared Redis client for this service - same instance spider-hub's
own RedisCache connects to (see social_crawler/services/redis.py there),
read-only from this side. Module-level singleton, same pattern as
app/services/kafka.py's `_producer`, so every caller (currently just
platform_token.py, but any future one too) reuses one connection pool
instead of each opening its own."""

from __future__ import annotations

from redis.asyncio import Redis

from app.core.config import settings

REDIS_KEY_PREFIX = "social_crawler:"

_client: Redis | None = None


def get_redis_client() -> Redis:
    global _client
    if _client is None:
        _client = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=True,
        )
    return _client
