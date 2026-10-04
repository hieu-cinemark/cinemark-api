"""Redis client dùng chung duy nhất của service này - cùng instance mà RedisCache của
spider-hub kết nối tới (xem social_crawler/clients/redis.py bên đó), phía này chỉ
đọc. Singleton cấp module, cùng kiểu với `_producer` của app/clients/kafka.py, để
mọi chỗ gọi (hiện chỉ có platform_token.py, nhưng cả những chỗ sau này) dùng chung
một connection pool thay vì mỗi chỗ tự mở."""

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
