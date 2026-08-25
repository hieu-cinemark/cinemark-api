from __future__ import annotations

from redis.asyncio import Redis

from app.core.config import settings

_REDIS_PREFIX = "social_crawler:"
_ACTIVE_ACCOUNT_KEY = "facebook:active_account"
_DEFAULT_ACCOUNT_KEY = "default"
_CACHE_KEY_TMPL = "facebook:session_cache:{account}"

_client: Redis | None = None


def _get_client() -> Redis:
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


async def get_facebook_token_status() -> tuple[str | None, int | None]:
    client = _get_client()
    account = await client.get(_REDIS_PREFIX + _ACTIVE_ACCOUNT_KEY)
    account = account.strip('"') if account else _DEFAULT_ACCOUNT_KEY

    cache_key = _REDIS_PREFIX + _CACHE_KEY_TMPL.format(account=account.strip().lower())
    ttl = await client.ttl(cache_key)
    if ttl is None or ttl < 0:
        return account, None
    return account, ttl
