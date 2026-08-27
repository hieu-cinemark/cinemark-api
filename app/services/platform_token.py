from __future__ import annotations

from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

_DEFAULT_ACCOUNT_KEY = "default"


async def get_token_status(platform: str) -> tuple[str | None, int | None]:
    """Whether spider-hub's cached session for this platform (see its
    graphql_client.py) is still usable. Same Redis key layout for every
    platform with a browser-bootstrap token cache - see spider-hub's
    constants/facebook.py + constants/threads.py: the active account lives
    under "<platform>:active_account", and that account's captured token
    cache under "<platform>:session_cache:{account}", both namespaced under
    RedisCache's "social_crawler:" prefix. The cache key's own Redis TTL
    already encodes validity (bootstrap.py sets it equal to
    CACHE_MAX_AGE_SECONDS), so "does the key still exist" *is* "is it
    valid", no separate age calculation needed."""
    client = get_redis_client()
    account = await client.get(f"{REDIS_KEY_PREFIX}{platform}:active_account")
    account = account.strip('"') if account else _DEFAULT_ACCOUNT_KEY

    cache_key = f"{REDIS_KEY_PREFIX}{platform}:session_cache:{account.strip().lower()}"
    ttl = await client.ttl(cache_key)
    if ttl is None or ttl < 0:
        return account, None
    return account, ttl
