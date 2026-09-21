from __future__ import annotations

from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

_DEFAULT_ACCOUNT_KEY = "default"


def account_key(account: dict) -> str:
    """The Redis session key spider-hub actually stores/looks this account
    up under - email-preferring, lowercased. Must match facebook/threads
    auth/accounts.py's own account_key() (`user.strip().lower()`) exactly,
    or a caller using a differently-cased/shaped key here (e.g. a raw
    platform_accounts.account_id) silently misses spider-hub's session and
    no-ops instead of acting on the intended account. Shared by
    app/api/routes/token_refresh.py and settings.py's nurture-accounts route
    so both sides can never drift apart again."""
    return (account.get("email") or account.get("account_id") or "").strip().lower()
# TikTok has no GraphQL session_cache TTL. A positive synthetic value makes
# TokenStatus.valid=True for the dashboard after identity/cookie refresh;
# the UI hides the countdown for tiktok.
_TIKTOK_VALID_TTL_SECONDS = 7 * 24 * 3600


async def _tiktok_session_status() -> tuple[str | None, int | None]:
    """Usable TikTok identity = enabled row, not checkpointed, cookie has sessionid.

    Written by spider-hub's tiktok auth bootstrap / cookie import
    (update_tiktok_identity + reactivate_account) — there is no
    tiktok:session_cache Redis key like Facebook/Threads.
    """
    from app.services.platform_config_db import list_accounts

    rows = await list_accounts("tiktok")
    usable = [
        row
        for row in rows
        if row.get("enabled")
        and (row.get("pool_status") or "active") != "checkpoint"
        and "sessionid=" in (row.get("cookie") or "").lower()
    ]
    if not usable:
        if not rows:
            return None, None
        latest = max(rows, key=lambda row: str(row.get("updated_at") or row.get("last_used_at") or ""))
        label = (latest.get("email") or latest.get("account_id") or "").strip() or None
        return label, None

    best = max(usable, key=lambda row: str(row.get("last_used_at") or row.get("updated_at") or ""))
    label = (best.get("email") or best.get("account_id") or "").strip() or None
    return label, _TIKTOK_VALID_TTL_SECONDS


async def get_token_status(platform: str) -> tuple[str | None, int | None]:
    """Whether the platform has a usable collection session right now.

    Facebook/Threads: spider-hub Redis session_cache TTL (graphql_client).
    TikTok: enabled platform_accounts row with a live sessionid cookie.
    """
    if platform == "tiktok":
        return await _tiktok_session_status()

    client = get_redis_client()
    account = await client.get(f"{REDIS_KEY_PREFIX}{platform}:active_account")
    account = account.strip('"') if account else _DEFAULT_ACCOUNT_KEY

    cache_key = f"{REDIS_KEY_PREFIX}{platform}:session_cache:{account.strip().lower()}"
    ttl = await client.ttl(cache_key)
    if ttl is None or ttl < 0:
        return account, None
    return account, ttl
