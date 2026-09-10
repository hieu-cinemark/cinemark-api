"""Computes a platform_accounts row's health from signals that already
exist - no new request is ever sent to Facebook/Threads/TikTok here. This
was a deliberate choice over an "active" probe (actually hitting the
platform right now): an active check needs its own account-scoped request
path (today's TikTokClient/bootstrap.py both always rotate/pick an account
themselves, never operate on one specific row), and would burn a real
request against the very platform this whole project is trying not to get
blocked by. Passive is instant, safe to run as often as someone clicks
"Check", and reuses exactly the same signals spider-hub's own auto-disable
logic already trusts - see:

  - TikTok: client.py's tiktok_block_streak:<device_id> Redis counter,
    incremented on every empty (likely-blocked) response, account disabled
    at streak >= 3 (see disable_account there).
  - Facebook/Threads: platform_token.get_token_status(), which mirrors
    spider-hub's own <platform>:active_account / session_cache:<account>
    Redis keys (see that module's docstring) - the same signal
    TokenStatusBadge shows on the dashboard already.

The trade-off: this can only ever be as fresh as the last real crawl/login
activity touched these signals. A row that hasn't been used in days reports
whatever it last did, not "right now"."""

from __future__ import annotations

from typing import Any

from app.services.platform_token import get_token_status
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client


async def _check_tiktok(account_id: str) -> str:
    # Mirrors the key client.py writes in spider-hub
    # (tiktok_block_streak:<device_id>, where device_id is stored in
    # platform_accounts.account_id for platform='tiktok' - see
    # social_crawler/services/db.py's docstring for that column reuse). The
    # key only exists once a block has actually happened (client.py's first
    # INCR creates it), so its mere presence is enough - no need to read the
    # count itself.
    client = get_redis_client()
    key = f"{REDIS_KEY_PREFIX}tiktok_block_streak:{account_id}"
    return "warning" if await client.get(key) else "ok"


async def _check_active_session(platform: str, account_id: str) -> str:
    active_account, ttl_seconds = await get_token_status(platform)
    if account_id.strip().lower() != (active_account or "").strip().lower():
        return "unknown"
    return "ok" if ttl_seconds is not None else "warning"


async def evaluate_account_health(account: dict[str, Any]) -> str:
    """Returns a status for one platform_accounts row: "disabled" | "ok" |
    "warning" | "unknown" - a plain string column, not an enum, so a future
    signal can introduce a new status value without a migration."""
    if not account["enabled"]:
        return "disabled"

    platform = account["platform"]
    account_id = account["account_id"]
    if platform == "tiktok":
        return await _check_tiktok(account_id)
    if platform in ("facebook", "threads"):
        return await _check_active_session(platform, account_id)
    return "unknown"
