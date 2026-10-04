from __future__ import annotations

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client

_DEFAULT_ACCOUNT_KEY = "default"


def account_key(account: dict) -> str:
    """Key session Redis mà spider-hub thực sự dùng để lưu/tra tài khoản này - ưu tiên
    email, chữ thường. Phải khớp chính xác với account_key() trong auth/accounts.py của
    facebook/threads (`user.strip().lower()`), nếu không một chỗ gọi dùng key khác hoa
    thường/khác dạng ở đây (ví dụ platform_accounts.account_id thô) sẽ âm thầm trượt
    session của spider-hub và không làm gì thay vì tác động lên đúng tài khoản. Dùng
    chung cho app/api/routes/token_refresh.py và route nurture-accounts của settings.py
    để hai bên không bao giờ lệch nhau nữa."""
    return (account.get("email") or account.get("account_id") or "").strip().lower()


# TikTok không có TTL session_cache GraphQL. Một giá trị synthetic dương làm
# TokenStatus.valid=True cho dashboard sau khi refresh danh tính/cookie; giao diện ẩn
# đồng hồ đếm ngược với tiktok.
_TIKTOK_VALID_TTL_SECONDS = 7 * 24 * 3600


async def _tiktok_session_status() -> tuple[str | None, int | None]:
    """Danh tính TikTok dùng được = dòng đang bật, không bị checkpoint, cookie có sessionid.

    Do bootstrap auth tiktok / import cookie của spider-hub ghi (update_tiktok_identity +
    reactivate_account) — không có key Redis tiktok:session_cache như Facebook/Threads.
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
    """Nền tảng hiện có session thu thập dùng được hay không.

    Facebook/Threads: TTL session_cache trong Redis của spider-hub (graphql_client).
    TikTok: dòng platform_accounts đang bật có cookie sessionid còn sống.
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
