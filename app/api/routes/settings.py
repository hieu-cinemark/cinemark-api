"""Trang Settings của dashboard: CRUD trên các bảng platform_accounts /
platform_proxies ở D1 (xem app/services/accounts/config_db/) - chính là các
bảng mà spider-hub đọc để lấy thông tin đăng nhập và cấu hình proxy. Chỗ khác chỉ
đọc; đây là nơi duy nhất ghi vào các bảng đó."""

from __future__ import annotations

import binascii
import re
import time
from typing import Any

from fastapi import APIRouter, Query
from pyotp import TOTP

from app.api.routes.settings_cookies import _record_cookie_saved, _refuse_bad_cookie, _with_user_agent
from app.clients.kafka import publish_nurture_request, publish_tiktok_identity_reset
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.core.logging import get_logger
from app.schemas.settings import (
    ACCOUNT_SECRET_FIELDS,
    AccountCreate,
    AccountCredentialsOut,
    AccountOut,
    AccountSetEnabled,
    AccountUpdate,
    CommentScheduleOut,
    CommentScheduleUpdate,
    CrawlScheduleOut,
    CrawlScheduleUpdate,
    FilterKeywordCreate,
    FilterKeywordOut,
    FilterKeywordUpdate,
    NurtureRequest,
    NurtureResponse,
    ProxyCreate,
    ProxyOut,
    ProxyProviderOut,
    ProxyProviderUpdate,
    ProxySettings,
    ProxySettingsOut,
    ProxyUpdate,
    TotpCodeResponse,
    parse_run_times,
)
from app.services.accounts.account_health import evaluate_account_health
from app.services.accounts.config_db import accounts as accounts_db
from app.services.accounts.config_db import proxy_settings as proxy_settings_db
from app.services.accounts.config_db import schedules as schedules_db
from app.services.accounts.platform_token import account_key as _account_key
from app.services.accounts.user_agent import merge_user_agent, split_user_agent

logger = get_logger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/accounts", response_model=list[AccountOut])
async def list_accounts(platform: str | None = Query(default=None)) -> list[AccountOut]:
    rows = await accounts_db.list_accounts(platform, with_secrets=False)
    return [AccountOut.masked(row) for row in rows]


@router.post("/accounts", response_model=AccountOut)
async def create_account(payload: AccountCreate) -> AccountOut:
    fields = _with_user_agent(payload.model_dump())
    _refuse_bad_cookie(fields["platform"], fields["account_id"], fields.get("cookie") or "")
    row = await accounts_db.create_account(fields)
    await _record_cookie_saved(row)
    return AccountOut.masked(row)


@router.patch("/accounts/{account_id}", response_model=AccountOut)
async def update_account(account_id: int, payload: AccountUpdate) -> AccountOut:
    changes = payload.model_dump(exclude_unset=True)
    # Danh sách tài khoản không còn trả giá trị bí mật (AccountOut.masked), nên form sửa
    # trên dashboard nhận về chuỗi rỗng - chuỗi rỗng ở đây nghĩa là "giữ nguyên", không
    # phải "xoá", để một lần lưu không vô tình xoá mật khẩu/cookie đang có.
    for name in ACCOUNT_SECRET_FIELDS:
        if changes.get(name) == "":
            changes.pop(name)
    # Ô User-Agent: trống = giữ nguyên (như các ô bí mật); có giá trị thì ghép vào cookie mới gửi lên, hoặc cookie
    # đang lưu. Form sửa nhận cookie đã tách UA (get_account_credentials), nên cookie mới gửi lên mà không kèm UA thì
    # giữ UA đang lưu.
    user_agent = (changes.pop("user_agent", None) or "").strip()
    if user_agent or "cookie" in changes:
        current = await accounts_db.get_account(account_id)
        if current is None:
            raise NotFoundError(f"Account {account_id} not found.")
        stored_cookie = current.get("cookie") or ""
        cookie = changes.get("cookie", stored_cookie)
        user_agent = user_agent or split_user_agent(cookie)[1] or split_user_agent(stored_cookie)[1]
        if user_agent:
            changes["cookie"] = merge_user_agent(cookie, user_agent)
        # Lưu lại đúng cookie cũ không phải cookie mới: không được làm spider-hub tưởng phiên đã được cứu.
        if changes.get("cookie") == stored_cookie:
            changes.pop("cookie")
        elif "cookie" in changes:
            _refuse_bad_cookie(current["platform"], current["account_id"], changes["cookie"])
    if not changes:
        row = await accounts_db.get_account(account_id)
        if row is None:
            raise NotFoundError(f"Account {account_id} not found.")
        return AccountOut.masked(row)
    row = await accounts_db.update_account(account_id, changes)
    if "cookie" in changes:
        await _record_cookie_saved(row)
    return AccountOut.masked(row)


@router.get("/accounts/{account_id}/credentials", response_model=AccountCredentialsOut)
async def get_account_credentials(account_id: int) -> AccountCredentialsOut:
    row = await accounts_db.get_account(account_id)
    if row is None:
        raise NotFoundError(f"Account {account_id} not found.")
    logger.info("account_credentials_viewed", account_id=account_id)
    secrets = {name: row.get(name) or "" for name in ACCOUNT_SECRET_FIELDS}
    secrets["cookie"], user_agent = split_user_agent(secrets["cookie"])
    return AccountCredentialsOut(id=row["id"], user_agent=user_agent, **secrets)


# Key Redis của spider-hub gắn theo từng tài khoản (constants/facebook.py, constants/threads.py) - xoá cùng tài khoản.
_ACCOUNT_REDIS_KEYS = ("session_cache", "storage_state", "comments_query", "replies_query", "adaptive_interval")


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int) -> dict[str, bool]:
    account = await accounts_db.get_account(account_id)
    await accounts_db.delete_account(account_id)
    if account is not None and account.get("platform") in ("facebook", "threads"):
        # Không dọn thì session cache + con trỏ active_account của tài khoản đã xoá vẫn còn: token-status vẫn báo tài
        # khoản đó và crawler vẫn dùng tiếp session của nó.
        platform, key = account["platform"], _account_key(account).strip().lower()
        client = get_redis_client()
        await client.delete(*(f"{REDIS_KEY_PREFIX}{platform}:{name}:{key}" for name in _ACCOUNT_REDIS_KEYS))
        active_key = f"{REDIS_KEY_PREFIX}{platform}:active_account"
        if str((await client.get(active_key)) or "").strip('"').strip().lower() == key:
            await client.delete(active_key)
        logger.info("account_deleted_sessions_purged", platform=platform, account=key)
    return {"ok": True}


_NURTURE_PLATFORMS = ("threads",)


@router.post("/accounts/nurture", response_model=NurtureResponse)
async def nurture_accounts(payload: NurtureRequest) -> NurtureResponse:
    """Xếp hàng một lượt lướt bảng tin Threads bằng phiên đã lưu, không tự đăng nhập.
    Facebook không còn luồng nuôi; chỉ cần bật account và lịch crawl."""
    targets: list[tuple[str, str | None]] = []
    if payload.account_id is not None:
        account = await accounts_db.get_account(payload.account_id)
        if account is None:
            raise NotFoundError(f"Account {payload.account_id} not found")
        platform = account["platform"]
        if platform not in _NURTURE_PLATFORMS:
            raise ValidationError("Session warm-up is only supported for Threads accounts")
        needle = _account_key(account)
        targets.append((platform, needle))
    else:
        platforms = _NURTURE_PLATFORMS if payload.platform == "all" else (payload.platform,)
        targets.extend((platform, None) for platform in platforms)

    queued = 0
    for platform, account_key in targets:
        ok = await publish_nurture_request(
            platform,
            account_key,
            visits=payload.visits,
        )
        if ok:
            queued += 1
    if queued == 0:
        raise UpstreamError("Could not queue session warm-up")
    return NurtureResponse(ok=True, queued=queued)


@router.post("/accounts/{account_id}/check", response_model=AccountOut)
async def check_account(account_id: int) -> AccountOut:
    """Kiểm tra sức khoẻ thụ động - đọc các tín hiệu spider-hub vốn đã duy trì (bộ đếm bị
    chặn trong Redis, cache session) thay vì gửi request mới tới nền tảng. Xem
    app/services/accounts/account_health.py."""
    account = await accounts_db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    status = await evaluate_account_health(account)
    row = await accounts_db.update_account_check_result(account_id, status=status)
    return AccountOut.masked(row)


@router.post("/accounts/{account_id}/totp-code", response_model=TotpCodeResponse)
async def get_totp_code(account_id: int) -> TotpCodeResponse:
    """Mã 2FA 6 chữ số hiện tại của tài khoản này, tính từ totp_secret đã lưu của nó -
    đúng phép tính mà một app authenticator làm. Dành cho người đang tự đăng nhập *bằng
    tay* khi việc đăng nhập tự động không qua được - họ vẫn đọc mã rồi tự gõ, endpoint
    này chỉ đỡ cho họ phải chạy `pyotp.TOTP(secret).now()` trong terminal."""
    account = await accounts_db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    # App/người bán thường cho secret dạng "ABCD EFGH ...", pyotp từ chối khoảng trắng.
    secret = "".join((account.get("totp_secret") or "").split())
    if not secret:
        raise ValidationError(f"Account {account_id} has no 2FA secret configured")
    totp = TOTP(secret)
    expires_in = totp.interval - (int(time.time()) % totp.interval)
    try:
        code = totp.now()
    except (binascii.Error, ValueError) as exc:
        raise ValidationError(f"Account {account_id} has an invalid 2FA secret") from exc
    return TotpCodeResponse(code=code, expires_in_seconds=expires_in)


@router.post("/accounts/{account_id}/enabled", response_model=AccountOut)
async def set_account_enabled(account_id: int, payload: AccountSetEnabled) -> AccountOut:
    """Nút bật/tắt tài khoản trên dashboard - bật lại cũng gỡ checkpoint/cooldown (xem
    config_db/accounts.set_account_enabled)."""
    row = await accounts_db.set_account_enabled(account_id, payload.enabled)
    await accounts_db.record_account_event(row, "account_enabled" if payload.enabled else "account_switched_off")
    return AccountOut.masked(row)


@router.post("/accounts/{account_id}/reset-cookies")
async def reset_tiktok_cookies(account_id: int) -> dict[str, bool]:
    """Kích hoạt việc lấy lại danh tính TikTok (device_id/odinId) chạy headless của
    spider-hub cho đúng một dòng tài khoản này - xem publish_tiktok_identity_reset
    trong app/clients/kafka.py. Chỉ dành cho TikTok: Facebook/Threads dùng các route
    token_refresh cho cả nền tảng (xem app/api/routes/token_refresh.py), chạy lại luồng
    bootstrap trình duyệt bằng mật khẩu/2FA của riêng chúng thay vì nhắm một dòng."""
    account = await accounts_db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    if account["platform"] != "tiktok":
        raise ValidationError("Cookie reset is only supported for tiktok accounts")
    ok = await publish_tiktok_identity_reset(account_id)
    return {"ok": ok}


@router.get("/proxies", response_model=list[ProxyOut])
async def list_proxies(platform: str | None = Query(default=None)) -> list[ProxyOut]:
    rows = await accounts_db.list_proxies(platform)
    return [ProxyOut(**row) for row in rows]


@router.post("/proxies", response_model=ProxyOut)
async def create_proxy(payload: ProxyCreate) -> ProxyOut:
    row = await accounts_db.create_proxy(payload.model_dump())
    return ProxyOut(**row)


@router.patch("/proxies/{proxy_id}", response_model=ProxyOut)
async def update_proxy(proxy_id: int, payload: ProxyUpdate) -> ProxyOut:
    row = await accounts_db.update_proxy(proxy_id, payload.model_dump(exclude_unset=True))
    return ProxyOut(**row)


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int) -> dict[str, bool]:
    await accounts_db.delete_proxy(proxy_id)
    return {"ok": True}


def _proxy_settings_out(row: dict) -> ProxySettingsOut:
    settings = row.get("settings") if isinstance(row.get("settings"), dict) else {}
    merged = proxy_settings_db.merge_proxy_settings(settings)
    return ProxySettingsOut(values=ProxySettings(**merged), defaults=ProxySettings(), updated_at=row.get("updated_at"))


@router.get("/proxy", response_model=ProxySettingsOut)
async def get_proxy_settings() -> ProxySettingsOut:
    """Các tham số tinh chỉnh hành vi proxy mà spider-hub đọc (ghim proxy, cooldown, health
    check, nhịp gọi API của nhà cung cấp, backoff khi xếp hàng lại, số lần thử danh tính
    synthetic của TikTok)."""
    return _proxy_settings_out(await proxy_settings_db.get_proxy_settings())


@router.put("/proxy", response_model=ProxySettingsOut)
async def set_proxy_settings(payload: ProxySettings) -> ProxySettingsOut:
    if payload.cooldown_base_minutes > payload.cooldown_max_minutes:
        raise ValidationError("cooldown_base_minutes must not exceed cooldown_max_minutes")
    if payload.exhausted_backoff_base_seconds > payload.exhausted_backoff_max_seconds:
        raise ValidationError("exhausted_backoff_base_seconds must not exceed exhausted_backoff_max_seconds")
    row = await proxy_settings_db.upsert_proxy_settings(payload.model_dump())
    logger.info("proxy_settings_updated", **payload.model_dump())
    return _proxy_settings_out(row)


def _proxy_provider_out(row: dict) -> ProxyProviderOut:
    return ProxyProviderOut(
        key=row["key"],
        api_url=row.get("api_url") or "",
        token_set=bool(row.get("token")),
        proxy_count=int(row.get("proxy_count") or 0),
        updated_at=row.get("updated_at"),
        last_used_at=row.get("last_used_at"),
        last_status=row.get("last_status"),
    )


@router.get("/proxy/providers", response_model=list[ProxyProviderOut])
async def list_proxy_providers() -> list[ProxyProviderOut]:
    """Các nguồn proxy của nhà cung cấp (dòng proxy_providers) - nơi duy nhất
    spider-hub đọc key của nhà cung cấp. Không bao giờ trả về key, chỉ cho biết đã
    lưu key hay chưa."""
    return [_proxy_provider_out(row) for row in await proxy_settings_db.list_proxy_providers()]


@router.put("/proxy/providers/{key}", response_model=ProxyProviderOut)
async def set_proxy_provider(key: str, payload: ProxyProviderUpdate) -> ProxyProviderOut:
    """Upsert - có thể thêm nguồn proxy mới từ đây. Bỏ trống hoặc không gửi key thì
    giữ nguyên key đã lưu.

    Mỗi nguồn chỉ gồm 1 link reset + 1 key: link reset được gọi để xoay IP của cổng
    đã ghim, key đi trong placeholder {token}."""
    if not re.fullmatch(r"[a-z0-9_]{1,64}", key):
        raise ValidationError("provider key must be 1-64 chars of a-z, 0-9, _")
    token = payload.token.strip() if payload.token and payload.token.strip() else None
    row = await proxy_settings_db.upsert_proxy_provider(key, api_url=payload.api_url.strip(), token=token)
    logger.info("proxy_provider_updated", key=key, api_url=payload.api_url.strip(), token_changed=token is not None)
    return _proxy_provider_out(row)


@router.delete("/proxy/providers/{key}")
async def delete_proxy_provider(key: str) -> dict[str, bool]:
    """Xoá nguồn proxy. Nguồn đang được dùng (có dòng platform_proxies trỏ tới, hoặc là
    nguồn của tiktok_synthetic_provider) thì bị từ chối - xoá nó sẽ để những proxy đó mồ
    côi mà không ai hỏi."""
    await proxy_settings_db.delete_proxy_provider(key)
    logger.info("proxy_provider_deleted", key=key)
    return {"ok": True}


@router.get("/filter-keywords", response_model=list[FilterKeywordOut])
async def list_filter_keywords(category: str | None = Query(default=None)) -> list[FilterKeywordOut]:
    rows = await schedules_db.list_filter_keywords(category)
    return [FilterKeywordOut(**row) for row in rows]


@router.post("/filter-keywords", response_model=FilterKeywordOut)
async def create_filter_keyword(payload: FilterKeywordCreate) -> FilterKeywordOut:
    row = await schedules_db.create_filter_keyword(payload.model_dump())
    return FilterKeywordOut(**row)


@router.patch("/filter-keywords/{keyword_id}", response_model=FilterKeywordOut)
async def update_filter_keyword(keyword_id: int, payload: FilterKeywordUpdate) -> FilterKeywordOut:
    row = await schedules_db.update_filter_keyword(keyword_id, payload.model_dump(exclude_unset=True))
    return FilterKeywordOut(**row)


@router.delete("/filter-keywords/{keyword_id}")
async def delete_filter_keyword(keyword_id: int) -> dict[str, bool]:
    await schedules_db.delete_filter_keyword(keyword_id)
    return {"ok": True}


@router.get("/crawl-schedule", response_model=list[CrawlScheduleOut])
async def list_crawl_schedule() -> list[CrawlScheduleOut]:
    rows = await schedules_db.list_crawl_schedules()
    return [_crawl_schedule_out(row) for row in rows]


def _crawl_schedule_out(row: dict[str, Any]) -> CrawlScheduleOut:
    times = parse_run_times(row["run_time"]) or ["07:00"]
    return CrawlScheduleOut(**{**row, "run_time": times[0], "run_times": times})


@router.put("/crawl-schedule/{platform}", response_model=CrawlScheduleOut)
async def set_crawl_schedule(platform: str, payload: CrawlScheduleUpdate) -> CrawlScheduleOut:
    """Upsert - một nền tảng chưa có dòng nào cho tới khi lịch của nó được lưu lần đầu (xem
    app/services/crawl/scheduler/, chỗ đó chỉ bỏ qua nền tảng chưa có dòng chứ không coi đó
    là "mỗi nửa đêm" hay một mặc định ngầm nào khác)."""
    row = await schedules_db.upsert_crawl_schedule(
        platform,
        run_time=",".join(payload.run_times or []),
        enabled=payload.enabled,
        nurture_before=payload.nurture_before and platform != "facebook",
        nurture_after=payload.nurture_after and platform != "facebook",
    )
    return _crawl_schedule_out(row)


@router.get("/comment-schedule", response_model=list[CommentScheduleOut])
async def list_comment_schedule() -> list[CommentScheduleOut]:
    rows = await schedules_db.list_comment_crawl_schedules()
    return [CommentScheduleOut(**row) for row in rows]


@router.put("/comment-schedule/{platform}", response_model=CommentScheduleOut)
async def set_comment_schedule(platform: str, payload: CommentScheduleUpdate) -> CommentScheduleOut:
    """Upsert - một nền tảng chưa có dòng ở đây cho tới khi lượt quét comment của nó được
    lưu lần đầu, giống /crawl-schedule/{platform} phía trên."""
    row = await schedules_db.upsert_comment_crawl_schedule(
        platform, run_time=payload.run_time, enabled=payload.enabled, top_n=payload.top_n
    )
    return CommentScheduleOut(**row)


# --- Dọn bài không liên quan (app/services/relevance/cleanup.py) ---
# Cùng dạng với /settings/proxy: giá trị đã lưu được trộn lên trên mặc định của
# schema, nên một dòng có từ trước khi thêm trường mới (hoặc thiếu key) chỉ nhận giá
# trị mặc định cho key đó thay vì làm trang lỗi 500.


# --- Cấu hình Omar API (dev.omar-thing.site) --------------------------------
# Toggle dùng Omar API thay vì spider-hub cho crawl video/hashtag TikTok.
