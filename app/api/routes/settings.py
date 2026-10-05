"""Trang Settings của dashboard: CRUD trên các bảng platform_accounts /
platform_proxies ở Supabase (xem app/services/platform_config_db.py) - chính là các
bảng mà spider-hub đọc để lấy thông tin đăng nhập và cấu hình proxy. Chỗ khác chỉ
đọc; đây là nơi duy nhất ghi vào các bảng đó."""

from __future__ import annotations

import asyncio
import re
import time

from fastapi import APIRouter, Query
from pydantic import ValidationError as PydanticValidationError
from pyotp import TOTP

from app.ai.tasks.import_parser import parse_import
from app.clients.kafka import publish_nurture_request, publish_tiktok_identity_reset
from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.core.logging import get_logger
from app.schemas.settings import (
    ACCOUNT_SECRET_FIELDS,
    AccountCreate,
    AccountCredentialsOut,
    AccountOut,
    AccountSetProxy,
    AccountUpdate,
    AiProviderOut,
    AiProviderUpdate,
    AiSettingsOut,
    AiSettingsUpdate,
    AutoLoginRunHistoryEntry,
    AutoLoginSettings,
    AutoLoginSettingsOut,
    AutoLoginSettingsUpdate,
    CleanupRunHistoryEntry,
    CleanupSettings,
    CleanupSettingsOut,
    CleanupSettingsUpdate,
    CommentScheduleOut,
    CommentScheduleUpdate,
    CrawlScheduleOut,
    CrawlScheduleUpdate,
    FilterKeywordCreate,
    FilterKeywordOut,
    FilterKeywordUpdate,
    ImportCommitRequest,
    ImportCommitResponse,
    ImportParseRequest,
    ImportParseResponse,
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
)
from app.services import auto_login as auto_login_svc
from app.services import platform_config_db as db
from app.services.account_health import evaluate_account_health
from app.services.platform_token import account_key as _account_key
from app.services.scheduler import purge_in_progress, run_purge_now

logger = get_logger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])

# Giữ tham chiếu mạnh tới các task chạy nền kiểu bắn-rồi-quên - event loop chỉ giữ
# tham chiếu yếu, nên task không ai tham chiếu có thể bị garbage-collect giữa chừng.
_background_tasks: set[asyncio.Task] = set()


@router.get("/accounts", response_model=list[AccountOut])
async def list_accounts(platform: str | None = Query(default=None)) -> list[AccountOut]:
    rows = await db.list_accounts(platform, with_secrets=False)
    return [AccountOut.masked(row) for row in rows]


@router.post("/accounts", response_model=AccountOut)
async def create_account(payload: AccountCreate) -> AccountOut:
    row = await db.create_account(payload.model_dump())
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
    if not changes:
        row = await db.get_account(account_id)
        if row is None:
            raise NotFoundError(f"Account {account_id} not found.")
        return AccountOut.masked(row)
    row = await db.update_account(account_id, changes)
    return AccountOut.masked(row)


@router.get("/accounts/{account_id}/credentials", response_model=AccountCredentialsOut)
async def get_account_credentials(account_id: int) -> AccountCredentialsOut:
    row = await db.get_account(account_id)
    if row is None:
        raise NotFoundError(f"Account {account_id} not found.")
    logger.info("account_credentials_viewed", account_id=account_id)
    return AccountCredentialsOut(id=row["id"], **{name: row.get(name) or "" for name in ACCOUNT_SECRET_FIELDS})


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int) -> dict[str, bool]:
    await db.delete_account(account_id)
    return {"ok": True}


_NURTURE_PLATFORMS = ("facebook", "threads")


@router.post("/accounts/nurture", response_model=NurtureResponse)
async def nurture_accounts(payload: NurtureRequest) -> NurtureResponse:
    """Xếp hàng một lượt "làm ấm" bảng tin Facebook/Threads (cuộn, vài lượt like, tối đa
    một comment ngắn, mở vài bài rồi quay lại). Tài khoản chưa có session trình duyệt
    đã lưu sẽ bị bỏ qua ở phía sau - việc này không bao giờ gõ mật khẩu. Không hỗ trợ
    TikTok."""
    targets: list[tuple[str, str | None]] = []
    if payload.account_id is not None:
        account = await db.get_account(payload.account_id)
        if account is None:
            raise NotFoundError(f"Account {payload.account_id} not found")
        platform = account["platform"]
        if platform not in _NURTURE_PLATFORMS:
            raise ValidationError("Session warm-up is only supported for Facebook and Threads accounts")
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
            like=payload.like,
            comment=payload.comment,
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
    app/services/account_health.py."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    status = await evaluate_account_health(account)
    row = await db.update_account_check_result(account_id, status=status)
    return AccountOut.masked(row)


@router.post("/accounts/{account_id}/totp-code", response_model=TotpCodeResponse)
async def get_totp_code(account_id: int) -> TotpCodeResponse:
    """Mã 2FA 6 chữ số hiện tại của tài khoản này, tính từ totp_secret đã lưu của nó -
    đúng phép tính mà một app authenticator làm. Dành cho người đang tự đăng nhập *bằng
    tay* khi việc đăng nhập tự động không qua được - họ vẫn đọc mã rồi tự gõ, endpoint
    này chỉ đỡ cho họ phải chạy `pyotp.TOTP(secret).now()` trong terminal."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    secret = (account.get("totp_secret") or "").strip()
    if not secret:
        raise ValidationError(f"Account {account_id} has no 2FA secret configured")
    totp = TOTP(secret)
    expires_in = totp.interval - (int(time.time()) % totp.interval)
    return TotpCodeResponse(code=totp.now(), expires_in_seconds=expires_in)


@router.post("/accounts/{account_id}/reset-proxy", response_model=AccountOut)
async def reset_account_proxy(account_id: int) -> AccountOut:
    """Xoá việc ghim proxy cố định của một tài khoản (xem
    app/services/platform_config_db.reset_account_proxy) - lần crawl/bootstrap kế tiếp
    nó sẽ được ghim lại vào proxy đang có ít tài khoản nhất. Dùng từ dashboard khi bỏ một
    proxy hoặc cân bằng lại sau khi thêm proxy mới."""
    row = await db.reset_account_proxy(account_id)
    return AccountOut.masked(row)


@router.post("/accounts/{account_id}/set-proxy", response_model=AccountOut)
async def set_account_proxy(account_id: int, payload: AccountSetProxy) -> AccountOut:
    """Ghim tay một tài khoản vào một proxy cụ thể - xem
    app/services/platform_config_db.set_account_proxy. Ngược với reset-proxy phía trên
    (xoá ghim để quay về tự gán)."""
    row = await db.set_account_proxy(account_id, payload.proxy_id)
    return AccountOut.masked(row)


@router.post("/accounts/{account_id}/reset-cookies")
async def reset_tiktok_cookies(account_id: int) -> dict[str, bool]:
    """Kích hoạt việc lấy lại danh tính TikTok (device_id/odinId) chạy headless của
    spider-hub cho đúng một dòng tài khoản này - xem publish_tiktok_identity_reset
    trong app/clients/kafka.py. Chỉ dành cho TikTok: Facebook/Threads dùng các route
    token_refresh cho cả nền tảng (xem app/api/routes/token_refresh.py), chạy lại luồng
    bootstrap trình duyệt bằng mật khẩu/2FA của riêng chúng thay vì nhắm một dòng."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    if account["platform"] != "tiktok":
        raise ValidationError("Cookie reset is only supported for tiktok accounts")
    ok = await publish_tiktok_identity_reset(account_id)
    return {"ok": ok}


@router.get("/proxies", response_model=list[ProxyOut])
async def list_proxies(platform: str | None = Query(default=None)) -> list[ProxyOut]:
    rows = await db.list_proxies(platform)
    return [ProxyOut(**row) for row in rows]


@router.post("/proxies", response_model=ProxyOut)
async def create_proxy(payload: ProxyCreate) -> ProxyOut:
    row = await db.create_proxy(payload.model_dump())
    return ProxyOut(**row)


@router.patch("/proxies/{proxy_id}", response_model=ProxyOut)
async def update_proxy(proxy_id: int, payload: ProxyUpdate) -> ProxyOut:
    row = await db.update_proxy(proxy_id, payload.model_dump(exclude_unset=True))
    return ProxyOut(**row)


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int) -> dict[str, bool]:
    await db.delete_proxy(proxy_id)
    return {"ok": True}


def _proxy_settings_out(row: dict) -> ProxySettingsOut:
    # Key lạ/cũ bị bỏ qua; giá trị đã lưu mà không còn qua được kiểm tra (ví dụ giới hạn
    # bị siết lại sau này) thì quay về mặc định thay vì làm cả trang Settings lỗi 500.
    stored = row.get("settings") if isinstance(row.get("settings"), dict) else {}
    merged = ProxySettings().model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            merged[key] = getattr(ProxySettings.model_validate({key: value}), key)
        except PydanticValidationError:
            logger.warning("proxy_setting_invalid_stored_value", key=key)
    return ProxySettingsOut(values=ProxySettings(**merged), defaults=ProxySettings(), updated_at=row.get("updated_at"))


@router.get("/proxy", response_model=ProxySettingsOut)
async def get_proxy_settings() -> ProxySettingsOut:
    """Các tham số tinh chỉnh hành vi proxy mà spider-hub đọc (ghim proxy, cooldown, health
    check, nhịp gọi API của nhà cung cấp, backoff khi xếp hàng lại, số lần thử danh tính
    synthetic của TikTok)."""
    return _proxy_settings_out(await db.get_proxy_settings())


@router.put("/proxy", response_model=ProxySettingsOut)
async def set_proxy_settings(payload: ProxySettings) -> ProxySettingsOut:
    if payload.cooldown_base_minutes > payload.cooldown_max_minutes:
        raise ValidationError("cooldown_base_minutes must not exceed cooldown_max_minutes")
    if payload.exhausted_backoff_base_seconds > payload.exhausted_backoff_max_seconds:
        raise ValidationError("exhausted_backoff_base_seconds must not exceed exhausted_backoff_max_seconds")
    row = await db.upsert_proxy_settings(payload.model_dump())
    logger.info("proxy_settings_updated", **payload.model_dump())
    return _proxy_settings_out(row)


def _proxy_provider_out(row: dict) -> ProxyProviderOut:
    return ProxyProviderOut(
        key=row["key"],
        api_url=row.get("api_url") or "",
        token_set=bool(row.get("token")),
        ip_allowlist=bool(row["ip_allowlist"]),
        updated_at=row.get("updated_at"),
    )


@router.get("/proxy/providers", response_model=list[ProxyProviderOut])
async def list_proxy_providers() -> list[ProxyProviderOut]:
    """Các gói proxy xoay vòng của nhà cung cấp (dòng proxy_providers) - nơi duy nhất
    spider-hub đọc token của nhà cung cấp. Không bao giờ trả về token, chỉ cho biết đã
    lưu token hay chưa."""
    return [_proxy_provider_out(row) for row in await db.list_proxy_providers()]


@router.put("/proxy/providers/{key}", response_model=ProxyProviderOut)
async def set_proxy_provider(key: str, payload: ProxyProviderUpdate) -> ProxyProviderOut:
    """Upsert - có thể thêm key nhà cung cấp mới từ đây. Bỏ trống hoặc không gửi token thì
    giữ nguyên token đã lưu."""
    if not re.fullmatch(r"[a-z0-9_]{1,64}", key):
        raise ValidationError("provider key must be 1-64 chars of a-z, 0-9, _")
    token = payload.token.strip() if payload.token and payload.token.strip() else None
    row = await db.upsert_proxy_provider(
        key, api_url=payload.api_url.strip(), token=token, ip_allowlist=payload.ip_allowlist
    )
    logger.info("proxy_provider_updated", key=key, api_url=payload.api_url.strip(), token_changed=token is not None)
    return _proxy_provider_out(row)


@router.get("/filter-keywords", response_model=list[FilterKeywordOut])
async def list_filter_keywords(category: str | None = Query(default=None)) -> list[FilterKeywordOut]:
    rows = await db.list_filter_keywords(category)
    return [FilterKeywordOut(**row) for row in rows]


@router.post("/filter-keywords", response_model=FilterKeywordOut)
async def create_filter_keyword(payload: FilterKeywordCreate) -> FilterKeywordOut:
    row = await db.create_filter_keyword(payload.model_dump())
    return FilterKeywordOut(**row)


@router.patch("/filter-keywords/{keyword_id}", response_model=FilterKeywordOut)
async def update_filter_keyword(keyword_id: int, payload: FilterKeywordUpdate) -> FilterKeywordOut:
    row = await db.update_filter_keyword(keyword_id, payload.model_dump(exclude_unset=True))
    return FilterKeywordOut(**row)


@router.delete("/filter-keywords/{keyword_id}")
async def delete_filter_keyword(keyword_id: int) -> dict[str, bool]:
    await db.delete_filter_keyword(keyword_id)
    return {"ok": True}


@router.get("/crawl-schedule", response_model=list[CrawlScheduleOut])
async def list_crawl_schedule() -> list[CrawlScheduleOut]:
    rows = await db.list_crawl_schedules()
    return [CrawlScheduleOut(**row) for row in rows]


@router.put("/crawl-schedule/{platform}", response_model=CrawlScheduleOut)
async def set_crawl_schedule(platform: str, payload: CrawlScheduleUpdate) -> CrawlScheduleOut:
    """Upsert - một nền tảng chưa có dòng nào cho tới khi lịch của nó được lưu lần đầu (xem
    app/services/scheduler.py, chỗ đó chỉ bỏ qua nền tảng chưa có dòng chứ không coi đó
    là "mỗi nửa đêm" hay một mặc định ngầm nào khác)."""
    row = await db.upsert_crawl_schedule(
        platform,
        run_time=payload.run_time,
        enabled=payload.enabled,
        nurture_before=payload.nurture_before,
        nurture_after=payload.nurture_after,
    )
    return CrawlScheduleOut(**row)


@router.get("/comment-schedule", response_model=list[CommentScheduleOut])
async def list_comment_schedule() -> list[CommentScheduleOut]:
    rows = await db.list_comment_crawl_schedules()
    return [CommentScheduleOut(**row) for row in rows]


@router.put("/comment-schedule/{platform}", response_model=CommentScheduleOut)
async def set_comment_schedule(platform: str, payload: CommentScheduleUpdate) -> CommentScheduleOut:
    """Upsert - một nền tảng chưa có dòng ở đây cho tới khi lượt quét comment của nó được
    lưu lần đầu, giống /crawl-schedule/{platform} phía trên."""
    row = await db.upsert_comment_crawl_schedule(
        platform, run_time=payload.run_time, enabled=payload.enabled, top_n=payload.top_n
    )
    return CommentScheduleOut(**row)


async def _ai_settings_out(row: dict) -> AiSettingsOut:
    from app.ai.defaults import AI_PROMPT_TASKS, default_system_prompts

    provider = await db.get_ai_provider("kira")
    defaults = default_system_prompts()
    stored = row.get("prompts") if isinstance(row.get("prompts"), dict) else {}
    prompts = []
    for task in AI_PROMPT_TASKS:
        default = defaults.get(task, "")
        current = str(stored.get(task) or "").strip() or default
        prompts.append(
            {
                "task": task,
                "system_prompt": current,
                "default_system_prompt": default,
            }
        )
    return AiSettingsOut(
        enabled=bool(row.get("enabled")),
        # Lấy thẳng từ ai_providers - rỗng nghĩa là "chưa đặt", không bao giờ đoán.
        model=str((provider or {}).get("model") or ""),
        configured=bool(provider and provider.get("base_url") and provider.get("api_key")),
        prompts=prompts,
        active_report_provider=str(row.get("active_report_provider") or "kira"),
        updated_at=row.get("updated_at"),
    )


@router.get("/ai", response_model=AiSettingsOut)
async def get_ai_settings() -> AiSettingsOut:
    row = await db.get_ai_settings()
    return await _ai_settings_out(row)


@router.put("/ai", response_model=AiSettingsOut)
async def set_ai_settings(payload: AiSettingsUpdate) -> AiSettingsOut:
    from app.ai.client import invalidate_provider_cache
    from app.ai.defaults import AI_PROMPT_TASKS
    from app.ai.kira import invalidate_ai_runtime_cache

    allowed = set(AI_PROMPT_TASKS)
    prompts = {key: value for key, value in payload.prompts.items() if key in allowed and isinstance(value, str)}
    row = await db.upsert_ai_settings(
        enabled=payload.enabled, prompts=prompts, active_report_provider=payload.active_report_provider
    )

    existing_provider = await db.get_ai_provider("kira")
    await db.upsert_ai_provider(
        "kira",
        base_url=(existing_provider or {}).get("base_url") or "",
        api_key=None,
        model=payload.model.strip(),
    )
    invalidate_provider_cache("kira")
    invalidate_ai_runtime_cache()
    logger.info(
        "ai_settings_updated", enabled=payload.enabled, model=payload.model.strip(), prompt_tasks=sorted(prompts)
    )
    return await _ai_settings_out(row)


def _ai_provider_out(row: dict) -> AiProviderOut:
    return AiProviderOut(
        key=row["key"],
        base_url=row.get("base_url") or "",
        api_key_set=bool(row.get("api_key")),
        model=row.get("model") or "",
        updated_at=row.get("updated_at"),
    )


@router.get("/ai/providers", response_model=list[AiProviderOut])
async def list_ai_providers() -> list[AiProviderOut]:
    """Mọi LLM provider đã cấu hình ({key, base_url, model} - không bao giờ trả về
    api_key, chỉ cho biết đã đặt hay chưa). Xem app/ai/client.py."""
    rows = await db.list_ai_providers()
    return [_ai_provider_out(row) for row in rows]


@router.put("/ai/providers/{key}", response_model=AiProviderOut)
async def set_ai_provider(key: str, payload: AiProviderUpdate) -> AiProviderOut:
    """Upsert - `key` không cần có sẵn, nên có thể thêm provider mới từ đây mà không phải
    sửa code/schema. Bỏ trống hoặc không gửi api_key thì giữ nguyên secret đã lưu."""
    from app.ai.client import invalidate_provider_cache

    api_key = payload.api_key.strip() if payload.api_key and payload.api_key.strip() else None
    row = await db.upsert_ai_provider(
        key, base_url=payload.base_url.strip(), api_key=api_key, model=payload.model.strip()
    )
    invalidate_provider_cache(key)
    logger.info("ai_provider_updated", key=key, base_url=payload.base_url.strip(), model=payload.model.strip())
    return _ai_provider_out(row)


@router.post("/import/parse", response_model=ImportParseResponse)
async def import_parse(payload: ImportParseRequest) -> ImportParseResponse:
    """Bước 1 của import hàng loạt có AI hỗ trợ (xem app/ai/tasks/import_parser.py) - biến
    dữ liệu tài khoản/proxy dán vào dạng tự do thành các dòng ứng viên có cấu trúc để
    dashboard hiển thị thành bản xem trước sửa được. Không bao giờ tự ghi vào DB; xem
    /import/commit bên dưới cho việc đó."""
    try:
        rows = await parse_import(payload.target, payload.format_hint, payload.content)
    except Exception as exc:
        raise ValidationError(
            f"Could not parse the pasted content ({exc}) - try describing the format more precisely, "
            "or pasting a smaller batch."
        ) from exc
    if not rows:
        raise ValidationError("No rows could be extracted from the pasted content - check the format description.")
    return ImportParseResponse(rows=rows)


@router.post("/import/commit", response_model=ImportCommitResponse)
async def import_commit(payload: ImportCommitRequest) -> ImportCommitResponse:
    """Bước 2 - người vận hành đã xem/sửa bản xem trước của /import/parse và đang xác
    nhận. Ghi từng dòng bằng create_account/create_proxy (cùng đường cột được cho phép
    mà form thêm tài khoản/proxy thông thường dùng, xem platform_config_db.py) - dòng nào
    lỗi (ví dụ account_id trùng) thì bỏ qua, không làm hỏng phần còn lại của lô."""
    created = 0
    failed = 0
    for row in payload.rows:
        # Giữ "enabled" mà người vận hành đã đặt rõ khi sửa bản xem trước /import/parse (ví
        # dụ bỏ chọn một dòng đáng ngờ) - chỉ mặc định True khi dòng không có trường này.
        fields = {**row, "platform": payload.platform, "enabled": row.get("enabled", True)}
        try:
            if payload.target == "accounts":
                await db.create_account(fields)
            else:
                await db.create_proxy(fields)
            created += 1
        except Exception as exc:
            logger.warning("import_commit_row_failed", target=payload.target, error=str(exc))
            failed += 1
    return ImportCommitResponse(created=created, failed=failed)


# --- Dọn bài không liên quan (app/services/cleanup.py) ---
# Cùng dạng với /settings/proxy: giá trị đã lưu được trộn lên trên mặc định của
# schema, nên một dòng có từ trước khi thêm trường mới (hoặc thiếu key) chỉ nhận giá
# trị mặc định cho key đó thay vì làm trang lỗi 500.


def _cleanup_settings_out(row: dict, *, last_run: dict | None) -> CleanupSettingsOut:
    stored = row.get("settings") if isinstance(row.get("settings"), dict) else {}
    defaults = CleanupSettings()
    merged: dict = defaults.model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            merged[key] = getattr(CleanupSettings.model_validate({key: value}), key)
        except PydanticValidationError:
            logger.warning("cleanup_setting_invalid_stored_value", key=key)
    return CleanupSettingsOut(
        values=CleanupSettings(**merged),
        defaults=defaults,
        last_run_at=(last_run or {}).get("started_at") if last_run else None,
        last_run_summary=last_run,
        running=purge_in_progress(),
        updated_at=row.get("updated_at"),
    )


@router.get("/cleanup", response_model=CleanupSettingsOut)
async def get_cleanup_route() -> CleanupSettingsOut:
    """Các tham số đang có hiệu lực (giá trị lưu trên dashboard đè lên mặc định từ env),
    dòng của lần chạy gần nhất trong cleanup_run_history, và có lượt chạy nào đang diễn
    ra không (dashboard dùng thông tin này để khoá nút Run-now và hiện "Running…")."""
    row = await db.get_cleanup_settings()
    history = await db.list_cleanup_run_history(limit=1)
    return _cleanup_settings_out(row, last_run=history[0] if history else None)


@router.put("/cleanup", response_model=CleanupSettingsOut)
async def set_cleanup_route(payload: CleanupSettingsUpdate) -> CleanupSettingsOut:
    """Cập nhật một phần - key không có trong payload giữ nguyên giá trị đã lưu. Giống PUT
    của proxy-settings: không cần gửi lại mọi trường chỉ để bật/tắt."""
    allowed = {"run_time", "enabled", "grace_hours"}
    set_fields = set(payload.model_fields_set) & allowed
    values_to_write = {key: getattr(payload, key) for key in set_fields}
    row = await db.upsert_cleanup_settings(values_to_write)
    logger.info("cleanup_settings_updated", **values_to_write)
    history = await db.list_cleanup_run_history(limit=1)
    return _cleanup_settings_out(row, last_run=history[0] if history else None)


@router.post("/cleanup/run", response_model=dict)
async def run_cleanup_route() -> dict:
    """Kích hoạt tay từ dashboard. Trả về ngay {"started": True/False}; việc purge thật
    chạy trong task nền mà scheduler cũng dùng, nên người vận hành có thể chuyển trang
    mà không huỷ nó. `started: false` nghĩa là lượt trước vẫn đang chạy - dashboard nên
    hiển thị điều đó, không thử lại."""
    started = await run_purge_now()
    return {"started": started}


@router.get("/cleanup/history", response_model=list[CleanupRunHistoryEntry])
async def cleanup_history_route(limit: int = 20) -> list[CleanupRunHistoryEntry]:
    """Danh sách các dòng cleanup_run_history, mới nhất trước, giới hạn bởi `limit` (mặc
    định 20). Số liệu của mỗi mục là số id thật từ DELETE RETURNING trong cleanup.py -
    cùng con số được log trong `irrelevant_purge_done telegram=True`."""
    rows = await db.list_cleanup_run_history(limit=limit)
    return [CleanupRunHistoryEntry(**row) for row in rows]


# --- Bộ lập lịch auto-login (app/services/auto_login.py) ---
# Cùng dạng với /cleanup: GET trả về cấu hình đang có hiệu lực trên nền mặc định + cờ
# đang chạy + last_run_at; PUT là cập nhật một phần (giao diện bật/tắt trên dashboard
# chỉ gửi `{enabled: true}`); POST /run chạy một lượt riêng và trả về có thực sự bắt
# đầu không (False nếu đang có lượt khác chạy). /history cho xem các lượt gần đây.


@router.get("/auto-login", response_model=AutoLoginSettingsOut)
async def get_auto_login_route() -> AutoLoginSettingsOut:
    """Các tham số đang có hiệu lực (giá trị lưu trên dashboard đè lên mặc định từ env),
    cờ đang chạy (để dashboard làm mờ nút "Run now" khi có lượt đang chạy) và
    started_at của lượt gần nhất."""
    return await auto_login_svc.get_auto_login_settings_out()


@router.put("/auto-login", response_model=AutoLoginSettingsOut)
async def set_auto_login_route(payload: AutoLoginSettingsUpdate) -> AutoLoginSettingsOut:
    """Cập nhật một phần - key không có trong payload giữ nguyên giá trị đã lưu. Giống PUT
    của cleanup/proxy: không cần gửi lại mọi trường chỉ để bật/tắt.

    Kiểm tra bằng schema TRƯỚC khi ghi - interval_seconds sai (ví dụ <60) hay tên nền
    tảng lạ phải bị trả về lỗi ngay ở đây, không để làm hỏng vòng lặp scheduler về sau.
    """
    # Dựng bản sau khi trộn trước, để một key sai trả về 400 kèm thông báo rõ ràng thay
    # vì ghi được một nửa payload. Cùng cách làm với /cleanup - kiểu PutRecord của
    # schema_update_settings_update.
    set_fields = set(payload.model_fields_set) & set(AutoLoginSettings().model_dump().keys())
    if not set_fields:
        # Không có gì để cập nhật - chỉ trả về trạng thái hiện tại.
        return await auto_login_svc.get_auto_login_settings_out()
    values_to_write = {key: getattr(payload, key) for key in set_fields}
    # Kiểm tra giá trị sau khi trộn bằng schema để bắt lỗi ràng buộc trước khi tới
    # Supabase. Lấy cấu hình hiện tại, đè phần cập nhật lên, rồi cho Pydantic kiểm tra
    # kết quả.
    merged = (await auto_login_svc.resolve_auto_login_settings()).model_dump()
    merged.update(values_to_write)
    AutoLoginSettings(**merged)  # raise ValidationError nếu không hợp lệ
    await db.upsert_auto_login_settings(values_to_write)
    logger.info("auto_login_settings_updated", **values_to_write)
    return await auto_login_svc.get_auto_login_settings_out()


@router.post("/auto-login/run", response_model=dict)
async def run_auto_login_route() -> dict:
    """Kích hoạt tay từ nút "Run now" trên dashboard. Trả về ngay
    `{"started": True/False}`; lượt chạy thật chạy dưới dạng asyncio task nền, nên người
    vận hành có thể chuyển trang mà không huỷ nó. `started: false` nghĩa là lượt trước
    vẫn đang chạy (dashboard nên hiện "running..." thay vì thử lại)."""
    started = await _kick_auto_login_tick(triggered_by="manual")
    return {"started": started}


async def _kick_auto_login_tick(*, triggered_by: str) -> bool:
    """Lên lịch run_auto_login_tick thành một asyncio task bắn-rồi-quên, trả về True nếu
    thực sự đã lên lịch. Trả về False nếu đang có lượt khác chạy (khoá chống chạy trùng
    thật sự nằm trong run_auto_login_tick; hàm này chỉ quyết định sớm "lên lịch hay bỏ
    qua" để HTTP response có ý nghĩa với dashboard).

    Cùng kiểu bắn-rồi-quên mà scheduler.py dùng cho các task crawl/comment/auto_login -
    một lần publish Kafka chậm cho một tài khoản không được chặn chính request API đã yêu
    cầu chạy. Lỗi bên trong lượt chạy được bắt và lưu vào auto_login_run_history.error;
    không có gì nổi lên tới tầng HTTP.
    """
    if auto_login_svc.is_auto_login_in_flight():
        return False
    # force=True: bấm "Run now" thì vẫn chạy kể cả khi lịch đang tắt - nếu không, lượt
    # chạy trả về -1 mà không làm gì.
    task = asyncio.create_task(auto_login_svc.run_auto_login_tick(triggered_by=triggered_by, force=True))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return True


@router.get("/auto-login/history", response_model=list[AutoLoginRunHistoryEntry])
async def auto_login_history_route(limit: int = 20) -> list[AutoLoginRunHistoryEntry]:
    """Danh sách các dòng auto_login_run_history, mới nhất trước, giới hạn bởi `limit`
    (mặc định 20). Các cột `kafka_published` / `kafka_publish_failed` của mỗi dòng cho
    người vận hành biết bao nhiêu tài khoản trong số được thử đã thực sự tới được
    spider-hub (so với số bị rơi do Kafka sập giữa lượt)."""
    return await auto_login_svc.list_auto_login_history(limit=limit)
