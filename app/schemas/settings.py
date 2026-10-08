from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

FilterKeywordCategory = Literal["movie_relevant", "spam_offtopic"]


class AccountOut(BaseModel):
    id: int
    platform: str
    account_id: str
    password: str
    totp_secret: str
    cookie: str
    token: str
    email: str
    email_password: str
    enabled: bool
    created_at: datetime
    updated_at: datetime
    last_checked_at: datetime | None = None
    last_check_status: str | None = None
    # Chẩn đoán do AI sinh ra (xem diagnose_account_failure trong clients/kira.py của
    # spider-hub) mỗi khi tài khoản này bị tắt cứng - được xoá về null ở lần đăng nhập
    # thành công kế tiếp.
    last_check_note: str | None = None
    # Pool / circuit-breaker + ghim proxy cố định - do spider-hub ghi (services/pool.py,
    # db/accounts.py), ở đây chỉ đọc (xem platform_config_db.ACCOUNT_CREATE_COLUMNS, vốn
    # loại các cột này ra). pool_status là "active" | "checkpoint" (xem
    # db.record_account_outcome) - khác với last_check_status ở trên. Không bắt buộc dù cột
    # có NOT NULL DEFAULT (xem platform_config_db._ensure_pool_columns): một dòng có từ
    # trước khi có default đó, hoặc được ghi qua đường không áp default, không được làm cả
    # danh sách tài khoản lỗi 500.
    pool_status: str | None = "active"
    cooldown_until: datetime | None = None
    consecutive_failures: int | None = 0
    last_used_at: datetime | None = None
    assigned_proxy_id: int | None = None
    # Tên các trường bí mật đang có giá trị. Các trường đó luôn trả về rỗng ở đây (xem
    # AccountOut.masked) - giá trị thật chỉ lấy qua GET /settings/accounts/{id}/credentials.
    secrets_set: list[str] = Field(default_factory=list)

    @classmethod
    def masked(cls, row: dict[str, Any]) -> AccountOut:
        data = dict(row)
        # Danh sách tải bằng list_accounts(with_secrets=False) chỉ có cờ has_<trường>.
        data["secrets_set"] = [
            name for name in ACCOUNT_SECRET_FIELDS if data.pop(f"has_{name}", False) or data.get(name)
        ]
        for name in ACCOUNT_SECRET_FIELDS:
            data[name] = ""
        return cls(**data)


ACCOUNT_SECRET_FIELDS = ("password", "totp_secret", "cookie", "token", "email_password")


class AccountCredentialsOut(BaseModel):
    id: int
    password: str = ""
    totp_secret: str = ""
    cookie: str = ""
    token: str = ""
    email_password: str = ""


class AccountCreate(BaseModel):
    platform: str
    account_id: str
    password: str = ""
    totp_secret: str = ""
    cookie: str = ""
    token: str = ""
    email: str = ""
    email_password: str = ""
    enabled: bool = True


class AccountSetProxy(BaseModel):
    proxy_id: int


class AccountUpdate(BaseModel):
    account_id: str | None = None
    password: str | None = None
    totp_secret: str | None = None
    cookie: str | None = None
    token: str | None = None
    email: str | None = None
    email_password: str | None = None
    enabled: bool | None = None


class ProxyOut(BaseModel):
    id: int
    platform: str
    proxy_url: str
    username: str
    password: str
    login_use_proxy: bool
    enabled: bool
    created_at: datetime
    updated_at: datetime
    # Pool / circuit-breaker - do spider-hub ghi (db/proxies.py), ở đây chỉ đọc.
    # pool_status là "active" | "degraded". Không bắt buộc vì cùng lý do chưa migrate/chịu
    # được NULL như AccountOut ở trên.
    pool_status: str | None = "active"
    cooldown_until: datetime | None = None
    consecutive_failures: int | None = 0
    last_used_at: datetime | None = None
    # Số tài khoản đang được ghim cố định vào proxy này - xem
    # platform_config_db.list_proxies. Chỉ endpoint danh sách mới điền giá trị này.
    assigned_account_count: int = 0


class ProxyCreate(BaseModel):
    platform: str = "all"
    proxy_url: str
    username: str = ""
    password: str = ""
    login_use_proxy: bool = False
    enabled: bool = True


class ProxyUpdate(BaseModel):
    platform: str | None = None
    proxy_url: str | None = None
    username: str | None = None
    password: str | None = None
    login_use_proxy: bool | None = None
    enabled: bool | None = None


class FilterKeywordOut(BaseModel):
    id: int
    keyword: str
    category: FilterKeywordCategory
    enabled: bool
    created_at: datetime
    updated_at: datetime


class FilterKeywordCreate(BaseModel):
    keyword: str
    category: FilterKeywordCategory
    enabled: bool = True


class FilterKeywordUpdate(BaseModel):
    keyword: str | None = None
    category: FilterKeywordCategory | None = None
    enabled: bool | None = None


class CrawlScheduleOut(BaseModel):
    platform: str
    run_time: str
    enabled: bool
    last_triggered_date: date | None = None
    nurture_before: bool = False
    nurture_after: bool = False
    updated_at: datetime


class CrawlScheduleUpdate(BaseModel):
    # "HH:MM", 24 giờ, được app/services/scheduler.py hiểu theo giờ Asia/Ho_Chi_Minh - xem
    # module đó để biết vì sao một múi giờ cố định là đủ (một người vận hành, không cần múi
    # giờ riêng theo nền tảng).
    run_time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    enabled: bool = True
    nurture_before: bool = False
    nurture_after: bool = False


class CommentScheduleOut(BaseModel):
    platform: str
    run_time: str
    enabled: bool
    top_n: int
    last_triggered_date: date | None = None
    updated_at: datetime


class CommentScheduleUpdate(BaseModel):
    run_time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    enabled: bool = True
    # Từ 2026-10-08: số bài NÓNG mỗi phim được theo dõi comment mỗi giờ (3-30); run_time là giờ chạy lượt mẫu
    # phân tầng (và 12 tiếng sau) - xem app/services/comment_planner.py.
    top_n: int = Field(default=15, ge=1, le=500)


# --- Import tài khoản/proxy có AI hỗ trợ - xem app/ai/tasks/import_parser.py ---


class ImportParseRequest(BaseModel):
    target: Literal["accounts", "proxies"]
    format_hint: str
    content: str


class ImportParseResponse(BaseModel):
    rows: list[dict[str, Any]]


class ImportCommitRequest(BaseModel):
    target: Literal["accounts", "proxies"]
    platform: str
    rows: list[dict[str, Any]]


class ImportCommitResponse(BaseModel):
    created: int
    failed: int


class NurtureRequest(BaseModel):
    platform: Literal["facebook", "threads", "all"] = "all"
    account_id: int | None = None
    like: bool = True
    comment: bool = False  # tắt mặc định - xem --comment trong nurture_accounts.py của spider-hub
    visits: int = Field(default=3, ge=0, le=8)


class NurtureResponse(BaseModel):
    ok: bool
    queued: int


class TotpCodeResponse(BaseModel):
    # Mã 6 chữ số hiện tại, tính từ totp_secret đã lưu của chính tài khoản - đúng phép
    # tính mà mọi app authenticator làm. Hiển thị cho người tự hoàn tất đăng nhập *bằng
    # tay* (khi đăng nhập tự động của spider-hub không qua được) - endpoint này không bao
    # giờ tự gõ mã vào đâu cả.
    code: str
    expires_in_seconds: int


class AiPromptOut(BaseModel):
    task: str
    system_prompt: str
    default_system_prompt: str


class AiSettingsOut(BaseModel):
    enabled: bool
    model: str
    # Dòng "kira" trong ai_providers đã có đủ base_url và api_key chưa - thiếu thì nút
    # bật/tắt trên dashboard không gọi được provider.
    configured: bool
    prompts: list[AiPromptOut]
    # Provider mà phần tạo report topic mạng xã hội của app/ai/tasks/report.py gọi - "kira"
    # hoặc "bee", độc lập với `enabled` ở trên (cổng đó chỉ dành cho các bộ phân loại lúc
    # ingest; tạo report là hành động do người vận hành/lịch kích hoạt, cùng nhóm với
    # import trong Settings).
    active_report_provider: str
    updated_at: datetime | None = None


class AiSettingsUpdate(BaseModel):
    enabled: bool
    model: str = Field(min_length=1, max_length=120)
    prompts: dict[str, str] = Field(default_factory=dict)
    active_report_provider: str = Field(default="kira", pattern="^(kira|bee)$")


class AiProviderOut(BaseModel):
    key: str
    base_url: str
    # Có lưu secret hay chưa - không bao giờ trả về api_key thô.
    api_key_set: bool
    model: str
    updated_at: datetime | None = None


class AiProviderUpdate(BaseModel):
    base_url: str = Field(min_length=1, max_length=500)
    # None/rỗng thì giữ nguyên secret đã lưu, để dashboard đổi base_url/model mà không phải
    # gửi lại key mỗi lần.
    api_key: str | None = Field(default=None, max_length=500)
    model: str = Field(min_length=1, max_length=120)


class CronJob(BaseModel):
    name: str
    schedule: str
    source: str
    description: str
    last_run_at: datetime | None = None


# --- Hành vi proxy -------------------------------------------------------
# Giống DEFAULTS trong social_crawler/db/proxy_settings.py của spider-hub - giá trị mặc
# định của các trường bên dưới CHÍNH LÀ giá trị dự phòng spider-hub dùng khi thiếu
# key, nên khi thêm key phải giữ hai bên đồng bộ. Lưu thành một object jsonb trong dòng
# singleton proxy_settings (xem platform_config_db.get_proxy_settings).


class ProxySettings(BaseModel):
    # Ghim proxy cố định (services/pool.py)
    repin_after_consecutive_failures: int = Field(default=5, ge=1, le=100)
    # Cooldown của circuit-breaker: base * 2^failures, có trần (db/proxies.py)
    cooldown_base_minutes: float = Field(default=5.0, gt=0, le=240)
    cooldown_max_minutes: float = Field(default=120.0, gt=0, le=10080)
    # proxy_health_check.py (cron, mỗi 5 phút)
    health_check_ping_url: str = Field(
        default="https://www.google.com/generate_204", min_length=8, max_length=500, pattern=r"^https?://"
    )
    health_check_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    health_check_alert_after_failures: int = Field(default=2, ge=1, le=100)
    health_check_streak_ttl_hours: float = Field(default=6.0, gt=0, le=168)
    # API thuê proxy xoay vòng của nhà cung cấp (clients/proxy_provider.py)
    provider_request_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    provider_min_get_new_interval_seconds: float = Field(default=60.0, ge=0, le=3600)
    provider_max_cooldown_wait_seconds: float = Field(default=120.0, ge=0, le=3600)
    # crawl_request_consumer.py - backoff khi xếp hàng lại lúc cả pool proxy của một nền tảng bị sập
    exhausted_backoff_base_seconds: float = Field(default=30.0, gt=0, le=3600)
    exhausted_backoff_growth_factor: float = Field(default=2.0, ge=1, le=10)
    exhausted_backoff_max_seconds: float = Field(default=300.0, gt=0, le=86400)
    exhausted_max_requeues: int = Field(default=3, ge=0, le=100)
    # Danh tính khách synthetic của TikTok (spiders/tiktok/client.py)
    tiktok_synthetic_provider: str = Field(default="proxiestrust_tiktok_us", min_length=1, max_length=64)
    tiktok_hashtag_max_attempts: int = Field(default=8, ge=1, le=50)
    tiktok_comments_max_attempts: int = Field(default=8, ge=1, le=50)


class ProxySettingsOut(BaseModel):
    values: ProxySettings
    defaults: ProxySettings
    updated_at: datetime | None = None


class ProxyProviderOut(BaseModel):
    key: str
    api_url: str
    # Có lưu token trong DB hay chưa - không bao giờ trả về token thô.
    token_set: bool
    ip_allowlist: bool
    updated_at: datetime | None = None


class ProxyProviderUpdate(BaseModel):
    api_url: str = Field(min_length=8, max_length=500, pattern=r"^https?://")
    # None/rỗng thì giữ nguyên token đã lưu.
    token: str | None = Field(default=None, max_length=500)
    ip_allowlist: bool = False


# --- Dọn bài không liên quan (app/services/cleanup.py) ---
# Các tham số sửa được trên dashboard cho lượt dọn hằng ngày các bài gắn nhãn
# not_related cùng comment/snapshot của chúng. Cùng kiểu dòng singleton với
# ProxySettings (xem platform_config_db.get_cleanup_settings). Giá trị mặc định khớp
# với giá trị dự phòng từ env trong app/core/config.py để khi DB chưa có dòng thì hành
# vi vẫn giống cấu hình trước khi có dashboard.
#
# Ghi chú lịch sử: các dòng dropped_posts cũng từng bị dọn ở đây cho tới khi lake
# writer đảm nhận việc lưu trữ mọi quyết định loại bài qua topic Kafka
# ingest_decisions (xem app/clients/kafka.py:publish_ingest_decision +
# app/workers/lake_writer/main.py). Tham số retention_days đã bỏ vì lý do đó - trong
# D1 không còn gì để xoá theo tuổi nữa.

CLEANUP_REASONS_LABEL = "irrelevant_post_purge"


class CleanupSettings(BaseModel):
    # "HH:MM", 24 giờ, Asia/Ho_Chi_Minh (xem TIMEZONE trong app/services/scheduler.py).
    run_time: str = Field(default="03:00", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    enabled: bool = True
    # Bài được giữ chừng này thời gian sau khi bị gắn nhãn not_related rồi mới đủ điều kiện
    # bị xoá - cho cơ hội phát hiện một lần bộ phân loại chạy sai trước khi các nhãn sai
    # của nó biến mất.
    grace_hours: int = Field(default=24, ge=0, le=720)


class CleanupSettingsOut(BaseModel):
    values: CleanupSettings
    defaults: CleanupSettings
    last_run_at: datetime | None = None
    last_run_summary: dict[str, Any] | None = None
    running: bool = False
    updated_at: datetime | None = None


class CleanupSettingsUpdate(BaseModel):
    run_time: str | None = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    enabled: bool | None = None
    grace_hours: int | None = Field(default=None, ge=0, le=720)


class CleanupRunHistoryEntry(BaseModel):
    started_at: datetime
    finished_at: datetime | None = None
    dry_run: bool
    triggered_by: Literal["schedule", "manual"]
    posts_deleted: int = 0
    comments_deleted: int = 0
    snapshots_deleted: int = 0
    batches: int = 0
    remaining_posts: int | None = None
    error: str | None = None


# --- Bộ lập lịch auto-login (app/services/auto_login.py) ---
# Các tham số sửa được trên dashboard cho bộ lập lịch auto-login chạy mỗi giờ. Một dòng
# singleton trong auto_login_settings (cùng kiểu với ProxySettings / CleanupSettings -
# xem platform_config_db.get_auto_login_settings cho phía lưu trữ). Giá trị mặc định
# khớp với giá trị dự phòng từ env mà auto_login/scheduler.py của spider-hub dùng, nên
# một bản deploy mới với env AUTO_LOGIN_ENABLED=true + bật setting mới trên dashboard
# chạy giống hệt nhau dù bộ lập lịch chạy ở spider-hub hay cinemark-api.
#
# `platforms` là danh sách nền tảng duyệt qua mỗi lượt; consumer của spider-hub đặt
# consumer group theo danh sách này (mỗi nền tảng một group) để hàng đợi đăng nhập lại
# của Facebook không chặn đầu hàng của Threads (và ngược lại). `dry_run` được tôn
# trọng ở cả producer (vẫn publish message Kafka, gắn dry_run, để consumer log "lẽ ra
# đã đăng nhập lại tài khoản này" và không ghi gì) lẫn consumer (không thực sự mở
# Playwright). `telegram_alert` giống tuỳ chọn cùng tên trong CleanupSettings - mặc
# định tắt vì bản thân auto-login ban đầu cũng mặc định tắt.


class AutoLoginSettings(BaseModel):
    enabled: bool = False
    interval_seconds: int = Field(default=3600, ge=60, le=86400)
    platforms: list[Literal["facebook", "threads"]] = Field(default_factory=lambda: ["facebook", "threads"])
    dry_run: bool = False
    # Bỏ qua tài khoản có last_checked_at mới hơn chừng này giây - tránh để bộ lập lịch
    # dồn dập vào một tài khoản mà check_facebook_cookies.py vừa đánh dấu chết và chưa đủ
    # thời gian để trục trặc tạm thời qua đi. Mặc định 0 = không cooldown, xử lý mọi tài
    # khoản "chết" ở mỗi lượt (hành vi ban đầu).
    min_age_seconds: int = Field(default=0, ge=0, le=3600)
    telegram_alert: bool = True
    # Cron kiểm tra cookie (2026-10-07): mỗi cookie_check_interval_hours xếp một request type=cookie_check để
    # spider-hub chạy scripts/check_facebook_cookies.py (không đăng nhập, chỉ mở FB bằng phiên sẵn có). Tài khoản bị
    # FB đăng xuất được ghi last_check_status='dead' -> lượt auto-login kế tiếp nạp cookie mới. Độc lập với `enabled`:
    # tắt auto-login thì cron vẫn phát hiện + báo Telegram để nạp cookie tay.
    cookie_check_enabled: bool = True
    cookie_check_interval_hours: int = Field(default=6, ge=1, le=48)
    # Tối đa bấy nhiêu lần đăng nhập lại mỗi nền tảng mỗi lượt (2026-10-07): một lượt cookie check có thể đánh dấu
    # chết cả chục tài khoản, và đăng nhập lại hết trong vài phút qua 2 proxy (consumer chỉ nghỉ 5-10s giữa các lần)
    # là kiểu đăng nhập hàng loạt từ một IP mà FB gắn cờ. Phần còn lại chờ các lượt sau (mỗi interval_seconds).
    max_logins_per_tick: int = Field(default=2, ge=1, le=20)


class AutoLoginSettingsOut(BaseModel):
    values: AutoLoginSettings
    defaults: AutoLoginSettings
    running: bool = False
    last_run_at: datetime | None = None
    updated_at: datetime | None = None


class AutoLoginSettingsUpdate(BaseModel):
    enabled: bool | None = None
    interval_seconds: int | None = Field(default=None, ge=60, le=86400)
    platforms: list[Literal["facebook", "threads"]] | None = None
    dry_run: bool | None = None
    min_age_seconds: int | None = Field(default=None, ge=0, le=3600)
    telegram_alert: bool | None = None
    cookie_check_enabled: bool | None = None
    cookie_check_interval_hours: int | None = Field(default=None, ge=1, le=48)
    max_logins_per_tick: int | None = Field(default=None, ge=1, le=20)


class AutoLoginRunHistoryEntry(BaseModel):
    started_at: datetime
    finished_at: datetime | None = None
    triggered_by: Literal["schedule", "manual"]
    dry_run: bool
    interval_seconds: int
    platforms: list[str]
    # Khối JSON lấy từ auto_login_run_history.per_platform. Dạng:
    # { "<platform>": { "attempted": N, "relogged_in": N, ... } }
    # Dashboard hiển thị thành một bảng chi tiết nhỏ theo nền tảng dưới mỗi dòng lịch sử.
    per_platform: dict[str, Any] = Field(default_factory=dict)
    total_attempted: int = 0
    total_relogged_in: int = 0
    total_needs_human: int = 0
    total_failed: int = 0
    total_error: int = 0
    kafka_published: int = 0
    kafka_publish_failed: int = 0
    error: str | None = None
