from __future__ import annotations

import re
from datetime import date, datetime
from itertools import pairwise
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

FilterKeywordCategory = Literal["movie_relevant", "spam_offtopic"]
# Người vận hành tự đánh dấu, chỉ để phân biệt khi gán tài khoản: pool không đọc trường này. "rotating" là IP do
# nhà cung cấp đổi, "unknown" là chưa đo.
ProxyIpType = Literal["unknown", "static", "rotating"]


class AccountOut(BaseModel):
    """Một tài khoản nền tảng trả về dashboard. Các trường bí mật (mật khẩu, cookie, token, TOTP...) luôn rỗng ở đây
    (xem `masked`); giá trị thật chỉ lấy qua `GET /settings/accounts/{id}/credentials`. Các cột pool/cooldown do
    spider-hub ghi, ở đây chỉ đọc và đều cho phép null để một dòng cũ không làm cả danh sách lỗi 500."""

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
    # Ghi chú chẩn đoán mỗi khi tài khoản này bị tắt cứng - được xoá về null ở lần đăng nhập
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
    # Client duy nhất được dùng phiên này (manual | browser | http); null = chưa khai báo, Facebook không gửi gì.
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
    # cookie không kèm mục useragent=...; UA đã giải mã nằm ở user_agent (app/services/accounts/user_agent.py).
    cookie: str = ""
    token: str = ""
    email_password: str = ""
    user_agent: str = ""


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
    # UA trình duyệt đã tạo cookie (Facebook/Threads) - được ghép vào cookie dạng useragent=<base64>.
    user_agent: str = ""


class AccountSetEnabled(BaseModel):
    enabled: bool


class AccountUpdate(BaseModel):
    account_id: str | None = None
    password: str | None = None
    totp_secret: str | None = None
    cookie: str | None = None
    token: str | None = None
    email: str | None = None
    email_password: str | None = None
    enabled: bool | None = None
    user_agent: str | None = None


class ProxyOut(BaseModel):
    id: int
    platform: str
    proxy_url: str
    username: str
    password: str
    login_use_proxy: bool
    ip_type: ProxyIpType = "unknown"
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
    # Nguồn proxy sở hữu dòng này (NULL = nhập tay/tĩnh) - xem
    # platform_config_db.list_proxies. Trước đó không có quan hệ này, code đoán chủ
    # quan bằng cách so proxy_url với token của từng gói.
    provider_key: str | None = None
    # Mã riêng của cổng này trong hệ thống nhà cung cấp - điền vào placeholder {ref} của
    # link reset (vd proxy_id của webtui).
    provider_ref: str | None = None
    # Lần nguồn đó được gọi gần nhất + kết quả ("ok" / "error: ...").
    provider_last_used_at: datetime | None = None
    provider_last_status: str | None = None


def _clean_proxy_credential(value: str | None, field: str) -> str | None:
    """Bỏ khoảng trắng đầu/cuối; username không được chứa ":" (Basic auth tách user:pass ở dấu ":" đầu tiên). 16/11/2026
    proxy TikTok lưu username "leokelly130:" do tách sai dòng host:port:user:pass -> proxy trả 407 cả buổi."""
    if value is None:
        return None
    value = value.strip()
    if field == "username" and ":" in value:
        raise ValueError("Proxy username must not contain ':' - check how host:port:user:pass is split")
    return value


class ProxyCreate(BaseModel):
    platform: str = "all"
    proxy_url: str
    username: str = ""
    password: str = ""
    login_use_proxy: bool = False
    ip_type: ProxyIpType = "unknown"
    enabled: bool = True
    # Nguồn proxy sở hữu dòng này (NULL = nhập tay/tĩnh).
    provider_key: str | None = Field(default=None, max_length=64)
    # Mã riêng của cổng trong hệ thống nhà cung cấp (placeholder {ref} của link reset).
    provider_ref: str | None = Field(default=None, max_length=200)

    @field_validator("username", "password")
    @classmethod
    def _clean_credentials(cls, value, info):
        return _clean_proxy_credential(value, info.field_name)


class ProxyUpdate(BaseModel):
    platform: str | None = None
    proxy_url: str | None = None
    username: str | None = None
    password: str | None = None
    login_use_proxy: bool | None = None
    ip_type: ProxyIpType | None = None
    enabled: bool | None = None
    provider_key: str | None = Field(default=None, max_length=64)
    provider_ref: str | None = Field(default=None, max_length=200)

    @field_validator("username", "password")
    @classmethod
    def _clean_credentials(cls, value, info):
        return _clean_proxy_credential(value, info.field_name)


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


MAX_CRAWL_RUNS_PER_DAY = 8  # 3 -> 8: lượt sau trong ngày của FB/Threads chỉ lấy bài mới nhất, nhẹ
# Hai giờ chạy cách nhau ít nhất chừng này phút: scheduler.py còn bắn bù một lượt bị lỡ tới 30 phút sau giờ của nó.
MIN_CRAWL_RUN_GAP_MINUTES = 60
_HHMM = r"^([01]\d|2[0-3]):[0-5]\d$"


def parse_run_times(value: str | None) -> list[str]:
    """Cột crawl_schedules.run_time: một hoặc tối đa MAX_CRAWL_RUNS_PER_DAY giờ "HH:MM" cách nhau bằng dấu phẩy
    (từ 2026-10-09 - trước đó chỉ một giờ), đã sắp xếp."""
    return sorted({part.strip() for part in (value or "").split(",") if part.strip()})


class CrawlScheduleOut(BaseModel):
    platform: str
    # Giờ chạy đầu tiên trong ngày (giữ cho client cũ) - đủ danh sách ở run_times.
    run_time: str
    run_times: list[str] = []
    enabled: bool
    last_triggered_date: date | None = None
    nurture_before: bool = False
    nurture_after: bool = False
    updated_at: datetime


class CrawlScheduleUpdate(BaseModel):
    # "HH:MM", 24 giờ, được app/services/crawl/scheduler/ hiểu theo giờ Asia/Ho_Chi_Minh - xem
    # module đó để biết vì sao một múi giờ cố định là đủ (một người vận hành, không cần múi
    # giờ riêng theo nền tảng).
    # Client cũ gửi một giờ ở run_time; mới gửi run_times (1-3 giờ). Validator gộp về run_times.
    run_time: str | None = Field(default=None, pattern=_HHMM)
    run_times: list[str] | None = None
    enabled: bool = True
    nurture_before: bool = False
    nurture_after: bool = False

    @model_validator(mode="after")
    def _merge_run_times(self) -> CrawlScheduleUpdate:
        times = sorted(set(self.run_times or ([self.run_time] if self.run_time else [])))
        if not times:
            raise ValueError("At least one run time is required")
        if len(times) > MAX_CRAWL_RUNS_PER_DAY:
            raise ValueError(f"At most {MAX_CRAWL_RUNS_PER_DAY} run times per day")
        if any(not re.match(_HHMM, t) for t in times):
            raise ValueError("Run times must use the HH:MM format")
        minutes = [int(t[:2]) * 60 + int(t[3:]) for t in times]
        gaps = [b - a for a, b in pairwise(minutes)] + [minutes[0] + 1440 - minutes[-1]]
        if len(times) > 1 and min(gaps) < MIN_CRAWL_RUN_GAP_MINUTES:
            raise ValueError(f"Run times must be at least {MIN_CRAWL_RUN_GAP_MINUTES} minutes apart")
        self.run_times = times
        self.run_time = times[0]
        return self


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
    # Từ số bài NÓNG mỗi phim được theo dõi comment mỗi giờ (3-30); run_time là giờ chạy lượt mẫu
    # phân tầng (và 12 tiếng sau) - xem app/services/crawl/comment_planner.py.
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
    platform: Literal["threads", "all"] = "all"
    account_id: int | None = None
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
    # Model của nhà cung cấp đang chọn (rỗng nếu chưa chọn nhà cung cấp).
    model: str
    # Nhà cung cấp đang chọn đã có đủ base_url, api_key và model chưa.
    configured: bool
    prompts: list[AiPromptOut]
    # Nhà cung cấp đang chọn - dùng cho MỌI lời gọi AI, rỗng nếu chưa chọn.
    active_provider: str = ""
    updated_at: datetime | None = None


class AiActiveProviderUpdate(BaseModel):
    provider: str = Field(min_length=1, max_length=40, pattern="^[a-z0-9_-]+$")


class AiSettingsUpdate(BaseModel):
    enabled: bool
    prompts: dict[str, str] = Field(default_factory=dict)


class AiProviderOut(BaseModel):
    key: str
    base_url: str
    # Có lưu secret hay chưa - không bao giờ trả về api_key thô.
    api_key_set: bool
    model: str
    reasoning_effort: str = ""
    # Đây có phải nhà cung cấp đang chọn không.
    active: bool = False
    updated_at: datetime | None = None


class AiProviderUpdate(BaseModel):
    base_url: str = Field(min_length=1, max_length=500)
    # None/rỗng thì giữ nguyên secret đã lưu, để đổi base_url/model mà không phải gửi lại key mỗi lần.
    api_key: str | None = Field(default=None, max_length=500)
    model: str = Field(default="", max_length=120)
    # Mức suy luận cho model suy luận (vd Meta Muse: minimal, low, medium, high). Rỗng = không gửi; None = giữ giá trị cũ.
    reasoning_effort: str | None = Field(default=None, max_length=20, pattern="^[a-z]*$")


class AiModelsRequest(BaseModel):
    """Lấy danh sách model của một nhà cung cấp để chọn: api_key trống thì dùng key đã lưu của `key`."""

    base_url: str = Field(min_length=1, max_length=500)
    api_key: str | None = Field(default=None, max_length=500)
    key: str | None = Field(default=None, max_length=40, pattern="^[a-z0-9_-]+$")


class AiModelsOut(BaseModel):
    models: list[str]


class CronJob(BaseModel):
    name: str
    schedule: str
    source: str
    description: str
    last_run_at: datetime | None = None


# --- Hành vi proxy -------------------------------------------------------
# NGUỒN SỰ THẬT DUY NHẤT của cấu hình cào: spider-hub không có giá trị mặc định riêng mà
# chỉ đọc dòng proxy_settings; cinemark-api ghi đủ mọi khoá (mặc định bên dưới) khi khởi
# động (platform_config_db.ensure_proxy_settings_complete). Lưu thành một object jsonb trong dòng
# singleton proxy_settings (xem platform_config_db.get_proxy_settings).


class ProxySettings(BaseModel):
    """Mọi cài đặt cào chỉnh được từ dashboard (bảng `proxy_settings`): circuit-breaker proxy, health check, API xoay
    IP, backoff khi hết proxy, TikTok synthetic, số làn/độ sâu cào... Spider-hub đọc lại trong vòng một phút nên
    không cần restart. Thêm khoá mới thì phải thêm cả mặc định ở `social_crawler/db/proxy_settings.py` của
    spider-hub."""

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
    # API xoay IP của nhà cung cấp (clients/proxy_provider.py của spider-hub)
    provider_request_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    # Giãn cách tối thiểu giữa hai lần gọi cùng một gói - nhà cung cấp từ chối lời gọi
    # quá dồn dập.
    provider_min_call_interval_seconds: float = Field(default=60.0, ge=0, le=3600)
    provider_max_cooldown_wait_seconds: float = Field(default=120.0, ge=0, le=3600)
    # crawl_request_consumer.py - backoff khi xếp hàng lại lúc cả pool proxy của một nền tảng bị sập
    exhausted_backoff_base_seconds: float = Field(default=30.0, gt=0, le=3600)
    exhausted_backoff_growth_factor: float = Field(default=2.0, ge=1, le=10)
    exhausted_backoff_max_seconds: float = Field(default=300.0, gt=0, le=86400)
    exhausted_max_requeues: int = Field(default=3, ge=0, le=100)
    # Danh tính khách synthetic của TikTok (spiders/tiktok/client.py)
    tiktok_synthetic_provider: str = Field(default="tiktok_us", min_length=1, max_length=64)
    tiktok_hashtag_max_attempts: int = Field(default=8, ge=1, le=50)
    # Thăm dò quá chỗ TikTok báo hết feed hashtag (17/11/2026, spider-hub .../hashtag_search/probe.py): hasMore=false chỉ là
    # hết của MỘT cursor - gửi cursor lớn hơn vẫn ra video. Số request thăm dò cho mỗi lượt chạy, 0 = tắt.
    tiktok_probe_requests: int = Field(default=30, ge=0, le=500)
    tiktok_comments_max_attempts: int = Field(default=8, ge=1, le=50)
    # Hiệu năng cào (13/11/2026) - spider-hub đọc trong vòng một phút, không cần restart. Đây là nguồn sự thật duy nhất: spider-hub chỉ đọc dòng
    # proxy_settings mà cinemark-api ghi đủ khi khởi động (platform_config_db.ensure_proxy_settings_complete).
    # Chia làn (spider-hub runtime/lane_budget.py): lanes_total chia đều cho YouTube / page Facebook công khai / kênh Threads /
    # TikTok (mặc định 16 -> mỗi nhóm 4). Ba số dưới đặt > 0 thì nhóm đó dùng số riêng; 0 = tự chia.
    lanes_total: int = Field(default=16, ge=4, le=128)
    youtube_lanes: int = Field(default=0, ge=0, le=64)
    public_pages_workers: int = Field(default=0, ge=0, le=32)
    youtube_filter_concurrency: int = Field(default=6, ge=1, le=12)
    youtube_search_pages: int = Field(default=30, ge=1, le=200)
    youtube_reply_pages: int = Field(default=50, ge=0, le=500)
    public_pages_per_proxy: int = Field(default=3, ge=1, le=10)
    public_pages_gap_seconds: float = Field(default=3.0, ge=0, le=600)
    threads_channel_workers: int = Field(default=0, ge=0, le=16)
    # Bật/tắt trên dashboard (16/11/2026): TikTok mỗi job chạy cả feed hashtag lẫn tìm từ khoá rồi gộp (spider-hub
    # consumer/jobs/search.py); tác vụ public chạy liên tục khi hàng đợi trống - YouTube (crawl/public_continuous.py) và
    # kênh Threads không đăng nhập (crawl/threads_channels.py).
    tiktok_combine_search: bool = True
    youtube_continuous: bool = True
    threads_channels_continuous: bool = True
    # spider-hub consumer/jobs/youtube.py lấy transcript cho video mới (lưu post_transcripts, dùng khi lọc liên quan).
    youtube_transcripts: bool = True
    # Phía cinemark-api (scheduler)
    public_pages_queue_target: int = Field(default=20, ge=0, le=500)
    threads_channels_queue_target: int = Field(default=8, ge=0, le=200)
    youtube_comments_per_round: int = Field(default=1000, ge=0, le=10000)
    # Bỏ qua cào bình luận tự động của bài có ít hơn chừng này bình luận (18/11/2026) - mọi lượt tự động: bộ lập lịch bình luận
    # Facebook/Threads/TikTok (app/services/crawl/comment_planner.py), YouTube, hàng đợi bình luận Omar. 0 = tắt (vẫn cần >= 1).
    # Nút cào tay một bài cụ thể không bị chặn.
    comments_min_count: int = Field(default=10, ge=0, le=100000)
    # Mô hình phân loại tầng tác giả chạy local (app/services/ai_jobs/author_model.py, 18/11/2026): chỉ tự áp nhãn có xác suất >=
    # ngưỡng này; tác giả chưa chắc để job AI hoặc lần train sau.
    author_model_threshold: float = Field(default=0.9, ge=0.5, le=1.0)
    # Bình luận công khai Facebook (spider-hub spiders/facebook/auth/public_comments.py): proxy `public` ghim theo id.
    public_comments_proxy_id: int = Field(default=29, ge=1)
    public_comments_daily_cap: int = Field(default=400, ge=1, le=100000)
    public_comments_interval_seconds: float = Field(default=5.0, ge=1, le=600)
    public_comments_max_requests: int = Field(default=400, ge=1, le=100000)
    # Hạn mức request Facebook/Threads mỗi tài khoản (spider-hub runtime/search_budget.py, facebook_run_budget.py) - trước 17/11/2026 là
    # biến .env FACEBOOK_*; .env không còn được đọc. Giờ dạng "HH:MM,HH:MM" (giờ Việt Nam); các danh sách số cách nhau dấu phẩy.
    fb_daily_request_cap: int = Field(default=400, ge=0, le=100000)  # 0 = tắt mọi hạn mức ngày
    fb_cap_rest_until: str = Field(default="05:30", pattern=r"^\d{2}:\d{2}$")
    fb_budget_periods: str = Field(default="05:30,12:00,18:00", max_length=200)
    fb_fresh_share: float = Field(default=0.4, ge=0, le=1)
    fb_hourly_request_cap: int = Field(default=0, ge=0, le=100000)
    threads_hourly_request_cap: int = Field(default=0, ge=0, le=100000)
    fb_fresh_periods: str = Field(default="06:00,09:00,12:00,15:00,18:00,21:00", max_length=200)
    fb_deep_periods: str = Field(
        default="06:00,07:00,08:00,09:00,10:00,11:00,12:00,13:00,14:00,15:00,16:00,17:00,18:00,19:00,20:00,21:00,22:00",
        max_length=300,
    )
    fb_new_account_ramp: str = Field(default="0.2,0.4,0.7", max_length=100)
    fb_new_account_daily_cap: int = Field(default=500, ge=0, le=100000)
    fb_restart_daily_caps: str = Field(default="100,150,200,300", max_length=100)
    fb_restart_idle_days: int = Field(default=3, ge=0, le=365)
    fb_run_max_requests: int = Field(default=100, ge=0, le=100000)
    fb_run_max_seconds: int = Field(default=1800, ge=0, le=86400)
    fb_run_rest_seconds: int = Field(default=900, ge=0, le=86400)
    # Nhịp job tài khoản Meta (18/11/2026, trước đây env của spider-hub consumer/loop.py): giờ yên lặng, khoảng nghỉ giữa hai job,
    # giờ bỏ job tìm kiếm Facebook nằm chờ quá lâu. spider-hub đọc mỗi job, không cần restart.
    meta_quiet_hours: str = Field(default="23:30-05:30", pattern=r"^(\d{2}:\d{2}-\d{2}:\d{2})?$")
    facebook_job_pause_min_seconds: float = Field(default=30.0, ge=0, le=3600)
    facebook_job_pause_max_seconds: float = Field(default=90.0, ge=0, le=3600)
    facebook_comments_pause_min_seconds: float = Field(default=15.0, ge=0, le=3600)
    facebook_comments_pause_max_seconds: float = Field(default=30.0, ge=0, le=3600)
    threads_job_pause_min_seconds: float = Field(default=10.0, ge=0, le=3600)
    threads_job_pause_max_seconds: float = Field(default=30.0, ge=0, le=3600)
    other_job_pause_min_seconds: float = Field(default=3.0, ge=0, le=3600)
    other_job_pause_max_seconds: float = Field(default=8.0, ge=0, le=3600)
    facebook_stale_job_hours: float = Field(default=6.0, ge=0, le=72)
    facebook_fresh_stale_job_hours: float = Field(default=3.0, ge=0, le=72)
    facebook_deep_stale_job_hours: float = Field(default=2.0, ge=0, le=72)
    # Proxy công khai (không tài khoản) - trước 17/11/2026 là biến .env FB_PUBLIC_PAGES_*, THREADS_CHANNEL_*, FB_LOGGED_OUT_*, ...;
    # .env không còn được đọc, sửa ở Settings › Proxy của dashboard.
    fb_public_discovery_proxy_id: int = Field(default=31, ge=1, le=1000000)
    fb_public_pages_block_strikes: int = Field(default=3, ge=1, le=20)
    fb_public_pages_cooldown_minutes: int = Field(default=120, ge=1, le=1440)
    fb_public_pages_max_pages: int = Field(default=0, ge=0, le=1000)
    fb_public_pages_since_days: int = Field(default=60, ge=1, le=3650)
    fb_public_pages_window_days: int = Field(default=7, ge=0, le=365)
    fb_public_pages_ip_check_seconds: int = Field(default=60, ge=5, le=3600)
    fb_public_pages_request_interval: float = Field(default=3.0, ge=0.0, le=120.0)
    fb_public_pages_max_pace: int = Field(default=8, ge=1, le=64)
    fb_public_pages_min_rest_seconds: int = Field(default=300, ge=0, le=86400)
    fb_public_pages_doc_id: str = Field(default="", max_length=64)
    fb_public_yield_to_search: bool = False
    threads_channel_since_days: int = Field(default=30, ge=1, le=3650)
    threads_channel_max_pages: int = Field(default=20, ge=1, le=500)
    threads_channel_request_interval: float = Field(default=3.0, ge=0.0, le=120.0)
    threads_channel_cooldown_minutes: int = Field(default=30, ge=1, le=1440)
    threads_channel_rotating_retries: int = Field(default=3, ge=0, le=20)
    fb_logged_out_proxy_ids: str = Field(default="", max_length=200)
    fb_logged_out_delay_seconds: float = Field(default=4.0, ge=0.0, le=120.0)
    fb_logged_out_jitter_seconds: float = Field(default=2.0, ge=0.0, le=60.0)
    fb_logged_out_block_strikes: int = Field(default=3, ge=1, le=20)
    fb_logged_out_cooldown_minutes: int = Field(default=60, ge=1, le=1440)
    youtube_proxy_rest_seconds: int = Field(default=600, ge=10, le=86400)


class ProxySettingsOut(BaseModel):
    values: ProxySettings
    defaults: ProxySettings
    updated_at: datetime | None = None


class ProxyProviderOut(BaseModel):
    key: str
    # Link đổi IP của nguồn - có thể chứa placeholder {token} / {proxy} / {username} /
    # {password} / {ref}. Xem clients/proxy_provider.py của spider-hub.
    api_url: str
    # Có lưu key (token) trong DB hay chưa - không bao giờ trả về token thô.
    token_set: bool
    updated_at: datetime | None = None
    # Lần spider-hub thực sự gọi nguồn này + kết quả ("ok" / "error: ...").
    last_used_at: datetime | None = None
    last_status: str | None = None
    # Số proxy đang bật thuộc nguồn này (mỗi proxy thuộc đúng một nguồn).
    proxy_count: int = 0


class ProxyProviderUpdate(BaseModel):
    """Cấu hình một nguồn proxy: một link reset + một key. Mọi nhà cung cấp dùng chung
    một cơ chế nên không cần phân loại."""

    api_url: str = Field(min_length=0, max_length=500, pattern=r"^$|^https?://")
    # None/rỗng thì giữ nguyên key đã lưu.
    token: str | None = Field(default=None, max_length=500)


# --- Dọn bài không liên quan (app/services/relevance/cleanup.py) ---
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


class CleanupSettings(BaseModel):
    # "HH:MM", 24 giờ, Asia/Ho_Chi_Minh (xem TIMEZONE trong app/services/crawl/scheduler/).
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


# --- Cấu hình Omar API (dev.omar-thing.site) --------------------------------
# Dùng cho crawl video/hashtag thay vì spider-hub khi toggle bật trên dashboard.
# API key được đọc từ D1 (bảng omar_api_settings), không từ env.


class DiscoverySettings(BaseModel):
    """Công tắc AI tự thêm nguồn cào (hashtag TikTok, câu tìm kiếm FB/Threads, trang nguồn FB) - xem
    app/services/scheduler/discovery.discovery_enabled. Tắt: chỉ cào các từ khoá / trang đang có."""

    enabled: bool
    updated_at: datetime | None = None


class DiscoverySettingsUpdate(BaseModel):
    enabled: bool


class OmarApiSettingsOut(BaseModel):
    # Có api_key hay chưa - không bao giờ trả về key thô.
    api_key_set: bool
    base_url: str
    # Trần request/phút (0 = theo gói).
    requests_per_minute: int = 0
    # Tổng request mỗi ngày người dùng đặt (0 = theo hạn mức ngày của gói) và số request đồng thời (0 = mặc định).
    daily_requests: int = 0
    concurrency: int = 0
    # Chỉ có sau khi PUT đổi key: GET /usage báo key dùng được hay không (None = chưa kiểm tra). Key đúng thì lượt cào chạy liên tục.
    key_valid: bool | None = None
    usage: dict[str, Any] | None = None
    updated_at: datetime | None = None


class OmarApiSettingsUpdate(BaseModel):
    api_key: str | None = Field(default=None, max_length=500)
    base_url: str | None = Field(default=None, max_length=200)
    # None = giữ nguyên; 0 = theo giới hạn của gói.
    requests_per_minute: int | None = Field(default=None, ge=0, le=10000)
    daily_requests: int | None = Field(default=None, ge=0, le=10_000_000)
    concurrency: int | None = Field(default=None, ge=0, le=64)
