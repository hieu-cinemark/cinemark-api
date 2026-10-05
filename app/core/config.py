"""Cấu hình trung tâm của app - một instance Settings, đọc từ biến môi trường / .env
lúc import. Mọi module khác đọc cấu hình qua `settings`, không gọi thẳng
`os.getenv(...)`, để chỉ có đúng một chỗ biết service này cần những biến môi trường
nào."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    # Đường dẫn tuyệt đối để uvicorn/ingest khởi động từ thư mục khác vẫn đọc đúng .env
    # của repo này (".env" tương đối sẽ bỏ sót DB_MODE đặt rõ trong đó).
    model_config = SettingsConfigDict(
        env_file=str(_REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    log_level: str = "INFO"
    log_format: str = "console"  # "console" (dễ đọc cho người) | "json" (prod, máy đọc được)

    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None

    kafka_bootstrap_servers: str = "localhost:9092"

    # Cùng instance Redis mà RedisCache của spider-hub kết nối tới (xem
    # social_crawler/clients/redis.py bên đó) - phía này chỉ đọc, chỉ để báo trạng thái
    # cache session Facebook. Tên biến môi trường giống bên spider-hub để dùng chung một
    # file .env được.
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None

    cors_origins: str = "http://localhost:3000"

    # Khoá dùng chung cho mọi request (header X-API-Key) - xem app/core/auth.py. Để trống
    # thì API không yêu cầu xác thực (hành vi cũ).
    api_auth_key: str | None = None

    # Cloudflare D1 - cho ingest consumer đọc/ghi database D1 của cinemark-scraper qua
    # HTTP query API của Cloudflare (binding D1 chỉ có bên trong Worker; đây là cách duy
    # nhất để vào từ một tiến trình VPS thường). Xem app/services/d1.py. Tất cả đều không
    # bắt buộc - thiếu thì các lời gọi D1 bị bỏ qua (không phải lỗi).
    cloudflare_account_id: str | None = None
    cloudflare_api_token: str | None = None
    cloudflare_d1_database_id: str | None = None

    # "local" trỏ d1_query() của app.services.d1 vào một file SQLite local thay vì HTTP
    # query API của Cloudflare - không tốn quota đọc dòng hằng ngày của D1 khi dev/test ở
    # local. Trước đây mặc định trỏ tới bản sao do repo anh em cinemark-be duy trì
    # (scripts/pull-local-db.js) - repo đó không còn trong workspace này, nên bản sao giờ
    # nằm ở đây (xem scripts/pull_local_db.py, bản thay thế của repo này). Làm mới bằng
    # `python -m scripts.pull_local_db` khi dữ liệu local đã cũ (sao chép toàn bộ - mọi
    # bảng, mọi dòng - mới từ D1 thật mỗi lần chạy; chạy lại lúc nào cũng an toàn, nhưng
    # mỗi lần đều tốn quota đọc D1 thật).
    db_mode: str = "remote"  # "remote" | "local" — remote là Cloudflare D1
    local_db_path: str = str(_REPO_ROOT / ".local-db" / "scraper.sqlite")

    # Các file log mà route /logs của dashboard đọc phần cuối - cả hai tiến trình đều là
    # file text thường do ConsoleRenderer của structlog ghi ra (xem
    # spider-hub/social_crawler/logger.py và app/core/logging.py). Mặc định giả định bố
    # cục thư mục anh em mà workspace này đang dùng (cinemark-api/ và spider-hub/ nằm cạnh
    # nhau); ghi đè trong .env nếu bản deploy đặt chúng ở chỗ khác.
    spider_hub_consumer_log_path: str = str(_REPO_ROOT.parent / "spider-hub" / "consumer.log")
    ingest_consumer_log_path: str = str(_REPO_ROOT / "ingest_consumer.log")

    # Cùng instance Supabase Postgres mà social_crawler/db/ của spider-hub đọc cấu hình
    # tài khoản/proxy (bảng platform_accounts / platform_proxies) - phía này có cả quyền
    # đọc/ghi, cho trang Settings của dashboard. Thiếu thì là None (không phải lỗi) -
    # GET/POST/PATCH/DELETE trên /settings/* chỉ trả 502 thay vì app không khởi động được.
    database_url: str | None = None

    # Bộ phân loại LLM Kira (kiraai.vn). Bật/tắt lúc chạy và prompt theo từng task nằm ở
    # tab AI trong Settings của dashboard (bảng ai_settings trên Supabase) - biến môi
    # trường này chỉ cung cấp giá trị enabled ban đầu khi dòng đó được tạo lần đầu. Thông
    # tin đăng nhập (base_url/api_key) và model, của Kira và mọi provider khác (ví dụ
    # Beeknoee), nằm trong bảng ai_providers trên Supabase - xem app/ai/client.py và
    # app/services/platform_config_db.py. Công dụng chính hiện nay: phân loại liên quan,
    # cảm xúc comment và report topic/narrative (Beeknoee chỉ là tuỳ chọn cho report).
    kira_enabled: bool = False

    # Kira là bộ phân loại độ liên quan của bài lúc ingest (app/ai/tasks/post_relevance.py):
    # tối đa chừng này bài mỗi ngày UTC. Gom lô nên mỗi bài tốn khoảng 330 token (đo
    # 2026-10-05: ~2.000 token vào + ~435 token ra cho mỗi lô ~7 bài) - cùng tài khoản Kira còn
    # chạy cảm xúc comment (app/ai/tasks/sentiment.py), nên giữ chỗ cho task đó. Vượt hạn mức
    # thì bài chỉ còn quy tắc dự phòng (relevance_rules.has_film_context). 0 là tắt Kira cho bài.
    kira_post_relevance_daily_cap: int = 30000

    # Phim "chặt": tên phim trùng cụm từ thông dụng, nên kể cả bài Kira gán "related" cũng phải có
    # tín hiệu gắn với đúng phim (relevance_rules.film_context_reason: đạo diễn/diễn viên, "phim
    # <tên>", từ điện ảnh sát tên phim...) mới được hiện. Danh sách slug cách nhau bằng dấu phẩy.
    # Không bật cho mọi phim: với tên phim riêng biệt, 20-50% bài thật không có tín hiệu đó.
    strict_relevance_movie_slugs: str = "nguoi-duoc-chon"

    # Dọn dữ liệu bài không liên quan hằng ngày (app/services/cleanup.py), do
    # app/services/scheduler.py chạy lúc irrelevant_post_purge_time (giờ
    # Asia/Ho_Chi_Minh). Bài gắn nhãn not_related được giữ qua thời gian ân hạn trước để
    # vẫn còn phát hiện được một lượt phân loại sai. Việc giữ các dòng dropped_posts lịch
    # sử đã bỏ - lake writer (app/workers/lake_writer/main.py) giờ lưu trữ mọi quyết định
    # loại bài dưới bronze/entity=decisions/.
    irrelevant_post_purge_enabled: bool = True
    irrelevant_post_purge_time: str = "03:00"
    irrelevant_post_grace_hours: int = 24

    # Data lake R2 qua S3 API (app/clients/lake.py) - một token chỉ có quyền trên bucket
    # lake. Không bắt buộc: chỉ lake writer và script backfill lake cần, và writer từ chối
    # khởi động nếu thiếu.
    r2_endpoint: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    lake_bucket: str = "cinemark-lake"

    @property
    def strict_relevance_movies(self) -> frozenset[str]:
        return frozenset(slug.strip() for slug in self.strict_relevance_movie_slugs.split(",") if slug.strip())

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
