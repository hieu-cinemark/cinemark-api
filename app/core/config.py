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
    # Việc PHÂN LOẠI - gán nhãn comment (cảm xúc, khía cạnh, giai đoạn), độ liên quan của bài, giá trị comment của bài:
    # "jev" = model quyết định của Bee (app/ai/decisions.py, từ 21/10/2026 thay cho gọi thẳng TypeSafe), chạy SAU luật và
    # ViSoBERT - chỉ ca khó; "llm" = chat Bee như trước. Bình luận dùng bee_comment_model, bài viết dùng bee_post_model.
    classifier: str = "jev"
    bee_comment_model: str = "typesafe/jev-1.14"
    bee_post_model: str = "openai/gpt-6-luna-decisions"
    # Model nhà trồng (ViSoBERT fine-tune, app/ai/visobert.py) chạy trước Bee cho cảm xúc bình luận và nội dung bài: đủ
    # chắc từ mức này thì lấy luôn, không gọi Bee. Thư mục model (gitignore, 374MB) - thiếu / thiếu torch thì bỏ qua bước này.
    visobert_enabled: bool = True
    visobert_min_confidence: float = 0.7
    visobert_model_dir: str = "models/visobert-cinemark"
    visobert_batch_size: int = 32
    # Model độ liên quan của bài (app/ai/visobert_relevance.py) chạy trước AI: lấy luôn khi đủ chắc theo ngưỡng từng nhãn.
    visobert_relevance_enabled: bool = True
    visobert_relevance_model_dir: str = "models/visobert-relevance-relabel"
    visobert_relevance_related_min: float = 0.9
    visobert_relevance_not_related_min: float = 0.95
    # Comment Jev lỗi thì thử Kira (tắt mặc định: Kira chỉ còn duyệt BFS).
    jev_kira_fallback: bool = False
    jev_api_key: str | None = None
    jev_api_base_url: str = "https://api.typesafe.ai/v1"
    jev_model: str = "jev-latest"
    jev_concurrency: int = 16
    # Số lời gọi song song tối đa tới MỖI nhà cung cấp AI (Kira, Bee...) trong một tiến trình - xem app/ai/client.py.
    # Service giữ 2; script backfill (scripts/kira_label_comments.py --concurrency) nâng lên trong tiến trình của nó.
    ai_provider_concurrency: int = 2
    # Model Bee RIÊNG cho việc viết report Social Topic (app/ai/tasks/report.py) - mọi lời gọi Bee khác dùng model của dòng
    # "bee" trong ai_providers (Admin › AI). Rỗng = dùng model đó cho cả report. Từ 20/10/2026.
    bee_report_model: str = ""
    # Dưới ngưỡng tự tin: cảm xúc -> neutral, giai đoạn -> khac, khía cạnh -> không gắn.
    jev_min_confidence: float = 0.6
    jev_stage_min_confidence: float = 0.5
    jev_aspect_min_confidence: float = 0.5

    # Kira là bộ phân loại độ liên quan của bài lúc ingest (app/ai/tasks/post_relevance.py):
    # tối đa chừng này bài mỗi ngày UTC. Gom lô nên mỗi bài tốn khoảng 330 token (đo
    # 2026-10-05: ~2.000 token vào + ~435 token ra cho mỗi lô ~7 bài) - cùng tài khoản Kira còn
    # chạy cảm xúc comment (app/ai/tasks/sentiment.py), nên giữ chỗ cho task đó. Vượt hạn mức
    # thì bài chỉ còn quy tắc dự phòng (relevance_rules.has_film_context). 0 là tắt Kira cho bài.
    kira_post_relevance_daily_cap: int = 30000

    # False: ingest consumer KHÔNG hỏi Kira độ liên quan mà lưu bài ngay theo tín hiệu phim/từ khoá (như khi Kira không
    # kết luận); scripts/relabel_post_relevance.py --follow gán lại nhãn Kira ở nền. Dùng khi model Kira chậm (qwen có
    # lúc 60-180 giây một lô) - chờ Kira ngay trong ingest từng làm Kafka tồn hàng chục nghìn message.
    kira_ingest_relevance: bool = True

    # True: lọc bài liên quan hoàn toàn bằng luật, không AI (relevance_rules.mentions_film_manual/no_diacritics_reason):
    # khớp tên phim / viết tắt / từ khoá thì liên quan, phần chữ không có dấu tiếng Việt thì loại thẳng.
    # scripts/relabel_post_relevance.py khi đó tạm dừng phần Kira; --rules áp luật cho bài đang chờ.
    # Bài chỉ nhắc phim qua hashtag nhét cuối caption (relevance_rules.hashtag_only_mention) -> lưu nhưng ẩn. Tắt mặc định
    # tới khi độ chính xác đủ (dry-run 13/10/2026: ~50%).
    hashtag_only_filter: bool = False
    relevance_rules_only: bool = False

    # Giờ (Asia/Ho_Chi_Minh) AI tìm thêm hashtag TikTok cho các phim mỗi ngày (app/services/hashtag_discovery.py) -
    # trước giờ crawl buổi sáng để hashtag mới được cào ngay trong ngày. Rỗng = tắt.
    hashtag_discovery_time: str = "05:30"
    # Kira thêm tối đa chừng này hashtag mỗi phim / mỗi lượt duyệt (variant + hashtag đi kèm từ spider và Omar BFS).
    hashtag_discovery_max_per_movie: int = 5
    hashtag_discovery_max_per_run: int = 40
    # Ngoài lượt buổi sáng, sau mỗi lượt Omar lại duyệt hashtag BFS mới - cách nhau ít nhất chừng này giờ.
    hashtag_discovery_after_omar_hours: float = 3

    # Phim "chặt": tên phim trùng cụm từ thông dụng, nên kể cả bài Kira gán "related" cũng phải có
    # tín hiệu gắn với đúng phim (relevance_rules.film_context_reason: đạo diễn/diễn viên, "phim
    # <tên>", từ điện ảnh sát tên phim...) mới được hiện. Danh sách slug cách nhau bằng dấu phẩy.
    # Không bật cho mọi phim: với tên phim riêng biệt, 20-50% bài thật không có tín hiệu đó.
    strict_relevance_movie_slugs: str = "nguoi-duoc-chon"
    # Phim có tên là cụm từ thông dụng ("cộng, trừ, nhân, chia", "người mẹ xấu xa"): bài "uncertain" KHÔNG được nâng thành
    # liên quan chỉ vì có đủ tên phim (resolve_relevance, quy tắc full_title - 2026-10-16). Khác strict: không đụng bài
    # bộ phân loại đã gán "related".
    ambiguous_title_movie_slugs: str = "nguoi-duoc-chon,cong-tru-nhan-chia,nguoi-me-xau,hoang-hau-cuoi-cung,bo-sua-bay,yeu-sai-yeu-lai"

    # Omar TikTok API (dev.omar-thing.site) - dùng cho crawl video/hashtag thay vì
    # spider-hub. API key được đọc từ bảng omar_api_settings trên Supabase (xem
    # app/services/platform_config_db.py). Base URL có thể ghi đè trong .env khi dev
    # local cần trỏ vào instance khác.
    omar_api_base_url: str = "https://dev.omar-thing.site/api/v1"
    # Lượt cào hashtag TikTok qua Omar API mỗi giờ (app/services/omar_tiktok.py), ở phút này của mỗi giờ (lệch lượt
    # comment ở phút 5). Mỗi hashtag cào tới hết, con trỏ lưu trong Redis. Ngân sách mỗi giờ = hạn mức ngày còn lại (trừ
    # omar_daily_reserve để chừa cho chạy tay) chia đều cho số giờ tới lúc reset, trần omar_requests_per_hour (một giờ ở
    # 10 request/phút làm tối đa ~550). Hashtag đã cào hết được cào lại từ đầu sau omar_recrawl_days ngày.
    # Giới hạn mỗi phút đọc từ GET /usage của gói mỗi lượt (omar_requests_per_minute chỉ là dự phòng khi không đọc được);
    # omar_requests_per_hour = 0 nghĩa là không đặt trần riêng - nâng gói Omar thì tự dùng hết, không cần sửa.
    omar_round_minute: int = 20
    omar_requests_per_hour: int = 0
    omar_requests_per_minute: int = 10
    omar_daily_reserve: int = 50
    # False (mặc định): mỗi lượt dùng hết hạn mức còn lại liên tục; True: rải đều hạn mức ngày theo giờ.
    omar_spread_daily: bool = False
    # Số request Omar chạy song song (mỗi worker một hashtag); nhịp bắt đầu vẫn rải theo giới hạn mỗi phút của gói.
    omar_concurrency: int = 12
    # Video Omar đã gửi lên raw_posts trong chừng này giờ thì không gửi lại (0 = luôn gửi) - xem omar_tiktok._claim_unseen.
    omar_republish_hours: float = 48
    omar_recrawl_days: int = 7
    # Bình luận TikTok qua Omar (app/services/omar_comments.py): mỗi ngày (omar_comments_cadence="daily", từ 20/10/2026 khi
    # gói lên 50.000 request/ngày) hoặc mỗi tuần vào thứ omar_comments_weekday (0 = thứ Hai), lúc omar_comments_time giờ VN,
    # mỗi hashtag lấy omar_comments_top_per_hashtag video đăng trong 7 ngày qua, tương tác cao nhất, cào HẾT bình luận (kèm
    # trả lời nếu omar_comments_replies). Hàng đợi chạy sau phần video mới của mỗi lượt Omar (ưu tiên video). Chạy mỗi ngày thì chỉ lấy video
    # Omar chưa cào, hoặc đã cào nhưng số bình luận tăng từ omar_comments_regrowth_min và từ omar_comments_regrowth_ratio
    # (tỉ lệ) kể từ lần cào - không lật lại từ đầu video không có gì mới.
    omar_comments_cadence: str = "daily"
    omar_comments_regrowth_min: int = 20
    omar_comments_regrowth_ratio: float = 0.3
    omar_comments_weekday: int = 0
    omar_comments_time: str = "01:00"
    omar_comments_top_per_hashtag: int = 100
    omar_comments_replies: bool = True
    # Phần ngân sách mỗi lượt tối đa dành cho hàng đợi bình luận tuần - phần còn lại cho hashtag.
    omar_comments_budget_share: float = 0.5
    # Hashtag không có trên TikTok (trang 1 luôn lỗi 424) được kiểm tra lại sau chừng này giờ.
    omar_missing_recheck_hours: float = 24
    # Tìm theo từ khoá qua Omar (app/services/omar_search.py): mỗi câu (tên phim có dấu / không dấu + câu tìm kiếm
    # Facebook/Threads đang bật của phim) tìm cả video lẫn bài ảnh, tối đa omar_search_pages trang, mỗi ngày một lần.
    omar_search_pages: int = 30
    omar_search_budget_share: float = 0.2
    # Các kiểu tìm video cho mỗi câu, "date:sort" cách nhau bằng dấu phẩy (date: day/week/month/3months/6months, rỗng = mọi
    # lúc; sort: relevance/likes/newest). Đo 13/10/2026 "trại buôn người": week:newest chỉ ra 82 video rồi hết, không lọc ra
    # 207 video - chạy cả hai để vừa bắt video mới vừa lấy rộng.
    omar_search_modes: str = "week:newest,:relevance"
    # Tự sinh câu tìm mỗi ngày từ gợi ý tìm kiếm (search_suggest) và xu hướng VN (trending_searches) của TikTok, tối đa
    # omar_search_suggest_per_movie câu mỗi phim; câu tự sinh omar_search_prune_days lần tìm liền không ra video mới thì xoá.
    omar_search_suggest: bool = True
    omar_search_suggest_per_movie: int = 5
    omar_search_prune_days: int = 5
    # Cào theo kênh (app/services/omar_channels.py): kênh có >= min_posts bài liên quan trong lookback_days ngày, tối đa
    # omar_channels_max kênh, mỗi ngày lật /posts tới video cũ hơn omar_channels_days ngày (tối đa max_pages trang).
    omar_channels_budget_share: float = 0.2
    # Chia ngân sách theo hiệu quả (app/services/omar_yield.py): phần của tìm kiếm / kênh = phần gốc x (bài mới trên mỗi
    # request của nguồn đó / của hashtag) trong omar_adaptive_days ngày, kẹp trong [min, max].
    omar_adaptive_budget: bool = True
    omar_adaptive_days: int = 3
    omar_adaptive_min_share: float = 0.05
    omar_adaptive_max_share: float = 0.45
    omar_channels_min_posts: int = 2
    omar_channels_lookback_days: int = 90
    omar_channels_max: int = 300
    omar_channels_days: int = 30
    omar_channels_max_pages: int = 10
    # Kênh omar_channels_prune_runs lần cào liền không có video về phim nào thì nghỉ omar_channels_rest_days ngày.
    omar_channels_prune_runs: int = 14
    omar_channels_rest_days: int = 7
    # Nhịp gọi khi cào riêng một hashtag (nút "Thêm hashtag" ở tab Omar) - chừa phần còn lại cho lượt đang chạy.
    omar_manual_requests_per_minute: int = 30
    # Hashtag đã cào xong chỉ được cào lại từ đầu bằng ngân sách dư sau chừng này giờ - cào lại mỗi giờ làm ingest tồn
    # hàng chục nghìn bài cập nhật (12/10/2026).
    omar_recrawl_min_hours: float = 24
    # Số request D1 (REST API) đồng thời mỗi tiến trình - xem app/clients/d1.py.
    d1_max_concurrency: int = 8
    # Số liệu tổng video/lượt xem của hashtag (hashtag_info) được làm mới sau chừng này ngày.
    omar_hashtag_info_days: int = 7
    # "Video đang nổi" (app/services/omar_trending.py): mỗi omar_trending_interval_hours giờ đọc lại số liệu của
    # omar_trending_per_movie video TikTok tương tác cao nhất mỗi phim (đăng trong omar_trending_days ngày) qua Omar
    # video_info (1 request/video, tối đa omar_trending_max_requests mỗi lượt) -> bảng D1 omar_video_stats. Video có
    # >= omar_trending_rising_comments_per_hour bình luận/giờ (tối đa omar_trending_rising_limit mỗi lượt, chưa cào bình
    # luận trong omar_trending_comments_recrawl_hours giờ) được xếp vào hàng đợi bình luận Omar.
    # Follower kênh TikTok cho trang "Xuất thông tin theo kênh" (app/services/channel_profiles.py) - mỗi chừng này giờ một lượt.
    channel_profiles_interval_hours: float = 6
    omar_trending_enabled: bool = True
    omar_trending_interval_hours: float = 3
    omar_trending_per_movie: int = 30
    omar_trending_days: int = 7
    omar_trending_max_requests: int = 600
    omar_trending_rising_comments_per_hour: float = 20
    omar_trending_rising_limit: int = 20
    omar_trending_comments_recrawl_hours: float = 24

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

    # Cảnh báo vận hành qua Telegram (app/services/ops_alerts.py), scheduler kiểm tra mỗi ops_alerts_interval_minutes.
    # Mỗi cảnh báo chỉ lặp lại sau ops_alert_repeat_hours; hạn mức / hạn gói Omar / chi phí Jev mỗi ngày một lần.
    ops_alerts_enabled: bool = True
    ops_alerts_interval_minutes: int = 15
    ops_alert_repeat_hours: float = 6
    # Omar: đã dùng >= chừng này % hạn mức ngày; gói hết hạn trong chừng này ngày (và lần nữa khi còn <= 1 ngày).
    ops_omar_quota_alert_pct: float = 90
    # Chỉ báo hạn mức Omar khi còn ít nhất chừng này giờ tới lúc reset (cạn sớm).
    ops_omar_quota_early_hours: float = 6
    ops_omar_expiry_alert_days: int = 7
    # Omar: tỉ lệ request lỗi của lượt đang/vừa chạy (trong 1 giờ qua), chỉ xét khi đủ số lần gọi tối thiểu.
    ops_omar_error_rate_alert: float = 0.3
    ops_omar_error_min_attempts: int = 50
    # Không có bài TikTok / comment mới (MAX(scraped_at) trên D1) trong chừng này giờ.
    ops_post_drought_hours: float = 6
    ops_comment_drought_hours: float = 6
    # Ví AI hết tiền: cảnh báo nếu lỗi 402/hết số dư gần nhất xảy ra trong chừng này giờ.
    ops_ai_wallet_window_hours: float = 1
    # Chi phí Jev hôm nay (giờ VN, ước từ token đầu vào) vượt mức này (USD).
    ops_jev_daily_usd_alert: float = 2.0

    @property
    def ambiguous_title_movies(self) -> frozenset[str]:
        return frozenset(slug.strip() for slug in self.ambiguous_title_movie_slugs.split(",") if slug.strip())

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
