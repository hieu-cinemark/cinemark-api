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
    """Toàn bộ cấu hình của cinemark-api: mỗi thuộc tính là một biến môi trường (hoặc dòng trong `.env` của repo). Chỉ
    đọc qua instance `settings`; thêm biến mới thì khai báo ở đây kèm giá trị mặc định, đừng gọi `os.getenv` rải
    rác."""

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

    # Google News Light (SerpApi), tin sau công chiếu cập nhật mỗi 72 giờ.
    serpapi_api_key: str | None = None
    # Google News cho "Góc nhìn sau công chiếu": "rss" = Google News RSS miễn phí (app/clients/google.py), "serpapi" = như cũ.
    google_news_provider: str = "rss"
    serpapi_timeout_seconds: float = 120.0

    # Cloudflare D1 - cho ingest consumer đọc/ghi database D1 của cinemark-scraper qua
    # HTTP query API của Cloudflare (binding D1 chỉ có bên trong Worker; đây là cách duy
    # nhất để vào từ một tiến trình VPS thường). Xem app/repositories/d1/store.py. Tất cả đều không
    # bắt buộc - thiếu thì các lời gọi D1 bị bỏ qua (không phải lỗi).
    cloudflare_account_id: str | None = None
    cloudflare_api_token: str | None = None
    cloudflare_d1_database_id: str | None = None
    # Hai database D1 khác của Worker cinemark (cùng tài khoản): "app" = cinemark-db (nhãn tác giả, author_ai_*), "config"
    # = cinemark-config-db (settings). Dùng cho các job AI chuyển từ Worker sang (app/services/ai_jobs, 10/11/2026); thiếu
    # thì các job đó tự bỏ qua. Lấy id trong wrangler.toml của repo cinemark.
    cloudflare_d1_app_database_id: str | None = None
    cloudflare_d1_config_database_id: str | None = None
    # Bật các job AI chuyển từ Worker (gắn nhãn tác giả, niche kênh, Review Angles). Tắt = không chạy ở đâu cả.
    ai_jobs_enabled: bool = True
    # Review Angles: số lô AI (24 bài) tối đa mỗi ngày cho mọi phim (phim đang ưu tiên không tính). 16/11/2026: 300 lô không
    # theo kịp - Chị Chị Em Em 3 còn ~15 nghìn bài chưa đọc.
    review_angles_daily_batches: int = 600

    # "local" trỏ d1_query() của app.repositories.d1.store vào một file SQLite local thay vì HTTP
    # query API của Cloudflare - không tốn quota đọc dòng hằng ngày của D1 khi dev/test ở
    # local. Trước đây mặc định trỏ tới bản sao do repo anh em cinemark-be duy trì
    # (scripts/pull-local-db.js) - repo đó không còn trong workspace này, nên bản sao giờ
    # nằm ở đây (xem scripts/tools/pull_local_db.py, bản thay thế của repo này). Làm mới bằng
    # `python -m scripts.tools.pull_local_db` khi dữ liệu local đã cũ (sao chép toàn bộ - mọi
    # bảng, mọi dòng - mới từ D1 thật mỗi lần chạy; chạy lại lúc nào cũng an toàn, nhưng
    # mỗi lần đều tốn quota đọc D1 thật).
    db_mode: str = "remote"  # "remote" | "local" — remote là Cloudflare D1
    local_db_path: str = str(_REPO_ROOT / ".local-db" / "scraper.sqlite")

    # Nơi lưu các bảng cấu hình đã chuyển khỏi Supabase (ai_settings, ai_providers, ai_accuracy_runs, proxy_settings):
    # "postgres" (mặc định, như cũ) hoặc "d1" (database cinemark-config-db, cần CLOUDFLARE_D1_CONFIG_DATABASE_ID). Chỉ đổi sang
    # "d1" sau khi đã chép dữ liệu (python -m scripts.tools.copy_config_to_d1) và đặt cùng giá trị cho spider-hub
    # (CONFIG_DB_BACKEND) - hai bên đọc khác nơi thì cấu hình lệch nhau.
    config_db_backend: str = "postgres"

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

    # Nhà cung cấp AI (base_url, api_key, model) và nhà cung cấp đang chọn nằm ở bảng ai_providers / ai_settings trên Supabase,
    # sửa ở tab AI của dashboard spider-hub - xem app/ai/providers/client.py. Công tắc bật/tắt AI lúc chạy và prompt theo từng
    # task cũng ở tab đó (ai_settings).
    # Model nhà trồng (ViSoBERT fine-tune, app/ai/local/visobert.py) chạy trước Bee cho cảm xúc bình luận và nội dung bài: đủ
    # chắc từ mức này thì lấy luôn, không gọi Bee. Thư mục model (gitignore, 374MB) - thiếu / thiếu torch thì bỏ qua bước này.
    visobert_enabled: bool = True
    visobert_min_confidence: float = 0.7
    visobert_model_dir: str = "models/visobert-cinemark"
    visobert_batch_size: int = 32
    # Bình luận KHÔNG gọi AI (yêu cầu người dùng): luật -> cache -> ViSoBERT gắn nhãn MỌI câu (bỏ ngưỡng
    # visobert_min_confidence), không Jev/Bee, không lượt AI bổ sung khía cạnh/giai đoạn. Cảm xúc nội dung bài
    # (kind="post") vẫn như cũ. Bật lại = True.
    comment_ai_enabled: bool = False
    # Model độ liên quan của bài (app/ai/local/visobert_relevance.py) chạy trước AI: lấy luôn khi đủ chắc theo ngưỡng từng nhãn.
    visobert_relevance_enabled: bool = True
    visobert_relevance_model_dir: str = "models/visobert-relevance-relabel"
    visobert_relevance_related_min: float = 0.9
    visobert_relevance_not_related_min: float = 0.95
    # Số lời gọi song song tối đa tới MỖI nhà cung cấp AI trong một tiến trình - xem app/ai/providers/client.py.
    # Service giữ 2; script backfill (scripts/ml/*.py --concurrency) nâng lên trong tiến trình của nó.
    ai_provider_concurrency: int = 2
    # AI phân loại độ liên quan của bài lúc ingest (app/ai/tasks/post_relevance.py): tối đa chừng này bài mỗi ngày UTC (mỗi bài
    # khoảng 330 token khi gom lô). Vượt hạn mức thì bài chỉ còn quy tắc dự phòng (relevance_rules.has_film_context). 0 = tắt.
    ai_post_relevance_daily_cap: int = 30000

    # False: ingest KHÔNG hỏi AI độ liên quan mà lưu bài ngay theo tín hiệu phim/từ khoá; scripts/jobs/relabel_post_relevance.py
    # --follow gán lại nhãn ở nền. Dùng khi model chậm (từng làm Kafka tồn hàng chục nghìn message).
    # Mặc định False vì AI đang tạm dừng.
    ai_ingest_relevance: bool = False

    # True: lọc bài liên quan hoàn toàn bằng luật, không AI (relevance_rules.mentions_film_manual/no_diacritics_reason):
    # khớp tên phim / viết tắt / từ khoá thì liên quan, phần chữ không có dấu tiếng Việt thì loại thẳng.
    # scripts/jobs/relabel_post_relevance.py khi đó tạm dừng phần AI; --rules áp luật cho bài đang chờ.
    relevance_rules_only: bool = False

    # Lịch crawl Facebook theo hai phần hạn mức (app/services/crawl/crawl_lanes.py, 27/10/2026): mỗi giờ chạy của lịch là một
    # lượt "bài mới" ("Bài viết gần đây", tới 100 trang - spider dừng sau 10 trang liên tiếp không có bài mới), và mỗi giờ
    # một nhịp "đào bài cũ" xếp job (theo số tài khoản, xem bên dưới) trong khung facebook_deep_hours. Tắt -> lịch cũ
    # (lượt đầu quét 4/60 ngày, lượt sau "gần đây" sâu tới max_pages).

    # Số message ingest xử lý song song mỗi topic (app/workers/ingest_consumer/main.py) - xem comment ở đó.
    ingest_post_concurrency: int = 8
    ingest_comment_concurrency: int = 8

    facebook_crawl_lanes: bool = True
    # Tài khoản thử nghiệm: chỉ các khung giờ crawl_schedules khai báo rõ mới được nạp việc cào Facebook
    # có đăng nhập. Cào sâu hằng giờ / làm mới bài theo dõi hằng ngày phải bật riêng.
    facebook_background_crawl_enabled: bool = False
    # Số job đào mỗi giờ = ceil(số tài khoản FB dùng được x facebook_deep_jobs_per_account_hour), trong
    # [1, facebook_deep_jobs_max_per_hour]. Phần "deep" 300 request/tài khoản/ngày, ~10 request/job, khung 17 giờ
    # -> ~1,8 job/tài khoản/giờ. 0 = tắt nhịp đào.
    facebook_deep_jobs_per_account_hour: float = 2.0
    facebook_deep_jobs_max_per_hour: int = 60
    facebook_deep_hours: str = "06-22"
    # Đọc trang nguồn không đăng nhập: hàng đợi vơi là nạp thêm, spider-hub giới hạn số job song song mỗi proxy.
    facebook_public_pages_enabled: bool = True
    facebook_public_pages_min_gap_minutes: int = 60
    # Kênh Threads đọc không đăng nhập (app/services/crawl/threads_channels.py, spider-hub job threads_channel). Tìm kiếm và
    # bình luận Threads vẫn dùng tài khoản.
    threads_channels_enabled: bool = True
    threads_channel_interval_hours: int = 6
    # Tác vụ public (không tài khoản) chạy liên tục khi hàng đợi trống (16/11/2026, người vận hành chọn - lấy nhiều dữ liệu nhất):
    # - kênh Threads: hết kênh tới hạn thì nạp thêm kênh đã đọc lâu nhất, cách lần xếp trước ít nhất chừng này phút
    #   (giống facebook_public_pages_min_gap_minutes của page Facebook);
    # - YouTube: hàng đợi tìm kiếm + bình luận đều trống thì mỗi nhịp scheduler xếp youtube_continuous_batch phim chạy lâu
    #   nhất, mỗi phim cách lần trước ít nhất youtube_continuous_min_gap_hours giờ.
    # Bật/tắt từng loại ở dashboard (proxy_settings: youtube_continuous, threads_channels_continuous).
    threads_channels_min_gap_minutes: int = 60
    youtube_continuous_batch: int = 3
    youtube_continuous_min_gap_hours: float = 3
    # Tự thêm tác giả Threads có nhiều bài liên quan (mỗi ngày, không dùng AI); 0 = tắt.
    threads_channels_auto_add: int = 20

    # Giờ (Asia/Ho_Chi_Minh) AI tìm thêm câu tìm kiếm Facebook/Threads và duyệt trang nguồn mỗi ngày
    # (app/services/discovery/query_discovery.py) - trước giờ crawl buổi sáng. Rỗng = tắt. (Phần AI duyệt hashtag TikTok đã bỏ 17/11/2026.)
    hashtag_discovery_time: str = "05:30"

    # Phim "chặt": tên phim trùng cụm từ thông dụng, nên kể cả bài AI gán "related" cũng phải có
    # tín hiệu gắn với đúng phim (relevance_rules.film_context_reason: đạo diễn/diễn viên, "phim
    # <tên>", từ điện ảnh sát tên phim...) mới được hiện. Danh sách slug cách nhau bằng dấu phẩy.
    # Không bật cho mọi phim: với tên phim riêng biệt, 20-50% bài thật không có tín hiệu đó.
    # thêm cong-tru-nhan-chia - ViSoBERT gán "related" (độ tự tin ~1.0) cho mọi bài có cụm "cộng trừ nhân chia"
    # (bảng cửu chương, sách toán, nhạc chế); 1.245 bài như vậy đã được ẩn tay (danh sách ở ~/cinemark-ml/relabel_ctnc).
    strict_relevance_movie_slugs: str = "nguoi-duoc-chon,cong-tru-nhan-chia"
    # Phim có tên là cụm từ thông dụng ("cộng, trừ, nhân, chia", "người mẹ xấu xa"): bài "uncertain" KHÔNG được nâng thành
    # liên quan chỉ vì có đủ tên phim (resolve_relevance, quy tắc full_title - 2026-10-16). Khác strict: không đụng bài
    # bộ phân loại đã gán "related".
    ambiguous_title_movie_slugs: str = (
        "nguoi-duoc-chon,cong-tru-nhan-chia,nguoi-me-xau,hoang-hau-cuoi-cung,bo-sua-bay,yeu-sai-yeu-lai,"
        "con-mat-thu-ba,chuyen-di-nho-doi,me-min"
    )
    # Phim "chặt" CHỈ trên YouTube (16/11/2026): tìm theo tên phim trên YouTube ra cả video chỉ trùng cụm từ trong tiêu đề
    # ("mở con mắt thứ ba", gameshow "Chuyến Đi Nhớ Đời", truyện cổ tích "mẹ mìn") - mẫu 1 ngày: ~5/6 video Con Mắt Thứ Ba
    # đang hiện là tâm linh. Không bật cho nền tảng khác vì ở đó 20-50% bài thật không có tín hiệu phim (xem strict ở trên).
    youtube_strict_movie_slugs: str = "con-mat-thu-ba,chuyen-di-nho-doi,me-min,nguoi-me-xau"

    # Omar TikTok API (dev.omar-thing.site) - nguồn TikTok của cinemark-api: video theo hashtag + bình luận / trả lời. Client:
    # app/clients/omar.py. API key đọc từ bảng omar_api_settings trên Supabase (nhập ở dashboard > Settings > Omar), không để
    # trong .env. Base URL ghi đè được trong .env khi cần.
    omar_api_base_url: str = "https://dev.omar-thing.site/api/v1"
    # Lượt cào hashtag TikTok mỗi giờ (app/services/omar/omar_tiktok.py), ở phút này của mỗi giờ (lệch lượt comment ở
    # phút 5). Mỗi hashtag cào tới hết, con trỏ lưu trong Redis. Ngân sách: phần còn lại của hạn mức ngày của gói (GET /usage)
    # trừ omar_daily_reserve để chừa cho chạy tay, trần omar_requests_per_hour (0 = không trần riêng). Hashtag đã cào hết
    # được cào lại từ đầu sau omar_recrawl_days ngày (mặc định 1 từ 19/11/2026: mỗi ngày, ưu tiên cùng hashtag đang cào). Giới hạn mỗi phút theo gói (Lite 10, Basic 40, Pro 70, Premium+ 150,
    # Enterprise 500) - đọc từ GET /usage mỗi lượt, omar_requests_per_minute chỉ là dự phòng khi không đọc được; trần thấp
    # hơn đặt ở tab Omar.
    omar_round_minute: int = 20
    omar_requests_per_hour: int = 0
    omar_requests_per_minute: int = 10
    omar_daily_reserve: int = 50
    # Số request Omar chạy song song (mỗi worker một hashtag); nhịp bắt đầu vẫn rải theo giới hạn mỗi phút của gói.
    omar_concurrency: int = 8
    # Video Omar đã gửi lên raw_posts trong chừng này giờ thì không gửi lại (0 = luôn gửi) - xem omar_seen._claim_unseen.
    omar_republish_hours: float = 48
    omar_recrawl_days: int = 1
    # Bình luận TikTok qua Omar (app/services/omar/omar_comments.py): mỗi ngày (omar_comments_cadence="daily") hoặc mỗi tuần
    # vào thứ omar_comments_weekday (0 = thứ Hai), lúc omar_comments_time giờ VN, mỗi hashtag lấy omar_comments_top_per_hashtag
    # video đăng trong 7 ngày qua, tương tác cao nhất, cào HẾT bình luận (kèm trả lời nếu omar_comments_replies). Hàng đợi chạy
    # song song với phần hashtag của mỗi lượt Omar. Chạy mỗi ngày thì chỉ lấy video Omar chưa cào, hoặc đã cào nhưng
    # số bình luận tăng từ omar_comments_regrowth_min và từ omar_comments_regrowth_ratio (tỉ lệ) kể từ lần cào.
    omar_comments_cadence: str = "daily"
    omar_comments_regrowth_min: int = 20
    omar_comments_regrowth_ratio: float = 0.3
    omar_comments_weekday: int = 0
    omar_comments_time: str = "01:00"
    omar_comments_top_per_hashtag: int = 100
    omar_comments_replies: bool = True
    # Phần ngân sách mỗi lượt tối đa dành cho hàng đợi bình luận - phần còn lại cho hashtag.
    omar_comments_budget_share: float = 0.5
    # Hashtag không có trên TikTok (trang 1 luôn lỗi 424) được kiểm tra lại sau chừng này giờ.
    omar_missing_recheck_hours: float = 24
    # Nhịp gọi khi cào riêng một hashtag (nút "Thêm hashtag" ở tab Omar) - chừa phần còn lại cho lượt đang chạy.
    omar_manual_requests_per_minute: int = 5
    # Hashtag đã cào xong chỉ được cào lại từ đầu bằng ngân sách dư sau chừng này giờ - cào lại mỗi giờ làm ingest tồn
    # hàng chục nghìn bài cập nhật.
    omar_recrawl_min_hours: float = 24
    # Số request D1 (REST API) đồng thời mỗi tiến trình - xem app/clients/d1.py.
    d1_max_concurrency: int = 8
    # Số liệu tổng video/lượt xem của hashtag (hashtag_info) được làm mới sau chừng này ngày.
    omar_hashtag_info_days: int = 7
    # Follower kênh TikTok cho trang "Xuất thông tin theo kênh" (app/services/crawl/channel_profiles.py) - mỗi chừng này giờ một lượt.
    channel_profiles_interval_hours: float = 6
    # Quét lại page Facebook của phim đang chiếu cho mục "Đang nổi" (app/services/crawl/watch_pages.py). Mỗi lượt tối đa
    # per_round page, mỗi page max_pages trang bảng tin: ~15 request mỗi lượt, ~6 lượt mỗi ngày ngoài giờ yên lặng của Meta.
    facebook_watch_pages_enabled: bool = True
    facebook_watch_pages_interval_hours: float = 3
    facebook_watch_pages_per_round: int = 15
    facebook_watch_pages_max_pages: int = 5
    facebook_watch_pages_since_days: float = 14

    # Dọn dữ liệu bài không liên quan hằng ngày (app/services/relevance/cleanup.py), do
    # app/services/crawl/scheduler/ chạy lúc irrelevant_post_purge_time (giờ
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

    # Cảnh báo vận hành qua Telegram (app/services/ops/ops_alerts.py), scheduler kiểm tra mỗi ops_alerts_interval_minutes.
    # Mỗi cảnh báo chỉ lặp lại sau ops_alert_repeat_hours; hạn mức / hạn gói Omar / chi phí Jev mỗi ngày một lần.
    ops_alerts_enabled: bool = True
    # Bộ lập lịch (app/services/crawl/scheduler/) + bộ lấy mẫu ops chạy TRONG tiến trình web (mặc định, như trước) hay
    # trong service riêng `python -m app.workers.scheduler.main` (một tác vụ nặng của scheduler không còn làm
    # chậm dashboard). Bật service riêng thì đặt SCHEDULER_IN_WEB=false - không bao giờ chạy cả hai cùng lúc.
    scheduler_in_web: bool = True
    ops_alerts_interval_minutes: int = 15
    ops_alert_repeat_hours: float = 6
    # Omar: đã dùng >= chừng này % hạn mức ngày của gói; gói hết hạn trong chừng này ngày (và lần nữa khi còn <= 1 ngày).
    ops_omar_quota_alert_pct: float = 90
    ops_omar_expiry_alert_days: int = 3
    # Chỉ báo hạn mức Omar khi còn ít nhất chừng này giờ tới lúc reset (cạn sớm).
    ops_omar_quota_early_hours: float = 6
    # Omar: tỉ lệ request lỗi của lượt đang/vừa chạy (trong 1 giờ qua), chỉ xét khi đủ số lần gọi tối thiểu.
    ops_omar_error_rate_alert: float = 0.3
    ops_omar_error_min_attempts: int = 50
    # Không có bài TikTok / comment mới (MAX(scraped_at) trên D1) trong chừng này giờ.
    ops_post_drought_hours: float = 6
    ops_comment_drought_hours: float = 6
    # Ví AI hết tiền: cảnh báo nếu lỗi 402/hết số dư gần nhất xảy ra trong chừng này giờ.
    ops_ai_wallet_window_hours: float = 1
    # Chi phí Jev hôm nay (giờ VN, ước từ token đầu vào) vượt mức này (USD).

    @property
    def ambiguous_title_movies(self) -> frozenset[str]:
        return frozenset(slug.strip() for slug in self.ambiguous_title_movie_slugs.split(",") if slug.strip())

    @property
    def strict_relevance_movies(self) -> frozenset[str]:
        return frozenset(slug.strip() for slug in self.strict_relevance_movie_slugs.split(",") if slug.strip())

    @property
    def youtube_strict_movies(self) -> frozenset[str]:
        return frozenset(slug.strip() for slug in self.youtube_strict_movie_slugs.split(",") if slug.strip())

    def is_strict_movie(self, slug: str | None, platform: str | None) -> bool:
        """Phim "chặt" (cần tín hiệu gắn đúng phim mới hiện): mọi nền tảng, hoặc chỉ YouTube (youtube_strict_movie_slugs)."""
        return slug in self.strict_relevance_movies or (platform == "youtube" and slug in self.youtube_strict_movies)

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
