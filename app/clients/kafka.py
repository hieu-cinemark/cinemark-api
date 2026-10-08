"""Đẩy các yêu cầu kích hoạt crawl lên Kafka - phía producer của
crawl_request_consumer.py bên spider-hub, nơi lắng nghe các message này và khởi chạy
tiến trình `scrapy crawl` tương ứng. Cả các route /<platform>/run theo nền tảng
(app/api/routes/platform_scraper.py, nút "run" bấm tay) lẫn script lập lịch hằng
ngày đều publish qua cùng một hàm, nên ở phía sau không phân biệt được kích hoạt tay
hay theo lịch - một đường code, một hợp đồng.

Giống social_crawler/clients/kafka.py của spider-hub: bắn-rồi-quên, không bao giờ
chặn/làm hỏng request vì Kafka sập - một lần kích hoạt crawl không publish được thì
đơn giản là không chạy, có ghi log, không trả 500."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl_jobs import clear_drain
from app.services.task_queue import enqueue_published

logger = get_logger(__name__)

CRAWL_REQUESTS_TOPIC = "crawl_requests"

# Yêu cầu auto-login có topic riêng thay vì dùng lại crawl_requests - auto_login/consumer.py
# của spider-hub có logic điều phối hoàn toàn khác (một message = một lần thử đăng
# nhập lại bằng Playwright, không phải một tiến trình "scrapy crawl") và thuộc một
# consumer group riêng với cấu hình đồng thời riêng. Dùng lại crawl_requests sẽ bắt
# auto_login/consumer.py phải lọc bỏ luồng crawl ồn ào hơn nhiều chỉ để theo kịp. Xem
# app/services/auto_login.py:publish_auto_login_request cho phía producer.
AUTO_LOGIN_REQUESTS_TOPIC = "auto_login_requests"

_producer: AIOKafkaProducer | None = None

# Mọi bộ (nhãn, topic, consumer group) thực sự được consume ở đâu đó trong hệ thống -
# crawl_request_consumer.py của spider-hub (PLATFORM_CONSUMER_GROUPS, mỗi nền tảng một
# group, cùng đọc topic crawl_requests) và app/workers/ingest_consumer/main.py của
# chính cinemark-api (CONSUMER_GROUP_POSTS/COMMENTS). Khai báo cứng ở đây thay vì suy
# ra, vì đây là nơi duy nhất quan tâm tới mọi consumer trong pipeline, của cả hai repo
# cùng lúc - xem get_consumer_lag() bên dưới.
CONSUMER_GROUPS: tuple[tuple[str, str, str], ...] = (
    ("facebook", CRAWL_REQUESTS_TOPIC, "spider-hub.crawl-requests.facebook"),
    ("threads", CRAWL_REQUESTS_TOPIC, "spider-hub.crawl-requests.threads"),
    ("tiktok", CRAWL_REQUESTS_TOPIC, "spider-hub.crawl-requests.tiktok"),
    ("ingest_posts", "raw_posts", "cinemark-api.ingest.posts"),
    ("ingest_comments", "raw_comments", "cinemark-api.ingest.comments"),
    ("lake_posts", "raw_posts", "cinemark-api.lake"),
    ("lake_comments", "raw_comments", "cinemark-api.lake"),
    # Yêu cầu auto-login có consumer group riêng trong auto_login/consumer.py của
    # spider-hub - mỗi nền tảng một group để hàng đợi đăng nhập lại của Facebook không
    # chặn đầu hàng của Threads (và ngược lại).
    ("auto_login_facebook", AUTO_LOGIN_REQUESTS_TOPIC, "spider-hub.auto-login.facebook"),
    ("auto_login_threads", AUTO_LOGIN_REQUESTS_TOPIC, "spider-hub.auto-login.threads"),
)

# producer.start() chỉ raise KafkaError khi kết nối bị *từ chối* - một broker vẫn
# chạy nhưng không phản hồi (máy quá tải, đang nạp lại/bầu lại coordinator - đã xác
# nhận thực tế 2026-09-21 với container Kafka dev ở local) có thể làm nó treo mà không
# có exception nào. Vì đoạn này chạy đầu tiên trong on_startup của app/main.py, treo
# không giới hạn ở đây sẽ khiến mọi route (kể cả /health) không bao giờ truy cập được,
# cùng kiểu lỗi mà ensure_default_crawl_schedules từng gặp trước khi được xử lý giống
# vậy.
_START_TIMEOUT_SECONDS = 10.0


async def start_kafka_producer() -> None:
    global _producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    try:
        await asyncio.wait_for(producer.start(), timeout=_START_TIMEOUT_SECONDS)
    except KafkaError as exc:
        logger.warning("kafka_unavailable", error=str(exc))
        await producer.stop()
        return
    except TimeoutError:
        logger.warning("kafka_unavailable", error=f"producer.start() did not finish within {_START_TIMEOUT_SECONDS}s")
        asyncio.create_task(producer.stop())
        return
    _producer = producer


async def stop_kafka_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def publish_crawl_request(
    *,
    platform: str,
    keyword: str,
    keyword_id: str,
    max_pages: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    bfs_depth: int | None = None,
) -> bool:
    """Trả về request có thực sự được publish hay không - chỗ gọi tự quyết định báo gì cho
    người dùng nếu Kafka sập (ví dụ vẫn trả 202 vì nhiệm vụ của endpoint kích hoạt chỉ
    là yêu cầu, không bảo đảm giao tới nơi, hoặc hiện một cảnh báo - xem
    app/api/routes/platform_scraper.py). keyword_id là keywords.id của D1 (xem
    app/services/d1.py) - truyền qua nguyên vẹn để message raw_posts mà lượt crawl này
    sinh ra mang một id mà ingest consumer có thể tra ngược thẳng về D1."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", keyword_id=keyword_id)
        return False
    if platform == "tiktok" and not keyword.startswith("#"):
        logger.info("crawl_request_skipped_tiktok_text", keyword=keyword, keyword_id=keyword_id)
        return False
    # Một lần bấm Dừng trước đó bật platform_drain khoảng 15 phút để bỏ qua hàng tồn. Một
    # lần kích hoạt có chủ đích mới phải gỡ cờ đó, nếu không lượt crawl được publish xong
    # sẽ bị log ngay là request_skipped_drain.
    await clear_drain(platform)
    # Cho crawl_request_consumer.py của spider-hub theo dõi đúng tiến trình con này
    # (crawl_job:<platform> trong Redis) để nút Dừng trên dashboard (xem
    # app/services/crawl_jobs.py) có cái để huỷ - xem docstring của module đó để biết đầy
    # đủ cơ chế.
    run_id = str(uuid.uuid4())
    value: dict[str, Any] = {"platform": platform, "keyword": keyword, "keyword_id": keyword_id, "run_id": run_id}
    if max_pages is not None:
        value["max_pages"] = max_pages
    # Khoảng ngày chỉ có ở tìm kiếm Facebook. Threads/TikTok bỏ qua.
    if platform == "facebook":
        if start_date is not None:
            value["start_date"] = start_date.isoformat()
        if end_date is not None:
            value["end_date"] = end_date.isoformat()
    elif start_date is not None or end_date is not None:
        logger.info(
            "crawl_request_dates_dropped",
            platform=platform,
            keyword=keyword,
            keyword_id=keyword_id,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
    if bfs_depth:
        value["bfs_depth"] = int(bfs_depth)
    # Spider hashtag của TikTok hỏi Kira hashtag nào đi cùng là của phim (BFS) - kèm thông tin phim để nó nhận ra tên
    # diễn viên/nhân vật (2026-10-07: "#nadechkugimiya", diễn viên chính Quỷ Ăn Tạng 4, bị chấm "generic").
    if platform == "tiktok" and (movie_context := await _movie_context_for_keyword(keyword_id)):
        value["movie_context"] = movie_context
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{platform}:{keyword_id}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), keyword_id=keyword_id)
        return False
    await enqueue_published(value)
    return True


async def _movie_context_for_keyword(keyword_id: str) -> str | None:
    """Khối thông tin phim (app/ai/movie_context.py) của phim sở hữu từ khoá này, hoặc None khi không tra được -
    thiếu nó thì spider vẫn chạy, chỉ là Kira chấm hashtag kém hơn."""
    from app.ai.movie_context import movie_context_block
    from app.services.d1 import d1_query

    try:
        rows = await d1_query(
            "SELECT m.title, m.director, m.`cast` AS `cast`, m.distributor, m.released_at, m.description "
            "FROM keywords k JOIN movies m ON m.id = k.movie_id WHERE k.id = ?",
            [keyword_id],
            quiet=True,
        )
    except Exception as exc:  # noqa: BLE001 - không chặn việc publish vì thiếu thông tin phim
        logger.warning("movie_context_lookup_failed", keyword_id=keyword_id, error=str(exc))
        return None
    return movie_context_block(rows[0], logline_chars=250) if rows else None


# Mặc định dùng chung cho dashboard/API khi crawl comment. Feed reply gốc của Threads
# trên bài lớn thường cần hơn 40 trang mới hết paging_tokens; Facebook Comet mỗi trang
# khoảng 10 comment nên 10 trang chỉ phủ được khoảng 100 comment cấp một. Chỗ gọi vẫn
# có thể truyền max_pages nhỏ hơn để lấy mẫu nhanh.
DEFAULT_COMMENTS_MAX_PAGES = 80


async def publish_channel_videos_request(
    *, username: str, max_pages: int | None = None, keyword_id: str | None = None
) -> bool:
    """Xếp hàng type=channel_videos cho spider tiktok_channel_videos của TikTok (lưới video
    của một kênh @username). Handle nhập tự do - chưa có bảng "kênh theo dõi" trong D1."""
    handle = username.lstrip("@").strip()
    if not handle:
        return False
    payload: dict[str, Any] = {"username": handle, "run_id": str(uuid.uuid4())}
    if max_pages is not None:
        payload["max_pages"] = max_pages
    if keyword_id:
        payload["keyword_id"] = keyword_id
    return await publish_action_request("tiktok", "channel_videos", payload)


async def publish_comments_crawl_request(
    *,
    platform: str,
    post_external_id: str,
    post_url: str,
    max_pages: int = DEFAULT_COMMENTS_MAX_PAGES,
    bypass_drain: bool = True,
) -> bool:
    """Publish một request type="comments", gắn nhãn để _run_comments_spider trong
    crawl_request_consumer.py (spider-hub) chạy spider comment của nền tảng đó cho đúng
    một bài - chỉ các nền tảng trong COMMENTS_SPIDER_BY_PLATFORM của spider-hub
    (facebook, threads, tiktok - xem docstring của get_comment_mapper ở repo này cho
    registry tương ứng phía cinemark-api) mới thực sự có spider này; publish cho nền
    tảng khác thì phía consumer chỉ ghi log rồi bỏ. Cần cả post_url chứ không chỉ
    post_external_id: spider-hub dựng cache query comment (dùng chung cho mọi bài của
    tài khoản đó) từ một URL bài thật vào lần đầu cache bị thiếu/hết hạn, không phải từ
    một id số trần.

    bypass_drain=True (mặc định) dành cho một lần kích hoạt lẻ từ dashboard - "lấy
    comment cho đúng bài này" - để một lần bấm Dừng không liên quan trước đó ở chỗ khác
    không âm thầm nuốt mất nó (xem docstring của publish_action_request). Những chỗ gọi
    cố ý publish RẤT NHIỀU request loại này cùng lúc - lịch quét comment hằng ngày
    (_trigger_comments_platform trong app/services/scheduler.py) và backfill hàng loạt
    bằng tay (scripts/trigger_recent_keyword_comments.py) - thì truyền
    bypass_drain=False, để chính hàng tồn đó là thứ nút Dừng của nền tảng huỷ được. Đã
    xác nhận thực tế 2026-09-24: khi mọi request comment đều bị gán cứng
    bypass_drain=True, một hàng tồn comment tiktok bị kẹt/chậm (sâu hơn 500) hoàn toàn
    không có cách nào huỷ qua app - Dừng có bật cờ drain nhưng mọi message đang xếp hàng
    đều bỏ qua cờ đó theo thiết kế."""
    run_id = str(uuid.uuid4())
    return await publish_action_request(
        platform,
        "comments",
        {"post_id": post_external_id, "post_url": post_url, "max_pages": max_pages, "run_id": run_id},
        bypass_drain=bypass_drain,
    )


async def publish_action_request(
    platform: str, action: str, payload: dict[str, Any], *, bypass_drain: bool | None = None
) -> bool:
    """Publish một action request chung lên topic crawl_requests, gắn type=action để
    crawl_request_consumer.py xử lý được. Dùng cho kiểm tra tài khoản, refresh token và
    crawl comment.

    bypass_drain=None (mặc định) giữ hành vi ban đầu: True cho mọi action trừ
    refresh_token/cookie_import, để một request lẻ có chủ đích vẫn sống sót qua một lần
    bấm Dừng không liên quan trước đó còn trong TTL thay vì bị âm thầm bỏ qua - còn nếu
    xoá cờ drain ở đây thì sẽ xoá bfs_drain/comments_drain/platform_drain cho TOÀN BỘ
    nền tảng, cũng âm thầm gỡ chặn luôn hàng tồn thật mà lần Dừng muốn giữ lại. Truyền
    bypass_drain rõ ràng (xem docstring của publish_comments_crawl_request) khi chỗ gọi
    publish nhiều request loại này cùng lúc và MUỐN nút Dừng của nền tảng huỷ được chúng
    - xem _handle_request trong crawl_request_consumer.py phía spider-hub để biết chỗ
    thực sự đọc cờ này."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", platform=platform)
        return False
    key = str(uuid.uuid4())
    value: dict[str, Any] = {"type": action, "platform": platform, **payload}
    if action not in ("refresh_token", "cookie_import"):
        value["bypass_drain"] = True if bypass_drain is None else bypass_drain
        # Cho consumer bỏ một message bypass_drain đã nằm trong hàng đợi lúc bấm Dừng
        # (crawl_jobs.request_stop lưu thời điểm bấm) mà vẫn chạy những message được kích hoạt
        # sau đó.
        value["published_at"] = time.time()
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{action}:{platform}:{key}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), platform=platform)
        return False
    await enqueue_published(value)
    return True


async def publish_auto_login_request(platform: str, account_id: str, *, dry_run: bool = False) -> bool:
    """Publish một yêu cầu auto-login lên AUTO_LOGIN_REQUESTS_TOPIC. auto_login/consumer.py
    của spider-hub đọc nó và chạy luồng social_crawler.auto_login cho đúng account_id đó.

    Mỗi tài khoản một message (thay vì gom lô) để Kafka sập giữa lượt chỉ làm hỏng từng
    tài khoản lẻ thay vì cả một nền tảng - và vì mỗi tài khoản đằng nào cũng mở trình
    duyệt riêng, chi phí publish từng tài khoản không đáng kể so với chi phí Playwright.
    Key theo account_id để spider-hub có thể gộp các lần publish lặp lại của cùng một
    tài khoản trong consumer nếu sau này cần.

    Consumer của spider-hub tôn trọng `dry_run=true`: nó chỉ log dòng "lẽ ra đã đăng
    nhập lại tài khoản này" và không ghi gì cả. Giúp người vận hành kiểm tra nhanh danh
    sách ứng viên mà không có lượt đăng nhập thật nào - cùng ý tưởng với
    scripts.relogin_facebook_accounts nhưng điều khiển từ dashboard thay vì shell script.

    Trả về True nếu publish thành công, False nếu Kafka sập - chỗ gọi
    (auto_login.run_auto_login_tick) tự quyết định xử lý False thế nào (tính vào
    kafka_publish_failed, log cảnh báo, lượt chạy vẫn tiếp tục với phần còn lại của nền
    tảng)."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", platform=platform, kind="auto_login")
        return False
    key = f"{platform}:{account_id}"
    value: dict[str, Any] = {
        "type": "relogin",
        "platform": platform,
        "account_id": account_id,
        "dry_run": dry_run,
        "run_id": str(uuid.uuid4()),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await _producer.send_and_wait(AUTO_LOGIN_REQUESTS_TOPIC, key=key, value=value)
    except KafkaError as exc:
        logger.warning("auto_login_request_publish_failed", error=str(exc), platform=platform, account_id=account_id)
        return False
    return True


async def publish_tiktok_identity_reset(account_id: int) -> bool:
    """TikTok không có luồng bootstrap trình duyệt bằng query/mật khẩu để chạy lại như
    Facebook/Threads (xem tiktok/auth/bootstrap.py của spider-hub) - một lần kích hoạt
    "reset cookies" chỉ đích danh một dòng platform_accounts, và device_id/odinId của
    dòng đó được lấy lại bằng một lượt headless mới. Vẫn gắn type="refresh_token" để
    phần điều phối/theo dõi job sẵn có của crawl_request_consumer.py (trạng thái
    "refreshing..."/nút Dừng trên dashboard) chạy luôn, không cần thêm loại action riêng."""
    return await publish_action_request(
        "tiktok", "refresh_token", {"account_id": account_id, "run_id": str(uuid.uuid4())}
    )


async def publish_nurture_request(
    platform: str,
    account: str | None = None,
    *,
    like: bool = True,
    comment: bool = False,
    visits: int = 3,
) -> bool:
    """Xếp hàng type=nurture để crawl_request_consumer của spider-hub chạy
    `python -m social_crawler.nurture_accounts` cho facebook, threads hoặc tiktok. Phần
    làm ấm riêng của TikTok (vào các trang /tag/<hashtag>, không phải feed trang chủ -
    xem docstring module nurture_accounts.py của spider-hub) bỏ qua `comment` và coi
    `visits` là số trang hashtag cần vào; không có tham số riêng cho việc này nên một
    dạng lời gọi phủ được mọi nền tảng mà chỗ gọi không cần biết trường nào áp dụng."""
    payload: dict[str, Any] = {
        "like": like,
        "comment": comment,
        "visits": visits,
        "run_id": str(uuid.uuid4()),
    }
    if account:
        payload["account"] = account
    return await publish_action_request(platform, "nurture", payload)


async def publish_cookie_check_request(platform: str = "facebook", *, stale_hours: float | None = None) -> bool:
    """Xếp hàng type=cookie_check để crawl_request_consumer của spider-hub chạy scripts/check_facebook_cookies.py
    (chỉ tài khoản chưa được kiểm tra trong `stale_hours` giờ). Chung hàng đợi với crawl nên không mở cùng một
    session song song với một lượt crawl. Xem cookie_check_* trong AutoLoginSettings."""
    payload: dict[str, Any] = {"run_id": str(uuid.uuid4())}
    if stale_hours:
        payload["stale_hours"] = stale_hours
    return await publish_action_request(platform, "cookie_check", payload)


async def publish_cookie_import_request(
    platform: str, account_key: str, cookies: str, run_id: str | None = None
) -> str | None:
    """Publish type="cookie_import" để _import_cookies trong crawl_request_consumer.py chạy
    `bootstrap.py --cookies-file ... --account <account_key>` - đúng lệnh mà người vận
    hành lẽ ra phải tự chạy trong terminal để đưa cookie xuất từ một phiên trình duyệt
    thật, không tự động (xem import_cookies trong auth/cookies.py của facebook/threads
    bên spider-hub) - rồi nối thẳng sang một lần refresh token bình thường, nên một lần
    kích hoạt vừa tạo vừa refresh session. Vẫn hoàn toàn do người xác thực: chỉ tự động
    hoá các bước "đưa cookie vào Redis, rồi bắt token", không bao giờ tự đăng nhập - xem
    docstring của app/api/routes/token_refresh.py để biết đầy đủ luồng.

    Trả về run_id được sinh ra (hoặc None nếu bản thân việc publish thất bại) - chỗ gọi
    đưa thẳng vào refresh_tracker.start_refresh để panel log trực tiếp/badge trạng thái
    trên dashboard theo dõi lượt chạy này."""
    run_id = run_id or str(uuid.uuid4())
    ok = await publish_action_request(
        platform, "cookie_import", {"account_key": account_key, "cookies": cookies, "run_id": run_id}
    )
    return run_id if ok else None


async def publish_restore_session_request(platform: str, account_key: str, run_id: str | None = None) -> str | None:
    """Xếp hàng type=refresh_token ghim vào một account_key để spider-hub bắt lại token
    GraphQL từ storage_state trong Redis / cột cookie mà không cần người dán và không
    xoay sang một dòng khác trong pool."""
    run_id = run_id or str(uuid.uuid4())
    ok = await publish_action_request(platform, "refresh_token", {"account_key": account_key, "run_id": run_id})
    return run_id if ok else None


async def get_consumer_lag() -> list[dict[str, Any]]:
    """Số tồn đọng thật cho từng mục trong CONSUMER_GROUPS: offset cuối hiện tại của topic
    (high watermark) trừ đi offset đã *commit* gần nhất của group, cộng dồn qua các
    partition - lấy thẳng từ broker, không phải trạng thái của app.

    Đây cố ý là một con số khác với số "queued" của task_queue (một list Redis mà
    cinemark-api push vào khi publish và spider-hub pop ra khi start_task) - con số đó
    theo dõi sổ sách từng job và có thể bị kẹt mãi nếu một consumer không bao giờ gọi
    được start_task cho một mục nào đó (xem sự cố dẫn tới hàm này: một consumer group
    kafka-python bị kẹt giữa rebalance hàng giờ, để lại khoảng 168 request comment
    tiktok mồ côi trong Redis trong khi lag thật của topic cho group đó là 578 và đang
    tăng). Con số này không thể kẹt kiểu đó - mỗi lần gọi đều tính lại từ broker.

    Chỉ dùng các RPC admin chỉ đọc (list_consumer_group_offsets) và một consumer có
    group_id=None (không bao giờ tham gia group, chỉ hỏi broker high watermark của
    topic) - nên không bao giờ gây rebalance cho bất kỳ consumer group thật nào mà nó
    đang báo cáo."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers, group_id=None)
    try:
        await admin.start()
    except Exception as exc:
        logger.warning("kafka_lag_admin_connect_failed", error=str(exc))
        return [
            {"label": label, "topic": topic, "group_id": group_id, "lag": None, "error": "broker_unreachable"}
            for label, topic, group_id in CONSUMER_GROUPS
        ]
    try:
        await consumer.start()
    except Exception as exc:
        logger.warning("kafka_lag_consumer_connect_failed", error=str(exc))
        await admin.close()
        return [
            {"label": label, "topic": topic, "group_id": group_id, "lag": None, "error": "broker_unreachable"}
            for label, topic, group_id in CONSUMER_GROUPS
        ]

    results: list[dict[str, Any]] = []
    try:
        topics = sorted({topic for _, topic, _ in CONSUMER_GROUPS})
        described = await admin.describe_topics(topics)
        partitions_by_topic: dict[str, list[int]] = {}
        for entry in described:
            name = entry["topic"] if isinstance(entry, dict) else entry.topic
            parts = entry["partitions"] if isinstance(entry, dict) else entry.partitions
            partitions_by_topic[name] = [p["partition"] if isinstance(p, dict) else p.partition for p in parts]

        for label, topic, group_id in CONSUMER_GROUPS:
            partitions = partitions_by_topic.get(topic)
            if not partitions:
                results.append(
                    {"label": label, "topic": topic, "group_id": group_id, "lag": None, "error": "topic_not_found"}
                )
                continue
            try:
                tps = [TopicPartition(topic, p) for p in partitions]
                end_offsets = await consumer.end_offsets(tps)
                committed = await admin.list_consumer_group_offsets(group_id, partitions=tps)
                lag = 0
                for tp in tps:
                    end = end_offsets.get(tp, 0)
                    meta = committed.get(tp)
                    committed_offset = meta.offset if meta and meta.offset >= 0 else 0
                    lag += max(0, end - committed_offset)
                results.append({"label": label, "topic": topic, "group_id": group_id, "lag": lag, "error": None})
            except Exception as exc:
                logger.warning("kafka_lag_query_failed", label=label, topic=topic, group_id=group_id, error=str(exc))
                results.append({"label": label, "topic": topic, "group_id": group_id, "lag": None, "error": str(exc)})
    finally:
        await consumer.stop()
        await admin.close()
    return results


INGEST_DECISIONS_TOPIC = "ingest_decisions"


async def publish_ingest_decision(decision: dict[str, Any]) -> None:
    """Bắn-rồi-quên: mỗi bài mà ingest consumer giữ hoặc loại là một event, được lake
    writer (app/workers/lake_writer/main.py) lưu trữ dưới bronze/entity=decisions/. Đây
    là nguồn sự thật duy nhất cho mọi quyết định loại bài - bảng dropped_posts cũ trên
    D1 đã bị xoá."""
    if _producer is None:
        return
    try:
        await _producer.send(INGEST_DECISIONS_TOPIC, value=decision, key=str(decision.get("post_id") or ""))
    except KafkaError as exc:
        logger.warning("ingest_decision_publish_failed", error=exc)
