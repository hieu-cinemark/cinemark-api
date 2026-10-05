"""Service bộ lập lịch auto-login chạy mỗi giờ.

Cùng dạng với app/services/cleanup.py (cùng dòng setting jsonb singleton + bảng
run_history + helper resolve_settings): PUT /settings/auto-login và POST
/settings/auto-login/run của dashboard đều đi qua đây, và vòng lặp lập lịch trong
tiến trình ở app/services/scheduler.py gọi run_auto_login_tick ở mỗi chu kỳ.

Sao lại cần bộ lập lịch bên trong cinemark-api, khi spider-hub đã có
auto_login/scheduler.py riêng? Ba lý do:

  1. MỘT nguồn sự thật duy nhất cho câu "auto-login có đang bật không?". Dòng
     setting trên dashboard CHÍNH LÀ nguồn đó. Biến env AUTO_LOGIN_ENABLED của
     spider-hub là cổng ban đầu, nhưng env + một daemon riêng nghĩa là công tắc
     "Enable auto-login" trên dashboard chỉ lật một dòng Postgres mà phía spider-hub
     không bao giờ kiểm tra, và cách duy nhất để người vận hành biết "nó có thực sự
     đang bật không?" là SSH vào máy spider-hub rồi `cat /etc/spider-hub.env`. Để
     cinemark-api giữ lịch nghĩa là bật/tắt trên dashboard chắc chắn bắt đầu/dừng
     công việc ở ranh giới lượt kế tiếp, và một lệnh `ps aux | grep cinemark-api` là
     người vận hành biết cái gì đang chạy.

  2. LỊCH SỬ CHẠY cần nằm ở chỗ dashboard đọc được. Đặt nó trong Supabase (bảng
     auto_login_run_history) và logic chạy trong cinemark-api nghĩa là thẻ "last
     run" trên dashboard lấy dữ liệu từ cùng Postgres mà phần settings còn lại vẫn
     dùng - không cần endpoint HTTP mới, không có ranh giới auth, chỉ một câu SELECT.

  3. KAFKA là kênh truyền đúng sang spider-hub. spider-hub là tiến trình sở hữu
     Playwright, dấu vân tay trình duyệt và pool proxy. cinemark-api không import
     patchright (và không nên - nó là web server FastAPI, không phải nơi chạy trình
     duyệt headless), nên ta publish mỗi tài khoản một message Kafka
     auto_login_requests và để auto_login/consumer.py của spider-hub làm việc đăng
     nhập lại thật sự.

Module này phụ trách:

  * resolve_auto_login_settings(): đọc dòng singleton, trộn lên trên mặc định của
    AutoLoginSettings, trả về object đã được kiểm tra đầy đủ.
  * run_auto_login_tick(): một lượt của bộ lập lịch / kích hoạt tay. Đọc settings,
    query Supabase tìm các tài khoản cần đăng nhập lại, publish mỗi tài khoản một
    message Kafka, ghi dòng lịch sử chạy.
  * cờ in_flight: giống purge_in_progress của cleanup.py - dashboard dùng nó để làm
    mờ nút "Run now" khi đang có lượt chạy dở.

Vì sao không viết lại list_accounts_needing_relogin ở đây: query đó nằm trong
social_crawler/db/relogin.py của spider-hub vì điều kiện "cần đăng nhập lại" (cookie
chết + không bị checkpoint + không needs_manual) chính là điều kiện mà phía
spider-hub vốn đã dùng cho bộ lập lịch + consumer của nó. Ta đọc cùng dữ liệu thẳng
từ Supabase - cùng nguồn sự thật, không lặp logic nghiệp vụ."""

from __future__ import annotations

import asyncio
from typing import Any

from app.clients.kafka import publish_auto_login_request
from app.core.logging import get_logger
from app.schemas.settings import (
    AutoLoginRunHistoryEntry,
    AutoLoginSettings,
    AutoLoginSettingsOut,
)
from app.services import platform_config_db as platform_cfg

logger = get_logger(__name__)

# Theo mặc định của schema - giống auto_login/scheduler.py của spider-hub để mặc định
# trên dashboard khớp với những gì một bản deploy mới chỉ dùng env
# AUTO_LOGIN_ENABLED=true sẽ tạo ra.
DEFAULT_INTERVAL_SECONDS = 3600
DEFAULT_PLATFORMS = ("facebook", "threads")


# Theo dõi xem có lượt nào đang chạy không. Nút "Run now" của dashboard đọc cờ này để
# tự làm mờ + hiện "refreshing..." trong lúc có lượt đang chạy - cùng trải nghiệm với
# trường `running: bool` của lịch Cleanup. Đặt ở cấp module + asyncio.Lock vì hai
# endpoint có thể cùng cố kích hoạt (POST bấm tay + lượt của bộ lập lịch chạy cùng lúc
# đúng ranh giới chu kỳ), và ta muốn đúng một trong hai thực sự bắt đầu công việc.
_in_flight = False
_in_flight_lock = asyncio.Lock()


def is_auto_login_in_flight() -> bool:
    """Dùng cho nút "Run now" của dashboard - trả True khi run_auto_login_tick đang chạy dở
    để dashboard khoá nút kích hoạt + hiện spinner. Ngược lại là False (kể cả khoảng
    giữa các lượt của bộ lập lịch)."""
    return _in_flight


async def resolve_auto_login_settings() -> AutoLoginSettings:
    """Đọc dòng singleton từ Supabase, trộn giá trị đã lưu lên trên mặc định của
    AutoLoginSettings (để một dòng có từ trước khi thêm trường mới vẫn nhận giá trị mặc
    định cho trường đó), và kiểm tra kết quả bằng schema. Giống hệt dạng của
    app/services/cleanup.py:resolve_cleanup_settings - cùng kiểu cập nhật một phần +
    trộn lên mặc định, để schema là nguồn sự thật duy nhất về kiểu/ràng buộc của từng
    trường.

    Trường platforms được chuẩn hoá thành list (Postgres lưu dạng chuỗi phân cách bằng
    dấu phẩy để tương thích nhiều DB, schema nhận list) và tập đã sắp xếp theo chữ cái là
    thứ consumer của spider-hub đọc trong key của nó."""
    row = await platform_cfg.get_auto_login_settings()
    stored: dict[str, Any] = row.get("settings") or {}
    defaults = AutoLoginSettings()
    merged: dict[str, Any] = defaults.model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            # Kiểm tra lại bằng schema CHỈ cho key này để một giá trị lưu sai không làm cả lần đọc
            # settings lỗi 500.
            merged[key] = getattr(AutoLoginSettings.model_validate({key: value}), key)
        except Exception:
            logger.warning("auto_login_setting_invalid_stored_value", key=key)
    return AutoLoginSettings(**merged)


async def get_auto_login_settings_out() -> AutoLoginSettingsOut:
    """Tiện ích cho endpoint GET: bọc resolve + thêm các trường metadata mà schema có
    (`updated_at`). Giá trị mặc định lấy từ chính class schema - không cần bản sao riêng."""
    row = await platform_cfg.get_auto_login_settings()
    history = await platform_cfg.list_auto_login_run_history(limit=1)
    return AutoLoginSettingsOut(
        values=await resolve_auto_login_settings(),
        defaults=AutoLoginSettings(),
        running=is_auto_login_in_flight(),
        last_run_at=history[0]["started_at"] if history else None,
        updated_at=row.get("updated_at"),
    )


async def _list_accounts_needing_relogin(platform: str) -> list[dict[str, Any]]:
    """Chép lại câu SELECT trong social_crawler/db/relogin.py của spider-hub
    (list_accounts_needing_relogin). Giữ thành bản chép trực tiếp (không phải module dùng
    chung) vì hai bên kết nối Supabase bằng thư viện driver khác nhau (psycopg đồng bộ
    so với async) và dashboard ở đây không bao giờ cần các cột
    password/totp_secret/cookie/token/email/email_password như phía spider-hub - ta chỉ
    cần account_id (cho payload Kafka) và id (cho phần xem trước "cái gì sẽ chạy" trên
    dashboard).

    Trả về list rỗng khi lỗi DB (có log lỗi) để một trục trặc tạm thời của Supabase
    không làm hỏng cả lượt của bộ lập lịch - cùng kiểu phòng thủ với helper bên
    spider-hub."""
    try:
        from app.services.platform_config_db import _connect

        async with (
            _connect() as conn,
            conn.cursor(row_factory=__import__("psycopg.rows", fromlist=["dict_row"]).dict_row) as cur,
        ):
            await cur.execute(
                """
                SELECT id, account_id, platform, last_check_status, last_checked_at
                FROM platform_accounts
                WHERE platform = %s AND enabled = true
                AND status != 'checkpoint'
                AND last_check_status = 'dead'
                AND needs_manual_login = false
                ORDER BY last_checked_at ASC NULLS LAST, id ASC
                """,
                (platform,),
            )
            rows = await cur.fetchall()
        return list(rows or [])
    except Exception as exc:
        logger.error("auto_login_list_accounts_failed", platform=platform, error=str(exc))
        return []


async def _tick_one_platform(platform: str, *, dry_run: bool) -> tuple[dict[str, int], int, int]:
    """Chạy luồng auto-login cho một nền tảng: query Supabase lấy ứng viên, publish mỗi tài
    khoản một message Kafka, đếm kết quả. Trả về (per_status_counters, kafka_published,
    kafka_publish_failed).

    Các key của bộ đếm trạng thái:
      * attempted - số tài khoản đã thử publish (== len(rows))
      * relogged_in - để dành cho việc báo kết quả theo từng tài khoản của phía
        spider-hub (ở đây không điền - spider-hub sẽ ghi vào auto_login_run_history qua
        một đường cập nhật riêng; module này tạm ghi 0 và để cột `kafka_published` của
        dòng lịch sử báo cho dashboard "ta ĐÃ gửi đi những tài khoản này" so với bản tổng
        kết sau lượt. Chi tiết đầy đủ relogged_in / needs_human / error theo từng tài
        khoản sẽ vào `auto_login_run_history.per_platform` qua webhook của consumer
        spider-hub - xem docstring của nó.)
      * needs_human - để dành giống relogged_in
      * failed - như trên
      * error - như trên"""
    per_status: dict[str, int] = {
        "attempted": 0,
        "relogged_in": 0,
        "needs_human": 0,
        "failed": 0,
        "error": 0,
    }
    kafka_published = 0
    kafka_publish_failed = 0

    rows = await _list_accounts_needing_relogin(platform)
    per_status["attempted"] = len(rows)
    if not rows:
        return per_status, kafka_published, kafka_publish_failed

    logger.info(
        "auto_login_tick_platform_start",
        platform=platform,
        candidate_count=len(rows),
        dry_run=dry_run,
    )

    for row in rows:
        ok = await publish_auto_login_request(
            platform=platform,
            account_id=str(row["account_id"]),
            dry_run=dry_run,
        )
        if ok:
            kafka_published += 1
        else:
            kafka_publish_failed += 1
            # Không bỏ cuộc chỉ vì một lần publish lỗi - log lại và để tài khoản kế tiếp thử. Kafka
            # sập giữa lượt chỉ nên làm mất một phần của một lượt, không phải cả lịch. Bảng lịch sử
            # trên dashboard sẽ hiện số `kafka_publish_failed` để người vận hành phát hiện sự cố
            # một phần sau đó.

    logger.info(
        "auto_login_tick_platform_end",
        platform=platform,
        attempted=per_status["attempted"],
        kafka_published=kafka_published,
        kafka_publish_failed=kafka_publish_failed,
        dry_run=dry_run,
    )
    return per_status, kafka_published, kafka_publish_failed


async def run_auto_login_tick(*, triggered_by: str = "schedule", force: bool = False) -> int:
    """Chạy một lượt của bộ lập lịch auto-login. `triggered_by` là `"schedule"` cho vòng
    lặp lập lịch trong tiến trình và `"manual"` cho nút "Run now" trên dashboard - được
    ghi vào dòng lịch sử để người vận hành phân biệt hai loại về sau. `force=True` bỏ qua
    cổng enabled (dùng cho lần gọi đầu tiên của vòng lặp lập lịch sau khi khởi động, để
    API vừa restart không âm thầm bỏ mất lượt đầu tiên nếu setting trên dashboard được
    lưu trước khi tiến trình kịp khởi động).

    Trả về auto_login_run_history.id của lượt này (hoặc -1 nếu không ghi lượt nào vì
    lượt đó không làm gì do enabled=false)."""
    settings = await resolve_auto_login_settings()
    if not settings.enabled and not force:
        logger.info(
            "auto_login_tick_skipped_disabled",
            triggered_by=triggered_by,
        )
        return -1

    async with _in_flight_lock:
        global _in_flight
        if _in_flight:
            # Hai lần kích hoạt cùng lúc (POST bấm tay + lượt của bộ lập lịch chạy đúng cùng thời
            # điểm). Lượt của bộ lập lịch lặng lẽ thua ở đây - lượt bấm tay đã bắt đầu, và nút
            # "Run now" bị làm mờ trên dashboard là đủ để người dùng hiểu.
            logger.warning("auto_login_tick_already_in_flight", triggered_by=triggered_by)
            return -1
        _in_flight = True

    dry_run = settings.dry_run
    platforms = list(settings.platforms) or list(DEFAULT_PLATFORMS)
    interval_seconds = settings.interval_seconds or DEFAULT_INTERVAL_SECONDS

    run_id = -1
    error: str | None = None
    per_platform: dict[str, dict[str, int]] = {}
    kafka_published_total = 0
    kafka_publish_failed_total = 0
    try:
        run_id = await platform_cfg.record_auto_login_run_start(
            triggered_by=triggered_by,
            dry_run=dry_run,
            interval_seconds=interval_seconds,
            platforms=platforms,
        )
        logger.info(
            "auto_login_tick_start",
            run_id=run_id,
            triggered_by=triggered_by,
            dry_run=dry_run,
            platforms=platforms,
            interval_seconds=interval_seconds,
        )
        for platform in platforms:
            try:
                per_status, kafka_published, kafka_publish_failed = await _tick_one_platform(platform, dry_run=dry_run)
                per_platform[platform] = per_status
                kafka_published_total += kafka_published
                kafka_publish_failed_total += kafka_publish_failed
            except Exception as exc:
                # Một nền tảng lỗi không được làm chết cả lượt (cùng kiểu phòng thủ với try/except
                # quanh purge_irrelevant_posts trong lượt cleanup). Log lỗi dưới key của nền tảng để
                # dashboard hiện "facebook: <lỗi>".
                logger.exception(
                    "auto_login_tick_platform_crashed",
                    platform=platform,
                    run_id=run_id,
                )
                per_platform[platform] = {
                    "attempted": 0,
                    "relogged_in": 0,
                    "needs_human": 0,
                    "failed": 0,
                    "error": 1,
                    "exception": str(exc),
                }
        logger.info(
            "auto_login_tick_end",
            run_id=run_id,
            triggered_by=triggered_by,
            total_attempted=sum(p.get("attempted", 0) for p in per_platform.values()),
            kafka_published=kafka_published_total,
            kafka_publish_failed=kafka_publish_failed_total,
            per_platform=per_platform,
        )
    except Exception as exc:
        logger.exception("auto_login_tick_crashed")
        error = str(exc)
    finally:
        if run_id > 0:
            try:
                await platform_cfg.record_auto_login_run_finish(
                    run_id,
                    per_platform=per_platform,
                    kafka_published=kafka_published_total,
                    kafka_publish_failed=kafka_publish_failed_total,
                    error=error,
                )
            except Exception:
                logger.exception("auto_login_history_persist_failed", run_id=run_id)
        async with _in_flight_lock:
            _in_flight = False

    return run_id


async def list_auto_login_history(*, limit: int = 20) -> list[AutoLoginRunHistoryEntry]:
    """Đọc danh sách auto_login_run_history (mới nhất trước) và chuyển mỗi dòng thành
    schema pydantic mà dashboard dùng. Cùng dạng mà cleanup.py:list_cleanup_history trả
    ra - cả hai đều bọc platform_config_db.list_*_run_history và chuyển sang schema thân
    thiện với dashboard."""
    rows = await platform_cfg.list_auto_login_run_history(limit=limit)
    out: list[AutoLoginRunHistoryEntry] = []
    for row in rows:
        out.append(
            AutoLoginRunHistoryEntry(
                started_at=row["started_at"],
                finished_at=row.get("finished_at"),
                triggered_by=row.get("triggered_by") or "schedule",
                dry_run=bool(row.get("dry_run")),
                interval_seconds=int(row.get("interval_seconds") or DEFAULT_INTERVAL_SECONDS),
                platforms=[p.strip() for p in (row.get("platforms") or "").split(",") if p.strip()],
                per_platform=row.get("per_platform") or {},
                total_attempted=int(row.get("total_attempted") or 0),
                total_relogged_in=int(row.get("total_relogged_in") or 0),
                total_needs_human=int(row.get("total_needs_human") or 0),
                total_failed=int(row.get("total_failed") or 0),
                total_error=int(row.get("total_error") or 0),
                kafka_published=int(row.get("kafka_published") or 0),
                kafka_publish_failed=int(row.get("kafka_publish_failed") or 0),
                error=row.get("error"),
            )
        )
    return out
