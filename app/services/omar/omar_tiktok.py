"""Cào video TikTok theo hashtag qua Omar API (dev.omar-thing.site, client ở app/clients/omar.py) - chạy LIÊN TỤC khi key đúng
(scheduler/omar.omar_tick mở lượt mỗi giờ / ngay khi hết tạm dừng), song song với spider TikTok của spider-hub. Video đi qua ĐÚNG
đường ingest của spider: publish lên topic raw_posts với key "tiktok:<video_id>" và cùng tên trường TikTokVideoItem
(clients/omar.map_video), nên ingest consumer lọc liên quan, chống trùng, cập nhật tương tác và chọn bài cào comment như mọi
bài TikTok khác.

API key + tổng request mỗi ngày + số request đồng thời đọc từ bảng omar_api_settings trên D1 (nhập ở dashboard > Settings >
Omar). Không có key thì bỏ qua.

Mỗi hashtag được cào TỚI HẾT kết quả (hashtag_videos), con trỏ + id hashtag lưu trong Redis (omar:hashtag_state) - hết ngân sách thì
lượt sau đi tiếp (xem run_omar_round). Ngân sách mỗi lượt = hạn mức ngày còn lại của GÓI (GET /usage), nếu người dùng đặt
tổng request mỗi ngày thấp hơn thì theo số đó; nhịp gọi giữ dưới giới hạn mỗi phút của gói; 429 chờ Retry-After, 424 thử lại,
401/403 (key sai / hết hạn) và hết hạn mức ngày dừng lượt.

hashtag_videos trả response gốc của TikTok nên có đủ tác giả (tick xanh, follower, tài khoản shop, nhãn quảng cáo) - mỗi bài lưu
kèm các tín hiệu đó trong raw_json."""

from __future__ import annotations

import asyncio
import heapq
import itertools
import json
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select

from app.clients.omar import OmarStop, fetch_usage, map_video
from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.jsonutil import loads_or_none
from app.core.logging import get_logger
from app.repositories.d1.keywords import get_enabled_keywords
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import movies
from app.services.accounts.config_db.omar import get_omar_api_settings, omar_limits, omar_requests_cap
from app.services.omar import omar_comments

logger = get_logger(__name__)

from app.services.omar.omar_budget import Budget, Pacer
from app.services.omar.omar_fetch import fetch_hashtag_info, fetch_page, publish_videos
from app.services.omar.omar_hashtag import MAX_PAGE_FAILURES, RESET_KEEP, STATE_KEY, manual_running
from app.services.omar.omar_pause import (
    LAST_ROUND_KEY,
    RESUME_SLACK_SECONDS,
    _round,
    last_round,
    log_stop,
    pause_after_round,
    set_pause,
)
from app.services.omar.omar_seen import count_cooccur

# Trang rỗng liên tiếp mà vẫn has_more: coi như đã hết (tránh lặp vô hạn nếu Omar trả cursor lỗi).
_MAX_EMPTY_PAGES = 3
# Trang rỗng GIỮA CHỪNG (cursor khác "0") thường là Omar/TikTok giới hạn tạm thời - gọi lại sau vài phút lại có data (đo
# 16/10/2026). Để từ khoá đó chờ rồi gọi lại đúng cursor, lần chờ thứ n = n x _EMPTY_RETRY_SECONDS (3, 6, 9 phút), tối
# đa _EMPTY_RETRIES lần; vẫn rỗng thì để đó (giữ cursor, lượt sau đi tiếp) và làm từ khoá khác. Hashtag "để đó" _MAX_STALL_ROUNDS lượt liền thì coi là đã hết.
_EMPTY_RETRY_SECONDS = 180.0
_EMPTY_RETRIES = 3
_MAX_STALL_ROUNDS = 3
# Khoảng tối thiểu giữa hai request của CÙNG một từ khoá - các worker xen kẽ giữa các từ khoá thay vì gọi dồn một từ khoá.
_KEYWORD_GAP_SECONDS = 3.0


async def run_round_robin(
    items: list[Any],
    step: Callable[[Any], Awaitable[float | None]],
    *,
    workers: int,
    should_stop: Callable[[], bool],
) -> None:
    """Chạy `step` xoay vòng trên `items` với tối đa `workers` worker song song: mỗi lần một worker lấy item sẵn sàng sớm
    nhất, gọi step (một trang), step trả về số giây chờ trước lần gọi kế của item đó (xếp lại hàng) hoặc None (item xong).
    Một item chỉ ở một worker tại một thời điểm, nên các request xen kẽ giữa các từ khoá."""
    if not items:
        return
    order = itertools.count()
    heap: list[tuple[float, int, Any]] = [(0.0, next(order), item) for item in items]
    busy = 0

    async def worker() -> None:
        nonlocal busy
        while not should_stop():
            if not heap:
                if busy == 0:
                    return
                await asyncio.sleep(0.5)  # item đang ở worker khác có thể được xếp lại
                continue
            wait = heap[0][0] - time.monotonic()
            if wait > 0:
                await asyncio.sleep(min(wait, 1.0))
                continue
            _, _, item = heapq.heappop(heap)
            busy += 1
            try:
                delay = await step(item)
            finally:
                busy -= 1
            if delay is not None:
                heapq.heappush(heap, (time.monotonic() + delay, next(order), item))

    await asyncio.gather(*(worker() for _ in range(max(1, min(workers, len(items))))))


def plan_budget(day: dict[str, Any], *, daily_cap: int, reserve: int, hour_cap: int) -> tuple[int, int | None]:
    """(ngân sách request của lượt, số giây tới lúc reset ngày hoặc None khi không biết) từ phần `day` của GET /usage.

    Chạy liên tục: dùng hết hạn mức ngày còn lại (trừ `reserve` chừa cho chạy tay) ngay trong lượt - lượt có thể dài nhiều giờ,
    scheduler không mở lượt chồng lên; hết thì chờ reset ngày / key mới. `daily_cap` là tổng request mỗi ngày người dùng đặt
    ở tab Omar (0 = theo gói): thấp hơn hạn mức của gói thì lấy số đó, đã dùng bao nhiêu hôm nay lấy từ chính /usage. Không đọc
    được /usage thì chỉ dùng một phần tư trần mỗi giờ (và không quá `daily_cap`)."""
    if day.get("remaining") is None:
        budget = hour_cap // 4
        return (min(budget, daily_cap) if daily_cap > 0 else budget), None
    remaining, resets_in = int(day["remaining"]), int(day.get("resets_in") or 0)
    if daily_cap > 0 and day.get("used") is not None:
        remaining = min(remaining, max(0, daily_cap - int(day["used"])))
    return max(0, remaining - reserve), resets_in


async def _movie_priority() -> dict[str, float]:
    """movie_id -> số ngày lệch khỏi ngày chiếu (nhỏ = gần ngày chiếu, cào trước)."""
    rows = await d1_exec(select(movies.c.id, movies.c.released_at), quiet=True) or []
    today = datetime.now(tz=UTC).date()
    out: dict[str, float] = {}
    for row in rows:
        try:
            out[row["id"]] = abs((datetime.fromisoformat(str(row["released_at"])[:10]).date() - today).days)
        except TypeError, ValueError:
            out[row["id"]] = 365.0
    return out


_stop_requested = False


def round_status() -> dict[str, Any]:
    return {**_round, "stop_requested": _stop_requested}


async def round_status_full() -> dict[str, Any]:
    """Như round_status, nhưng sau restart (chưa có lượt nào trong tiến trình này) thì lấy lượt gần nhất đã lưu ở Redis."""
    if _round["running"] or _round["started_at"]:
        return round_status()
    saved = await last_round()
    return {**(saved or _round), "running": False, "stop_requested": False}


def is_running() -> bool:
    return bool(_round["running"])


def request_stop() -> bool:
    """Dừng lượt đang chạy sau request hiện tại (con trỏ đã lưu sau mỗi trang). False khi không có lượt nào chạy."""
    global _stop_requested
    if not _round["running"]:
        return False
    _stop_requested = True
    return True


def claim_round(trigger: str) -> bool:
    """Bật cờ đang chạy NGAY khi ra lệnh (đồng bộ, trước khi task kịp chạy) - không thì bấm "Chạy ngay" hai lần, hoặc bấm
    đúng lúc lượt theo lịch vừa kích hoạt, mở hai lượt song song tranh con trỏ và tốn gấp đôi request. False khi đang
    có lượt chạy."""
    global _stop_requested
    if _round["running"]:
        return False
    _stop_requested = False
    _round.update(
        running=True, trigger=trigger, started_at=datetime.now(tz=UTC).isoformat(), finished_at=None, stats={}
    )
    return True


async def run_omar_round_tracked(trigger: str) -> dict[str, Any]:
    """run_omar_round kèm thống kê sống cho dashboard. Bên gọi phải claim_round(trigger) thành công trước."""
    global _stop_requested
    try:
        return await run_omar_round(live=_round["stats"])
    finally:
        _round.update(running=False, finished_at=datetime.now(tz=UTC).isoformat())
        _stop_requested = False
        try:
            await get_redis_client().set(LAST_ROUND_KEY, json.dumps(_round, default=str), ex=30 * 24 * 3600)
        except Exception as exc:  # noqa: BLE001 - chỉ là thông tin hiển thị
            logger.warning("omar_last_round_save_failed", error=exc)


async def run_omar_round(live: dict[str, Any] | None = None) -> dict[str, Any]:
    """Một lượt mỗi giờ: settings.omar_concurrency worker, mỗi worker cào một hashtag TỚI HẾT video rồi nhận hashtag kế
    (hashtag chưa bắt đầu đứng đầu hàng), tới khi hết ngân sách. Con trỏ mỗi hashtag lưu trong
    Redis sau mỗi trang, lượt sau / hôm sau đi tiếp; cào tới has_more = false thì xong, cào lại từ đầu sau
    settings.omar_recrawl_days ngày (mặc định 1: mỗi ngày, cùng hàng ưu tiên với hashtag đang cào, chạy song song với bình
    luận), hoặc sớm hơn khi lượt còn ngân sách dư (hashtag xong lâu nhất trước).

    Ngân sách: CHẠY LIÊN TỤC - toàn bộ hạn mức ngày tự đặt còn lại (omar_daily_requests, trừ settings.omar_daily_reserve) dùng
    ngay trong lượt, hết thì chờ reset / key mới (key đọc lại từ D1 mỗi lượt).
    Nhịp gọi cũng theo giới hạn mỗi phút của gói - nâng gói Omar thì tự chạy nhanh/nhiều hơn."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    stats: dict[str, Any] = live if live is not None else {}
    stats.update(keywords=0, requests=0, videos=0, published=0, skipped_seen=0, failed=0, finished=0)
    if not api_key:
        logger.info("omar_round_skipped_no_key")
        return {**stats, "skipped": "no_key"}
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    if not keywords:
        return {**stats, "skipped": "no_keywords"}

    redis = get_redis_client()
    raw_state = await redis.hgetall(STATE_KEY) or {}
    state: dict[str, dict[str, Any]] = {}
    for key, value in raw_state.items():
        parsed = loads_or_none(value, source="omar_hashtag_state")
        if parsed is not None:
            state[key.decode() if isinstance(key, bytes) else key] = parsed
    now = datetime.now(tz=UTC)
    recrawl_before = (now - timedelta(days=settings.omar_recrawl_days)).isoformat()

    async def save(kw_id: str, entry: dict[str, Any]) -> None:
        state[kw_id] = entry
        try:
            await redis.hset(STATE_KEY, kw_id, json.dumps(entry))
        except Exception as exc:  # noqa: BLE001 - mất con trỏ chỉ làm lần sau cào lại từ đầu
            logger.warning("omar_state_save_failed", error=exc)

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        usage = await fetch_usage(api_key, client)
        # Tổng request mỗi ngày và số request đồng thời người dùng đặt ở tab Omar (0 = theo gói / settings.omar_concurrency).
        limits = await omar_limits()
        daily_cap, workers = limits["daily_requests"], limits["concurrency"] or settings.omar_concurrency
        day = (usage or {}).get("day") or {}
        per_minute = int(((usage or {}).get("minute") or {}).get("limit") or 0) or settings.omar_requests_per_minute
        hour_cap = per_minute * 55
        if settings.omar_requests_per_hour > 0:
            hour_cap = min(hour_cap, settings.omar_requests_per_hour)
        budget, resets_in = plan_budget(
            day, daily_cap=daily_cap, reserve=settings.omar_daily_reserve, hour_cap=hour_cap
        )
        reset_at = datetime.now(tz=UTC) + timedelta(seconds=resets_in) if resets_in is not None else None
        stats["budget"] = budget
        cap = await omar_requests_cap()
        stats["per_minute"] = min(per_minute, cap) if cap > 0 else per_minute
        if budget <= 0:
            logger.info("omar_round_no_budget", remaining=day.get("remaining"))
            if reset_at is not None:
                await set_pause("quota", reset_at + timedelta(seconds=RESUME_SLACK_SECONDS))
            return {**stats, "skipped": "no_budget"}
        pacer = Pacer(per_minute)
        spend = Budget(budget)

        def should_stop() -> bool:
            return _stop_requested or halted

        halted = False
        # Bình luận (app/services/omar/omar_comments.py) chạy SONG SONG với phần hashtag cần cào (mới và đến hạn cào lại),
        # tối đa settings.omar_comments_budget_share ngân sách, và TRƯỚC phần cào lại bằng ngân sách dư (phần đó dùng hết hạn
        # mức còn lại nên đặt sau thì bình luận không bao giờ tới lượt). Trước 12/10/2026 bình luận chạy trước và chặn mọi
        # hashtag; từ 12/10 chạy sau video; từ 19/11/2026 chạy song song để hashtag không phải đợi bình luận.
        comment_budget = Budget(int(budget * settings.omar_comments_budget_share), parent=spend)

        async def run_comments(comment_workers: int) -> None:
            nonlocal halted
            try:
                await omar_comments.process_queue(
                    client, pacer, api_key, comment_budget, stats, should_stop, workers=comment_workers
                )
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    log_stop(exc)
            except Exception as exc:  # noqa: BLE001 - phần bình luận lỗi không chặn phần hashtag
                logger.error("omar_comments_failed", error=exc)

        # 2. Tổng video / lượt xem của từng hashtag (hashtag_info), làm mới mỗi settings.omar_hashtag_info_days ngày.
        info_before = (now - timedelta(days=settings.omar_hashtag_info_days)).isoformat()
        stale_info = [kw for kw in keywords if ((state.get(kw["id"]) or {}).get("info_at") or "") < info_before]

        async def refresh_info(kw: dict[str, Any]) -> None:
            if should_stop() or not spend.take():
                return
            stats["requests"] += 1
            try:
                info = await fetch_hashtag_info(client, pacer, api_key, str(kw["keyword"]).lstrip("#"))
            except OmarStop:
                raise
            except Exception as exc:  # noqa: BLE001 - thử lại sau một ngày (lùi info_at), không phải mỗi lượt
                logger.warning("omar_hashtag_info_failed", hashtag=kw["keyword"], error=exc)
                retry_at = datetime.now(tz=UTC) - timedelta(days=settings.omar_hashtag_info_days - 1)
                await save(kw["id"], {**(state.get(kw["id"]) or {}), "info_at": retry_at.isoformat()})
                return
            await save(kw["id"], {**(state.get(kw["id"]) or {}), **info, "info_at": datetime.now(tz=UTC).isoformat()})

        priority = await _movie_priority()

        missing_before = (now - timedelta(hours=settings.omar_missing_recheck_hours)).isoformat()
        recrawl_gap_before = (now - timedelta(hours=settings.omar_recrawl_min_hours)).isoformat()

        def missing(kw: dict[str, Any]) -> bool:
            return ((state.get(kw["id"]) or {}).get("missing_at") or "") >= missing_before

        def todo(kw: dict[str, Any]) -> bool:
            entry = state.get(kw["id"]) or {}
            return not missing(kw) and (not entry.get("done_at") or entry["done_at"] < recrawl_before)

        # Hashtag chưa cào trang nào trước (hashtag mới thêm), rồi phim gần ngày chiếu trước.
        deep = sorted(
            (kw for kw in keywords if todo(kw)),
            key=lambda kw: (
                int((state.get(kw["id"]) or {}).get("pages") or 0) > 0,
                priority.get(kw["movie_id"], 365.0),
                kw["id"],
            ),
        )
        fresh = sorted(
            (
                kw
                for kw in keywords
                if not todo(kw)
                and not missing(kw)
                and ((state.get(kw["id"]) or {}).get("done_at") or "") < recrawl_gap_before
            ),
            key=lambda kw: (state.get(kw["id"]) or {}).get("done_at") or "",
        )

        async def fetch(
            kw: dict[str, Any], cursor: str, hashtag_id: str | None = None
        ) -> tuple[list[dict[str, Any]], str | None, str | None] | None:
            try:
                videos, next_cursor, hashtag_id = await fetch_page(
                    client, pacer, api_key, str(kw["keyword"]).lstrip("#"), cursor, hashtag_id
                )
            except OmarStop:
                raise
            except Exception as exc:  # noqa: BLE001 - một trang lỗi: để lượt sau thử lại đúng trang đó
                stats["failed"] += 1
                logger.warning("omar_page_failed", hashtag=kw["keyword"], cursor=cursor, error=exc)
                return None
            stats["requests"] += 1
            mapped = [video for video in (map_video(raw) for raw in videos) if video is not None]
            stats["videos"] += len(mapped)
            await count_cooccur(str(kw["keyword"]), kw["id"], mapped)
            await publish_videos(mapped, kw["id"], stats)
            return videos, next_cursor, hashtag_id

        entries: dict[str, dict[str, Any]] = {}
        failures: dict[str, int] = {}
        empty_tries: dict[str, int] = {}

        async def crawl_page(kw: dict[str, Any]) -> float | None:
            """MỘT trang của hashtag `kw` (cào tới hết bằng nhiều lần gọi xen kẽ với hashtag khác - run_round_robin). Trả về
            số giây chờ trước trang kế, hoặc None khi hashtag xong / để đó tới lượt sau / hết ngân sách. Con trỏ lưu sau mỗi
            trang nên lượt sau đi tiếp."""
            nonlocal halted
            kw_id = kw["id"]
            if kw_id in manual_running:
                return None  # đang được cào riêng (crawl_hashtag_now)
            entry = entries.get(kw_id)
            if entry is None:
                entry = state.get(kw_id) or {}
                if entry.get("done_at"):  # đến hạn cào lại từ đầu (giữ số liệu hashtag_info)
                    entry = {k: entry[k] for k in RESET_KEEP if k in entry}
                entry.setdefault("started_at", now.isoformat())
                entries[kw_id] = entry
            if should_stop() or not spend.take():
                return None
            cursor = entry.get("cursor") or "0"
            try:
                result = await fetch(kw, cursor, entry.get("hashtag_id"))
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    log_stop(exc)
                return None
            ever_done = bool((state.get(kw_id) or {}).get("done_at"))  # đã từng cào xong: có thật, chỉ lỗi tạm
            if result is None and cursor == "0" and not entry.get("pages") and not ever_done:
                # Trang 1 lỗi (sau các lần thử lại trong omar_get) ở hashtag chưa từng lấy được trang nào: hashtag không
                # có trên TikTok (Omar trả 424 - biến thể AI tự đặt như #ChiChiEmEm32026). Bỏ qua, kiểm tra lại sau
                # settings.omar_missing_recheck_hours giờ (phim sắp chiếu có thể có hashtag sau).
                entry["missing_at"] = datetime.now(tz=UTC).isoformat()
                stats["missing"] = int(stats.get("missing") or 0) + 1
                await save(kw_id, entry)
                return None
            if result is None:
                # Trang lỗi (sau các lần thử lại trong omar_get): thử lại đúng trang đó sau một lúc, xen các hashtag khác.
                failures[kw_id] = failures.get(kw_id, 0) + 1
                return None if failures[kw_id] >= MAX_PAGE_FAILURES else 30.0 * failures[kw_id]
            failures[kw_id] = 0
            entry.pop("missing_at", None)
            videos, next_cursor, hashtag_id = result
            if hashtag_id:
                entry["hashtag_id"] = hashtag_id
            if not videos and cursor != "0":
                # Rỗng giữa chừng: thường chỉ bị giới hạn tạm thời - chờ vài phút rồi gọi lại đúng cursor.
                empty_tries[kw_id] = empty_tries.get(kw_id, 0) + 1
                if empty_tries[kw_id] <= _EMPTY_RETRIES:
                    stats["empty_retries"] = int(stats.get("empty_retries") or 0) + 1
                    return _EMPTY_RETRY_SECONDS * empty_tries[kw_id]
                if next_cursor is None:
                    # Vẫn rỗng: để đó (giữ cursor, lượt sau gọi lại); _MAX_STALL_ROUNDS lượt liền thì coi là đã hết.
                    entry["stalls"] = int(entry.get("stalls") or 0) + 1
                    if entry["stalls"] < _MAX_STALL_ROUNDS:
                        stats["stalled"] = int(stats.get("stalled") or 0) + 1
                        logger.info(
                            "omar_hashtag_stalled", hashtag=kw["keyword"], cursor=cursor, stalls=entry["stalls"]
                        )
                        await save(kw_id, entry)
                        return None
            empty_tries[kw_id] = 0
            entry["pages"] = int(entry.get("pages") or 0) + 1
            if videos:
                entry.pop("stalls", None)
            empty = 0 if videos else int(entry.get("empty") or 0) + 1
            if next_cursor is None or empty >= _MAX_EMPTY_PAGES:
                entry.update(cursor=None, empty=0, stalls=0, done_at=datetime.now(tz=UTC).isoformat(), fresh_at=None)
                stats["finished"] += 1
                logger.info("omar_hashtag_finished", hashtag=kw["keyword"], pages=entry["pages"])
                await save(kw_id, entry)
                return None
            entry.update(cursor=next_cursor, empty=empty)
            await save(kw_id, entry)
            return _KEYWORD_GAP_SECONDS

        async def pool(job, items: list[tuple[dict[str, Any], int]]) -> list[dict[str, Any]]:
            """Chạy `items` (hashtag, số trang) trên tối đa settings.omar_concurrency worker song song - mỗi hashtag chỉ
            nằm ở một worker. Trả về các hashtag còn trang."""
            nonlocal halted
            queue: asyncio.Queue[tuple[dict[str, Any], int]] = asyncio.Queue()
            for item in items:
                queue.put_nowait(item)
            still: list[dict[str, Any]] = []

            async def worker() -> None:
                nonlocal halted
                while not queue.empty() and not should_stop() and not spend.exhausted:
                    kw, pages = queue.get_nowait()
                    try:
                        if await job(kw, pages):
                            still.append(kw)
                    except OmarStop as exc:
                        if not halted:
                            halted = True
                            stats["stopped"] = str(exc)[:100]
                            log_stop(exc)

            await asyncio.gather(*(worker() for _ in range(max(1, min(settings.omar_concurrency, len(items))))))
            return still

        # settings.omar_concurrency worker xoay vòng giữa các hashtag, mỗi lần một trang (run_round_robin) - không gọi dồn một
        # hashtag; hashtag chưa bắt đầu đứng đầu hàng nên hashtag mới thêm được cào ngay.
        stats["keywords"] = len(deep)
        stats["concurrency"] = workers
        # 19/11/2026: hashtag và bình luận chạy SONG SONG (người vận hành muốn hashtag cào lại mỗi ngày mà không đợi bình luận
        # xong). Số worker chia đôi để tổng request đồng thời vẫn bằng `workers`; nhịp mỗi phút do chung một Pacer nên
        # không vượt giới hạn của gói, bình luận bị chặn ở omar_comments_budget_share nên không nuốt hết ngân sách hashtag.
        comment_workers = max(1, workers // 2)
        hashtag_workers = max(1, workers - comment_workers)
        await asyncio.gather(
            run_round_robin(
                deep,
                crawl_page,
                workers=hashtag_workers,
                should_stop=lambda: should_stop() or spend.exhausted,
            ),
            run_comments(comment_workers),
        )
        # Còn ngân sách: cào lại TỪ ĐẦU các hashtag đã xong lâu nhất (crawl tự đặt lại con trỏ khi có done_at) - bắt video
        # mới và cập nhật tương tác thay vì để hạn mức ngày bỏ phí.
        stats["recrawl_keywords"] = len(fresh)
        if not spend.exhausted and not should_stop():
            entries.clear()  # cào lại từ đầu: crawl_page tự đặt lại con trỏ khi có done_at
            await run_round_robin(
                fresh,
                crawl_page,
                workers=workers,
                should_stop=lambda: should_stop() or spend.exhausted,
            )
        if stale_info and not should_stop():

            async def info_job(kw: dict[str, Any], _pages: int) -> bool:
                await refresh_info(kw)
                return False

            await pool(info_job, [(kw, 1) for kw in stale_info])

    pending = sum(1 for kw in keywords if todo(kw))
    stats["pending_keywords"] = pending
    if not _stop_requested:
        try:
            await pause_after_round(stats, api_key, reset_at, spend.exhausted)
        except Exception as exc:  # noqa: BLE001 - không ghi được thì lượt sau tự kiểm tra hạn mức như trước
            logger.warning("omar_pause_save_failed", error=exc)
    if _stop_requested:
        stats["stopped"] = "manual_stop"
    logger.info("omar_round_finished", total_keywords=len(keywords), **stats)
    return {**stats, "pending_keywords": pending}
