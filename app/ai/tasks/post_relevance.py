"""Kira là bộ phân loại độ liên quan của bài viết ở bước ingest. Ingest consumer hỏi
Kira về mọi bài đã qua được các luật cố định (app/services/relevance_rules.py);
việc kiểm tra từ khoá theo chuỗi con chỉ là dự phòng khi Kira không đưa ra kết
luận (đang tắt, vượt giới hạn ngày, lỗi). Kira được biết thông tin của phim đích
(đạo diễn/diễn viên/nhà phát hành/ngày ra rạp) cùng tên các phim khác đang theo
dõi - nhờ vậy mới phân biệt được "bài nói về ĐÚNG phim này" với bài nói về một phim
trùng tên, hay bài nước ngoài dùng chung một hashtag không dấu.

Theo lô: classify_post_relevance_kira() vẫn nhận từng bài, nhưng một bộ gom lô nhỏ
gom các bài mà ingest consumer đang xử lý cùng lúc (tối đa BATCH_SIZE, chờ tối đa
BATCH_WINDOW_S) thành MỘT lời gọi Kira. Mỗi lời gọi một bài khiến ingest chậm hơn
khoảng 0,8 bài/giây so với giới hạn 2 lời gọi đồng thời của Kira, và lặp lại
system prompt cho từng bài.

Prompt: task "post_relevance" (sửa được ở tab AI của Settings trên dashboard;
POST_RELEVANCE_SYSTEM_PROMPT là giá trị mặc định) chứa tiêu chí gán nhãn. Khung
JSON theo lô nằm trong user prompt dựng ở đây, nên system prompt bị sửa cũng không
làm hỏng việc parse.

Kiểm soát chi phí: tuân theo nút bật/tắt Kira trên dashboard (call_kira không có
force) và giới hạn số bài mỗi ngày (settings.kira_post_relevance_daily_cap, đếm
trong Redis). Fail open: bài nào gặp bất kỳ vấn đề gì sẽ nhận None, và bên gọi khi
đó giữ bài lại thay vì loại.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.ai.kira import call_kira, parse_json_response
from app.ai.movie_context import movie_context_block
from app.ai.prompts.post_relevance import POST_RELEVANCE_SYSTEM_PROMPT
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

TASK = "post_relevance"
MAX_CONTENT_CHARS = 1500
BATCH_SIZE = 10
BATCH_WINDOW_S = 1.5
# Số lô được chờ Kira cùng lúc; semaphore của Kira (app/ai/client.py) chạy 2 lô, lô
# thứ ba để sẵn lô kế tiếp.
MAX_BATCHES_IN_FLIGHT = 3
_LABELS = {"relevant": "related", "irrelevant": "not_related", "uncertain": "uncertain"}

Verdict = dict[str, Any]


def movie_block(movie: dict[str, Any]) -> str:
    # Logline ngắn: đủ để nhận ra tên nhân vật/bối cảnh của phim mà không phình prompt của cả lô BATCH_SIZE bài.
    return movie_context_block(movie, logline_chars=250)


@dataclass
class _Job:
    content: str
    movie: dict[str, Any]
    keyword: str | None
    platform: str | None
    other_titles: list[str]
    future: asyncio.Future[Verdict | None] = field(repr=False)


def batch_prompt(jobs: list[_Job]) -> str:
    targets = {job.movie.get("title") for job in jobs}
    others = sorted({title for job in jobs for title in job.other_titles if title not in targets})
    posts = "\n\n".join(
        f"[{index}] {movie_block(job.movie)}\n"
        f"Found via: {job.keyword or job.movie.get('title') or ''} (platform: {job.platform or 'unknown'})\n"
        f"POST:\n{job.content[:MAX_CONTENT_CHARS]}"
        for index, job in enumerate(jobs, 1)
    )
    return (
        f"OTHER TRACKED FILMS (a post about one of these is not about its target): {', '.join(others)}\n\n"
        f"Classify each of the {len(jobs)} numbered posts against ITS OWN target film.\n\n"
        f"{posts}\n\n"
        "Reply with JSON only, no markdown, exactly one entry per post keyed by its number "
        "(this replaces any single-post reply format described earlier):\n"
        '{"results": [{"i": 1, "classification": "relevant" | "irrelevant" | "uncertain", '
        '"score": <0.0-1.0>, "reason": "<at most 15 words>"}]}'
    )


def _verdict(entry: Any) -> Verdict | None:
    if not isinstance(entry, dict) or entry.get("classification") not in _LABELS:
        return None
    try:
        confidence = float(entry.get("score") or 0.0)
    except TypeError, ValueError:
        confidence = 0.0
    return {
        "label": _LABELS[entry["classification"]],
        "confidence": confidence,
        "reason": str(entry.get("reason") or "")[:200],
    }


async def _classify_batch(jobs: list[_Job]) -> list[Verdict | None]:
    started = time.monotonic()
    try:
        response = await call_kira(
            task=TASK,
            system_prompt=POST_RELEVANCE_SYSTEM_PROMPT,
            user_prompt=batch_prompt(jobs),
            max_tokens=300 + 120 * len(jobs),
            platform=jobs[0].platform if len({job.platform for job in jobs}) == 1 else None,
        )
        parsed = parse_json_response(response)
    except Exception as exc:  # noqa: BLE001 - fail open, bên gọi giữ lại các bài
        logger.warning("kira_post_relevance_failed", error=exc, batch_size=len(jobs))
        return [None] * len(jobs)

    results = parsed.get("results") if isinstance(parsed, dict) else None
    verdicts: list[Verdict | None] = [None] * len(jobs)
    for entry in results if isinstance(results, list) else []:
        index = entry.get("i") if isinstance(entry, dict) else None
        if isinstance(index, int) and 1 <= index <= len(jobs):
            verdicts[index - 1] = _verdict(entry)
    missing = verdicts.count(None)
    log = logger.warning if missing else logger.info
    log(
        "kira_post_relevance_batch",
        batch_size=len(jobs),
        missing=missing,
        latency_ms=round((time.monotonic() - started) * 1000),
        **({"reply": str(parsed)[:200]} if missing == len(jobs) else {}),
    )
    return verdicts


class _Batcher:
    """Gom các request đồng thời thành lô. Gắn với event loop đang chạy và được tạo lại
    nếu loop đổi (mỗi lần asyncio.run() trong test/script)."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[_Job] | None = None
        self._collector: asyncio.Task[None] | None = None
        self._in_flight: asyncio.Semaphore | None = None

    def _ensure(self) -> asyncio.Queue[_Job]:
        loop = asyncio.get_running_loop()
        if self._loop is not loop or self._collector is None or self._collector.done():
            self._loop = loop
            self._queue = asyncio.Queue()
            self._in_flight = asyncio.Semaphore(MAX_BATCHES_IN_FLIGHT)
            self._collector = loop.create_task(self._collect(), name="kira-post-relevance-batcher")
        assert self._queue is not None
        return self._queue

    async def submit(self, job: _Job) -> Verdict | None:
        self._ensure().put_nowait(job)
        return await job.future

    async def _collect(self) -> None:
        assert self._queue is not None and self._in_flight is not None
        loop = asyncio.get_running_loop()
        while True:
            batch = [await self._queue.get()]
            deadline = loop.time() + BATCH_WINDOW_S
            while len(batch) < BATCH_SIZE and (remaining := deadline - loop.time()) > 0:
                try:
                    batch.append(await asyncio.wait_for(self._queue.get(), remaining))
                except TimeoutError:
                    break
            await self._in_flight.acquire()
            loop.create_task(self._run(batch)).add_done_callback(lambda _task: self._in_flight.release())  # type: ignore[union-attr]

    @staticmethod
    async def _run(batch: list[_Job]) -> None:
        verdicts: list[Verdict | None] = [None] * len(batch)
        try:
            verdicts = await _classify_batch(batch)
        finally:
            for job, verdict in zip(batch, verdicts, strict=True):
                if not job.future.done():
                    job.future.set_result(verdict)


_batcher = _Batcher()


async def _within_daily_cap() -> bool:
    cap = settings.kira_post_relevance_daily_cap
    if cap <= 0:
        return False
    from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client

    key = f"{REDIS_KEY_PREFIX}kira_post_relevance:{datetime.now(tz=UTC):%Y-%m-%d}"
    try:
        client = get_redis_client()
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, 2 * 24 * 3600)
    except Exception as exc:  # noqa: BLE001 - không có bộ đếm thì không tốn chi phí
        logger.warning("kira_post_relevance_cap_check_failed", error=exc)
        return False
    if count == cap + 1:
        logger.warning("kira_post_relevance_daily_cap_reached", cap=cap)
    return count <= cap


async def classify_post_relevance_kira(
    *,
    content: str | None,
    movie: dict[str, Any],
    keyword: str | None,
    platform: str | None,
    other_titles: list[str],
) -> Verdict | None:
    """{"label": related|not_related|uncertain, "confidence": float, "reason": str},
    hoặc None khi Kira đang tắt, vượt giới hạn ngày, không kết nối được hoặc trả lời
    sai định dạng. Được gom lô chung với các bài khác đang phân loại cùng lúc."""
    if not content or not content.strip() or not movie.get("title"):
        return None
    if not await _within_daily_cap():
        return None
    future: asyncio.Future[Verdict | None] = asyncio.get_running_loop().create_future()
    job = _Job(
        content=content, movie=movie, keyword=keyword, platform=platform, other_titles=other_titles, future=future
    )
    return await _batcher.submit(job)
