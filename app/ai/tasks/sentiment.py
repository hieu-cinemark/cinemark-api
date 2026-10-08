"""Phân loại cảm xúc comment - dùng Kira (xem app/ai/kira.py; Beeknoee chỉ viết
report). Cùng lời gọi đó còn gán khía cạnh được khen/chê và giai đoạn khán giả
(hóng/đã xem) theo bộ cố định trong app/ai/aspects.py (2026-10-07) - thêm vài chục token
đầu ra mỗi comment, không thêm lời gọi nào.

Theo lô: mỗi lời gọi Kira phân loại tối đa BATCH_SIZE comment. Với mỗi comment, chi
phí cố định của một lời gọi (system prompt, độ trễ, một suất đồng thời) lớn hơn hẳn
bản thân comment, nên mỗi lời gọi một comment không theo kịp một ngày crawl bận
(khoảng 22 nghìn comment). Đi qua task "sentiment" của call_kira, nên nút bật/tắt
Kira và prompt ghi đè trên dashboard đều có hiệu lực.

Được gọi từ vòng sweep chạy nền của ingest consumer
(app/workers/ingest_consumer/sentiment_sweep.py) và
scripts/backfill_comment_sentiment.py - không bao giờ gọi trực tiếp cho từng
comment.
(Độ liên quan của bài là việc của Kira - app/ai/tasks/post_relevance.py.)

Fail open: lời gọi lỗi thì kết quả của cả lô là None, và comment đơn giản là chưa
được phân loại cho tới lần sweep sau.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, TypedDict
from zoneinfo import ZoneInfo

from app.ai.aspects import aspect_prompt_lines, parse_aspects, parse_stage, stage_prompt_lines
from app.ai.kira import call_kira, kira_is_enabled, parse_json_response
from app.ai.movie_context import movie_context_block
from app.ai.prompts.sentiment import SENTIMENT_DATA_PROMPT, SENTIMENT_SYSTEM_PROMPT
from app.ai.tasks.sentiment_rules import cached_comment_labels, remember_comment_labels, rule_sentiment
from app.core.logging import get_logger
from app.services.d1 import MIN_CONTENT_LENGTH

logger = get_logger(__name__)

# Ngày chiếu trong bảng movies là ngày Việt Nam - so với "hôm nay" theo giờ Việt Nam.
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
VALID_SENTIMENTS = {"positive", "negative", "neutral"}


class CommentLabel(TypedDict):
    sentiment: str
    aspects: list[str]  # "key:+" / "key:-", key trong app.ai.aspects.ASPECTS
    stage: str  # key trong app.ai.aspects.STAGES


# 10 thay vì 25 (2026-10-06): khi Kira tải cao, một lô 25 comment mất ~170-180s (model suy luận ẩn) và gateway
# của Kira cắt ở ~180s -> 504, mất trắng cả lô. Lô 10 xong khoảng 1 phút; đổi lại tốn thêm token system prompt.
BATCH_SIZE = 10
MAX_MESSAGE_CHARS = 600
MAX_TOKENS = 6000


def _classifiable(message: str | None) -> bool:
    return bool(message) and len(message.strip()) >= MIN_CONTENT_LENGTH


def _films_block(films: list[dict[str, Any] | None]) -> tuple[list[str], str]:
    """(nhãn "[F1] " cho từng comment - rỗng khi không biết phim, khối FILMS cho prompt). Mỗi phim chỉ mô tả một lần
    dù nhiều comment trong lô cùng phim."""
    index: dict[str, int] = {}
    blocks: list[str] = []
    tags: list[str] = []
    for film in films:
        title = (film or {}).get("title")
        if not title:
            tags.append("")
            continue
        if title not in index:
            index[title] = len(index) + 1
            blocks.append(f"F{index[title]}:\n{movie_context_block(film, logline_chars=200, max_cast=8)}")
        tags.append(f"[F{index[title]}] ")
    return tags, "\n\n".join(blocks) or "(unknown)"


def _user_prompt(messages: list[str], films: list[dict[str, Any] | None] | None = None) -> str:
    tags, films_text = _films_block(films or [None] * len(messages))
    lines = [
        f"{i}. {tag}{' '.join(m.split())[:MAX_MESSAGE_CHARS]}"
        for i, (m, tag) in enumerate(zip(messages, tags, strict=True), 1)
    ]
    return SENTIMENT_DATA_PROMPT.format(
        count=len(messages),
        comments="\n".join(lines),
        aspects=aspect_prompt_lines(),
        stages=stage_prompt_lines(),
        films=films_text,
        today=datetime.now(tz=_VN).date().isoformat(),
    )


def _label(entry: dict) -> CommentLabel | None:
    sentiment = entry.get("sentiment")
    if sentiment not in VALID_SENTIMENTS:
        return None
    return {
        "sentiment": sentiment,
        "aspects": parse_aspects(entry.get("aspects")),
        "stage": parse_stage(entry.get("stage")),
    }


async def _classify_batch(
    messages: list[str], films: list[dict[str, Any] | None] | None = None
) -> list[CommentLabel | None]:
    try:
        response = await call_kira(
            task="sentiment",
            system_prompt=SENTIMENT_SYSTEM_PROMPT,
            user_prompt=_user_prompt(messages, films),
            # Để dư: model có reasoning tốn token ẩn trước khi ra JSON (một lô 25 comment tốn
            # 1.700 token trong khi đầu ra chỉ khoảng 250 token).
            max_tokens=MAX_TOKENS,
        )
        parsed = parse_json_response(response)
        results = parsed.get("results") if isinstance(parsed, dict) else None
        if not isinstance(results, list):
            raise TypeError(f"unexpected sentiment shape: {str(parsed)[:200]}")
    except Exception as exc:  # noqa: BLE001 - fail open, các comment tạm chưa được phân loại
        logger.warning("kira_sentiment_failed", error=exc, batch_size=len(messages))
        return [None] * len(messages)

    labels: list[CommentLabel | None] = [None] * len(messages)
    for position, entry in enumerate(results, 1):
        if not isinstance(entry, dict):
            continue
        index, label = entry.get("i"), _label(entry)
        if index is None and len(results) == len(messages):
            # Model bỏ mất số thứ tự nhưng vẫn trả lời đủ mọi comment theo đúng thứ tự - đã gặp
            # thực tế khi system prompt cũ (mỗi lần một comment) còn được lưu.
            index = position
        if isinstance(index, int) and 1 <= index <= len(messages) and label is not None:
            labels[index - 1] = label
    missing = labels.count(None)
    if missing:
        logger.warning("kira_sentiment_incomplete", missing=missing, batch_size=len(messages))
    return labels


async def classify_comments(
    messages: list[str | None], films: list[dict[str, Any] | None] | None = None
) -> list[CommentLabel | None]:
    """Cùng độ dài/thứ tự với `messages`: {"sentiment", "aspects", "stage"}, hoặc None với tin nhắn quá ngắn để
    phân loại, khi Kira đang tắt, hoặc khi lời gọi của lô đó lỗi. `films` (cùng độ dài, tuỳ chọn) là dòng movies của
    phim mà mỗi comment nằm dưới - để AI nhận ra diễn viên/nhân vật và biết phim đã ra rạp chưa."""
    labels: list[CommentLabel | None] = [None] * len(messages)
    todo = [(i, m.strip()) for i, m in enumerate(messages) if m is not None and _classifiable(m)]
    routed: Counter[str] = Counter()

    # 1. Luật (chỉ emoji, quá ngắn, chỉ tag bạn bè) - chạy cả khi Kira tắt. Không có nội dung nên không có khía cạnh.
    rest = []
    for i, m in todo:
        if (sentiment := rule_sentiment(m)) is not None:
            labels[i] = {"sentiment": sentiment, "aspects": [], "stage": "khac"}
            routed["rule"] += 1
        else:
            rest.append((i, m))
    # 2. Nhãn Kira đã gắn cho đúng nội dung này trước đó (Redis).
    if rest:
        for (i, _), hit in zip(rest, await cached_comment_labels([m for _, m in rest]), strict=True):
            if hit is not None and (label := _label(hit)) is not None:
                labels[i] = label
                routed["cache"] += 1
        rest = [(i, m) for i, m in rest if labels[i] is None]

    # 3. Phần còn lại mới gửi Kira, rồi ghi nhãn vào cache cho lần gặp lại.
    if rest and await kira_is_enabled():
        for start in range(0, len(rest), BATCH_SIZE):
            chunk = rest[start : start + BATCH_SIZE]
            chunk_films = [films[i] if films else None for i, _ in chunk]
            for (i, _), label in zip(chunk, await _classify_batch([m for _, m in chunk], chunk_films), strict=True):
                labels[i] = label
        done = [(m, label) for i, m in rest if (label := labels[i]) is not None]
        await remember_comment_labels(done)
        routed["kira"] = len(done)
        routed["kira_failed"] = len(rest) - len(done)
    if routed:
        logger.info("sentiment_routed", **routed)
    return labels


async def classify_sentiments(messages: list[str | None]) -> list[str | None]:
    """Chỉ nhãn cảm xúc của classify_comments()."""
    return [label["sentiment"] if label else None for label in await classify_comments(messages)]


async def classify_sentiment(message: str | None) -> str | None:
    """Hàm tiện ích phân loại một comment, bọc classify_sentiments()."""
    return (await classify_sentiments([message]))[0]
