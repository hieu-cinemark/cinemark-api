"""Phân loại cảm xúc comment - dùng Kira (xem app/ai/kira.py; Beeknoee chỉ viết
report).

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

from app.ai.kira import call_kira, kira_is_enabled, parse_json_response
from app.ai.prompts.sentiment import SENTIMENT_DATA_PROMPT, SENTIMENT_SYSTEM_PROMPT
from app.ai.tasks.sentiment_rules import cached_labels, remember_labels, rule_sentiment
from app.core.logging import get_logger
from app.services.d1 import MIN_CONTENT_LENGTH

logger = get_logger(__name__)

VALID_SENTIMENTS = {"positive", "negative", "neutral"}
# 10 thay vì 25 (2026-10-06): khi Kira tải cao, một lô 25 comment mất ~170-180s (model suy luận ẩn) và gateway
# của Kira cắt ở ~180s -> 504, mất trắng cả lô. Lô 10 xong khoảng 1 phút; đổi lại tốn thêm token system prompt.
BATCH_SIZE = 10
MAX_MESSAGE_CHARS = 600
MAX_TOKENS = 6000


def _classifiable(message: str | None) -> bool:
    return bool(message) and len(message.strip()) >= MIN_CONTENT_LENGTH


def _user_prompt(messages: list[str]) -> str:
    lines = [f"{i}. {' '.join(m.split())[:MAX_MESSAGE_CHARS]}" for i, m in enumerate(messages, 1)]
    return SENTIMENT_DATA_PROMPT.format(count=len(messages), comments="\n".join(lines))


async def _classify_batch(messages: list[str]) -> list[str | None]:
    try:
        response = await call_kira(
            task="sentiment",
            system_prompt=SENTIMENT_SYSTEM_PROMPT,
            user_prompt=_user_prompt(messages),
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

    labels: list[str | None] = [None] * len(messages)
    for position, entry in enumerate(results, 1):
        if not isinstance(entry, dict):
            continue
        index, sentiment = entry.get("i"), entry.get("sentiment")
        if index is None and len(results) == len(messages):
            # Model bỏ mất số thứ tự nhưng vẫn trả lời đủ mọi comment theo đúng thứ tự - đã gặp
            # thực tế khi system prompt cũ (mỗi lần một comment) còn được lưu.
            index = position
        if isinstance(index, int) and 1 <= index <= len(messages) and sentiment in VALID_SENTIMENTS:
            labels[index - 1] = sentiment
    missing = labels.count(None)
    if missing:
        logger.warning("kira_sentiment_incomplete", missing=missing, batch_size=len(messages))
    return labels


async def classify_sentiments(messages: list[str | None]) -> list[str | None]:
    """Cùng độ dài/thứ tự với `messages`: "positive"/"negative"/"neutral", hoặc None với
    tin nhắn quá ngắn để phân loại, khi Kira đang tắt, hoặc khi lời gọi của lô đó lỗi."""
    labels: list[str | None] = [None] * len(messages)
    todo = [(i, m.strip()) for i, m in enumerate(messages) if m is not None and _classifiable(m)]
    routed: Counter[str] = Counter()

    # 1. Luật (chỉ emoji, quá ngắn, chỉ tag bạn bè) - chạy cả khi Kira tắt.
    rest = []
    for i, m in todo:
        if (label := rule_sentiment(m)) is not None:
            labels[i] = label
            routed["rule"] += 1
        else:
            rest.append((i, m))
    # 2. Nhãn Kira đã gắn cho đúng nội dung này trước đó (Redis).
    if rest:
        for (i, _), hit in zip(rest, await cached_labels([m for _, m in rest]), strict=True):
            if hit in VALID_SENTIMENTS:
                labels[i] = hit
                routed["cache"] += 1
        rest = [(i, m) for i, m in rest if labels[i] is None]

    # 3. Phần còn lại mới gửi Kira, rồi ghi nhãn vào cache cho lần gặp lại.
    if rest and await kira_is_enabled():
        for start in range(0, len(rest), BATCH_SIZE):
            chunk = rest[start : start + BATCH_SIZE]
            for (i, _), label in zip(chunk, await _classify_batch([m for _, m in chunk]), strict=True):
                labels[i] = label
        done = [(m, labels[i]) for i, m in rest if labels[i] is not None]
        await remember_labels(done)
        routed["kira"] = len(done)
        routed["kira_failed"] = len(rest) - len(done)
    if routed:
        logger.info("sentiment_routed", **routed)
    return labels


async def classify_sentiment(message: str | None) -> str | None:
    """Hàm tiện ích phân loại một comment, bọc classify_sentiments()."""
    return (await classify_sentiments([message]))[0]
