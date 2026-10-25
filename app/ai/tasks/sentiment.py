"""Phân loại cảm xúc comment - dùng Kira (xem app/ai/kira.py; Beeknoee chỉ viết
report). Cùng lời gọi đó còn gán khía cạnh được khen/chê và giai đoạn khán giả
(hóng/đã xem) theo bộ cố định trong app/ai/aspects.py (2026-10-07) - thêm vài chục token
đầu ra mỗi comment, không thêm lời gọi nào.

Theo lô: mỗi lời gọi Kira phân loại tối đa BATCH_SIZE comment. Với mỗi comment, chi
phí cố định của một lời gọi (system prompt, độ trễ, một suất đồng thời) lớn hơn hẳn
bản thân comment, nên mỗi lời gọi một comment không theo kịp một ngày crawl bận
(khoảng 22 nghìn comment). Đi qua task "sentiment" của call_llm, nên nút bật/tắt
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
from typing import Any, NotRequired, TypedDict
from zoneinfo import ZoneInfo

from app.ai import visobert
from app.ai.aspects import aspect_prompt_lines, parse_aspects, parse_stage, stage_prompt_lines
from app.ai.decisions import POST, decisions_supported
from app.ai.jev import classify_comments_jev, use_jev
from app.ai.movie_context import movie_context_block
from app.ai.prompts.sentiment import SENTIMENT_DATA_PROMPT, SENTIMENT_SYSTEM_PROMPT
from app.ai.runtime import ai_is_enabled, call_llm, parse_json_response
from app.ai.tasks.sentiment_rules import cached_comment_labels, remember_comment_labels, rule_sentiment
from app.core.config import settings
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
    source: NotRequired[str]  # "rule": gán bằng rule_sentiment (comment không có nội dung) - ghi vào sentiment_source


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
            blocks.append(f"F{index[title]}:\n{movie_context_block(film or {}, logline_chars=200, max_cast=8)}")
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
        response = await call_llm(
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
        logger.warning("llm_sentiment_failed", error=exc, batch_size=len(messages))
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
        logger.warning("llm_sentiment_incomplete", missing=missing, batch_size=len(messages))
    return labels


async def classify_comments(
    messages: list[str | None],
    films: list[dict[str, Any] | None] | None = None,
    *,
    kind: str = "comment",
    home_model: bool = True,
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
            labels[i] = {"sentiment": sentiment, "aspects": [], "stage": "khac", "source": "rule"}
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

    # 3. Model nhà trồng (ViSoBERT, app/ai/visobert.py): câu đủ chắc lấy luôn - chỉ cảm xúc, không khía cạnh/giai đoạn.
    #    Lượt chỉ bổ sung khía cạnh/giai đoạn (home_model=False) bỏ qua bước này. Bình luận khi tắt AI
    #    (settings.comment_ai_enabled=False): ViSoBERT quyết mọi câu, không ngưỡng, và dừng ở đây.
    home_only = kind == "comment" and not settings.comment_ai_enabled
    if rest and home_model:
        predictions = await visobert.predict([m for _, m in rest])
        if predictions is not None:
            min_confidence = 0.0 if home_only else settings.visobert_min_confidence
            for (i, _), (sentiment, confidence) in zip(rest, predictions, strict=True):
                if confidence >= min_confidence:
                    labels[i] = {"sentiment": sentiment, "aspects": [], "stage": "khac", "source": "visobert"}
                    routed["visobert"] += 1
            rest = [(i, m) for i, m in rest if labels[i] is None]
    if home_only:
        if rest:
            routed["unlabelled_no_ai"] = len(rest)  # model không dùng được (thiếu torch/thư mục) - để lượt sau
        if routed:
            logger.info("sentiment_routed", **routed)
        return labels

    # 4. Ca khó -> model quyết định của Bee (settings.classifier = "jev"): bình luận jev-1.14, nội dung bài OpenAI.
    jev_mode = use_jev()
    if rest and jev_mode:
        jev_labels = await classify_comments_jev(
            [m for _, m in rest],
            [films[i] if films else None for i, _ in rest],
            **({"kind": POST} if kind == "post" else {}),
        )
        for (i, _), label in zip(rest, jev_labels, strict=True):
            if label is not None and (parsed := _label(label)) is not None:
                labels[i] = parsed
        done = [(m, label) for i, m in rest if (label := labels[i]) is not None]
        await remember_comment_labels(done)
        routed["jev"] = len(done)
        rest = [(i, m) for i, m in rest if labels[i] is None]

    # 5. Chat Bee: khi không dùng model quyết định, hoặc Jev lỗi và bật settings.jev_kira_fallback. Ghi nhãn vào cache cho lần gặp lại.
    # Cả khi nhà cung cấp không có /decisions (dòng bee tạm trỏ sang Kira): ca khó dùng chat (vd. qwen3.8-flash).
    if rest and (not jev_mode or settings.jev_kira_fallback or not decisions_supported()) and await ai_is_enabled():
        for start in range(0, len(rest), BATCH_SIZE):
            chunk = rest[start : start + BATCH_SIZE]
            chunk_films = [films[i] if films else None for i, _ in chunk]
            for (i, _), label in zip(chunk, await _classify_batch([m for _, m in chunk], chunk_films), strict=True):
                labels[i] = label
        done = [(m, label) for i, m in rest if (label := labels[i]) is not None]
        await remember_comment_labels(done)
        routed["llm"] = len(done)
        routed["llm_failed"] = len(rest) - len(done)
    if routed:
        logger.info("sentiment_routed", **routed)
    return labels


async def classify_sentiments(messages: list[str | None]) -> list[str | None]:
    """Chỉ nhãn cảm xúc của classify_comments()."""
    return [label["sentiment"] if label else None for label in await classify_comments(messages)]
