"""Gán cảm xúc, khía cạnh và giai đoạn khán giả cho bình luận.

Thứ tự xử lý mỗi bình luận: luật (emoji, quá ngắn, chỉ tag) -> cache nhãn theo nội dung -> model tại chỗ (ViSoBERT) ->
model quyết định -> chat LLM theo lô. Bước sau chỉ chạy cho phần bước trước chưa kết luận.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, NotRequired, TypedDict
from zoneinfo import ZoneInfo

from app.ai.domain.aspects import aspect_prompt_lines, parse_aspects, parse_stage, stage_prompt_lines
from app.ai.domain.cinema_visit import cinema_visit_aspect
from app.ai.domain.movie_context import movie_context_block
from app.ai.local import visobert
from app.ai.prompts.sentiment import SENTIMENT_DATA_PROMPT, SENTIMENT_SYSTEM_PROMPT
from app.ai.providers.decisions import POST, decisions_supported
from app.ai.providers.jev import classify_comments_jev, use_jev
from app.ai.runtime import ai_is_enabled, call_llm, parse_json_response
from app.ai.tasks.sentiment_rules import cached_comment_labels, is_tag_or_icon, remember_comment_labels, rule_sentiment
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import MIN_CONTENT_LENGTH

logger = get_logger(__name__)

# Ngày chiếu trong bảng movies là ngày Việt Nam - so với "hôm nay" theo giờ Việt Nam.
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
VALID_SENTIMENTS = {"positive", "negative", "neutral"}


class CommentLabel(TypedDict):
    sentiment: str
    aspects: list[str]  # "key:+" / "key:-", thêm "di_rap:0" cho chuyện đi rạp trung tính
    stage: str  # key trong app.ai.domain.aspects.STAGES
    source: NotRequired[str]  # "rule": gán bằng rule_sentiment (comment không có nội dung) - ghi vào sentiment_source


# Lô nhỏ để mỗi lời gọi xong trong ~1 phút, không bị gateway cắt (lô 25 hay quá 180 giây).
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
            # Để dư cho reasoning token ẩn trước khi ra JSON.
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
            # Model bỏ số thứ tự nhưng vẫn trả đủ, đúng thứ tự: chấp nhận.
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

    # 1. Luật (chỉ emoji, quá ngắn, chỉ tag bạn bè) - chạy cả khi AI tắt; không có khía cạnh.
    rest = []
    for i, m in todo:
        if (sentiment := rule_sentiment(m)) is not None:
            stage = "hong" if is_tag_or_icon(m) else "khac"
            labels[i] = {"sentiment": sentiment, "aspects": [], "stage": stage, "source": "rule"}
            routed["rule"] += 1
        else:
            rest.append((i, m))
    # 2. Nhãn AI đã gắn cho đúng nội dung này trước đó (cache Redis).
    if rest:
        for (i, _), hit in zip(rest, await cached_comment_labels([m for _, m in rest]), strict=True):
            if hit is not None and (label := _label(hit)) is not None:
                labels[i] = label
                routed["cache"] += 1
        rest = [(i, m) for i, m in rest if labels[i] is None]

    # 3. Model nhà trồng (ViSoBERT, app/ai/local/visobert.py): câu đủ chắc lấy luôn - chỉ cảm xúc, không khía cạnh/giai đoạn.
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
        return _with_cinema_visits(messages, labels, kind)

    # 4. Ca khó -> model quyết định (settings.classifier = "jev").
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

    # 5. Chat LLM: khi không dùng model quyết định, provider không có /decisions, hoặc Jev lỗi và bật jev_kira_fallback.
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
    return _with_cinema_visits(messages, labels, kind)


def _with_cinema_visits(
    messages: list[str | None], labels: list[CommentLabel | None], kind: str
) -> list[CommentLabel | None]:
    if kind != "comment":
        return labels
    for message, label in zip(messages, labels, strict=True):
        if not message or label is None or label.get("source") == "rule":
            continue
        # Ưu tiên nhãn theo ngữ cảnh của AI/cache; chỉ bổ sung khi còn thiếu chủ đề này.
        if not any(tag.startswith("di_rap:") for tag in label["aspects"]) and (tag := cinema_visit_aspect(message)):
            label["aspects"] = [*label["aspects"], tag]
    return labels


async def classify_sentiments(messages: list[str | None]) -> list[str | None]:
    """Chỉ nhãn cảm xúc của classify_comments()."""
    return [label["sentiment"] if label else None for label in await classify_comments(messages)]
