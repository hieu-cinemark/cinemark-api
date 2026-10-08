"""Gán nhãn cảm xúc không cần Kira (bình luận chỉ emoji / quá ngắn / chỉ tag bạn bè) và cache nhãn Kira
theo nội dung - xem classify_comments trong app/ai/tasks/sentiment.py."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import emoji

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger

logger = get_logger(__name__)

_URL = re.compile(r"https?://\S+", re.IGNORECASE)
_MENTION = re.compile(r"@\S+")
# Bảng tự chỉnh dần. Emoji khóc/cười để trung lập: trên bài phim, 😭 hay là "xúc động", 😂 có thể là chê.
_POSITIVE = set("♥❤😍🥰😘🔥👏👍💯🤩😊🥳💕💖💗💓💞💘🫶❣😻✨🙌😁😄🤗💪🎉")
_NEGATIVE = set("😡🤬😠👎🤮🤢💩😒🙄😞")


def _only_tags(text: str) -> bool:
    """Chỉ tag bạn bè: "@Nguyễn Văn A", "@Lan @Hoa" - tag Facebook là họ tên đầy đủ có dấu cách, nên các
    từ sau @ được tính là tên khi viết hoa chữ đầu. "@Lan đẹp quá" có chữ thường -> không phải."""
    tokens = text.split()
    return bool(tokens) and tokens[0].startswith("@") and all(t.startswith("@") or t[:1].isupper() for t in tokens)


def rule_sentiment(message: str) -> str | None:
    if not message:
        return None

    if _only_tags(_URL.sub(" ", message)):
        return "neutral"
    text = _MENTION.sub(" ", _URL.sub(" ", message))
    letters = []

    for c in emoji.replace_emoji(text, " "):
        if c.isalpha():
            letters.append(c)

    if letters and len(letters) > 3:
        return None

    if not letters:
        found = [e["emoji"].replace("\ufe0f", "") for e in emoji.emoji_list(text)]
        score = sum((e[0] in _POSITIVE) - (e[0] in _NEGATIVE) for e in found)
        return "positive" if score > 0 else "negative" if score < 0 else "neutral"
    return "neutral"


CACHE_TTL = 30 * 24 * 3600


def _cache_key(message: str) -> str:
    normalized = " ".join(message.lower().split())
    return f"{REDIS_KEY_PREFIX}sentiment_cache:{hashlib.sha1(normalized.encode()).hexdigest()}"


async def cached_labels(messages: list[str]) -> list[str | None]:
    try:
        values = await get_redis_client().mget([_cache_key(m) for m in messages])
        return [v.decode() if isinstance(v, bytes) else v for v in values]
    except Exception:  # noqa: BLE001 - Redis lỗi thì coi như cache trống
        return [None] * len(messages)


async def remember_labels(pairs: list[tuple[str, str]]) -> None:
    try:
        pipe = get_redis_client().pipeline()
        for message, label in pairs:
            pipe.set(_cache_key(message), label, ex=CACHE_TTL)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001 - không ghi được cache thì lần sau gọi Kira lại, không sao
        logger.warning("sentiment_cache_write_failed", error=exc)


# Nhãn đầy đủ (sentiment + aspects + stage) từ 2026-10-07 - key riêng, vì cache cũ ở trên chỉ có sentiment: dùng nó
# thì comment trùng nội dung sẽ không bao giờ có khía cạnh.
def _label_cache_key(message: str) -> str:
    normalized = " ".join(message.lower().split())
    return f"{REDIS_KEY_PREFIX}comment_label_cache:{hashlib.sha1(normalized.encode()).hexdigest()}"


async def cached_comment_labels(messages: list[str]) -> list[dict[str, Any] | None]:
    try:
        values = await get_redis_client().mget([_label_cache_key(m) for m in messages])
    except Exception:  # noqa: BLE001 - Redis lỗi thì coi như cache trống
        return [None] * len(messages)
    result: list[dict[str, Any] | None] = []
    for value in values:
        try:
            parsed = json.loads(value) if value else None
        except TypeError, ValueError:
            parsed = None
        result.append(parsed if isinstance(parsed, dict) else None)
    return result


async def remember_comment_labels(pairs: list[tuple[str, dict[str, Any]]]) -> None:
    try:
        pipe = get_redis_client().pipeline()
        for message, label in pairs:
            pipe.set(_label_cache_key(message), json.dumps(label, ensure_ascii=False), ex=CACHE_TTL)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001 - không ghi được cache thì lần sau gọi Kira lại, không sao
        logger.warning("comment_label_cache_write_failed", error=exc)
