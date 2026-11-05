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
# Chỗ giữ chỗ nền tảng chèn vào nội dung comment ("[Sticker] Đã zịiii", "[Photo]") - không phải lời người viết.
_PLACEHOLDER = re.compile(r"\[(?:sticker|photo|gif|video|image|ảnh|hình ảnh|nhãn dán|file đính kèm)\]", re.IGNORECASE)
# Chỉ tiếng cười: haha, hihi, kkk, =)), :))) - không nói gì về phim.
_LAUGH = re.compile(r"^(?:(?:ha|he|hi|ho|hu|hj){2,}h*|[hk]{3,}|=+\)+|:+\)+|=+d+|x+d+|lol|wkwk+)[.!~]*$", re.IGNORECASE)
_EMPTY_LITERALS = {"none", "null", "nan", "undefined"}
# Nhãn dán TikTok lưu thành chữ trong ngoặc vuông: "[Cười chảy nước mắt][Ấm lòng]". Cả comment chỉ có chúng thì là icon.
_BRACKET = re.compile(r"\[[^\[\]]{1,30}\]")
_BRACKET_ONLY = re.compile(r"^(?:\s*\[[^\[\]]{1,30}\]\s*)+$")
# Họ phổ biến: tag tên không còn "@" (nền tảng bỏ ký tự @ khi lưu) vẫn nhận ra được qua "<Họ> <Tên viết hoa>...".
_SURNAMES = {
    "nguyễn", "trần", "lê", "phạm", "hoàng", "huỳnh", "phan", "vũ", "võ", "đặng", "bùi", "đỗ", "hồ", "ngô",
    "dương", "lý", "đinh", "đoàn", "lâm", "mai", "trịnh", "trương", "lưu", "cao", "tạ", "hà", "quách", "thái",
    "châu", "tô", "tăng", "vương", "kiều", "la", "văn", "phùng", "triệu", "hứa", "lương", "từ", "chu", "đào",
}
# Bảng tự chỉnh dần. Emoji khóc/cười để trung lập: trên bài phim, 😭 hay là "xúc động", 😂 có thể là chê.
_POSITIVE = set("♥❤😍🥰😘🔥👏👍💯🤩😊🥳💕💖💗💓💞💘🫶❣😻✨🙌😁😄🤗💪🎉")
_NEGATIVE = set("😡🤬😠👎🤮🤢💩😒🙄😞")


def _only_tags(text: str) -> bool:
    """Chỉ tag bạn bè: "@Nguyễn Văn A", "@Lan @Hoa" - tag Facebook là họ tên đầy đủ có dấu cách, nên các
    từ sau @ được tính là tên khi viết hoa chữ đầu. "@Lan đẹp quá" có chữ thường -> không phải."""
    tokens = text.split()
    return bool(tokens) and tokens[0].startswith("@") and all(t.startswith("@") or t[:1].isupper() for t in tokens)


def strip_placeholders(message: str | None) -> str:
    """Nội dung comment bỏ các chỗ giữ chỗ "[Sticker]"/"[Photo]" - đưa cho model phần người viết thật sự gõ."""
    return " ".join(_PLACEHOLDER.sub(" ", message or "").split())


def _only_plain_name_tags(text: str) -> bool:
    """Tag tên không có "@": "Nguyễn Văn A", "Trần Thị B Lê Văn C" - mọi từ viết hoa chữ đầu và từ đầu là họ phổ biến.
    "Thái Hoà khó mời lắm" có chữ thường -> không phải."""
    tokens = [t.strip(".,!?~-") for t in emoji.replace_emoji(text, " ").split()]
    tokens = [t for t in tokens if t]
    return (
        2 <= len(tokens) <= 9
        and tokens[0].lower() in _SURNAMES
        and all(t[:1].isupper() and t.replace("-", "").isalpha() for t in tokens)
    )


def _only_brackets(message: str | None) -> bool:
    return bool(_BRACKET_ONLY.match(emoji.replace_emoji(message or "", "")))


def rule_sentiment(message: str | None) -> str | None:
    """Nhãn không cần model cho comment KHÔNG mang nội dung (rỗng, chỉ sticker/emoji, chỉ tag bạn bè, chỉ tiếng cười,
    quá ngắn); None nghĩa là có chữ, để Kira/ViSoBERT gán."""
    text = _PLACEHOLDER.sub(" ", message or "")
    if _only_brackets(message):
        text = _BRACKET.sub(" ", text)
    if not text.strip() or text.strip().lower() in _EMPTY_LITERALS:
        return "neutral"

    if _only_tags(_URL.sub(" ", text)) or _only_plain_name_tags(_URL.sub(" ", text)):
        return "neutral"
    text = _MENTION.sub(" ", _URL.sub(" ", text))
    letters = []

    for word in emoji.replace_emoji(text, " ").split():
        if _LAUGH.match(word):
            continue
        letters.extend(c for c in word if c.isalpha())

    if letters and len(letters) > 3:
        return None

    if not letters:
        found = [e["emoji"].replace("\ufe0f", "") for e in emoji.emoji_list(text)]
        score = sum((e[0] in _POSITIVE) - (e[0] in _NEGATIVE) for e in found)
        return "positive" if score > 0 else "negative" if score < 0 else "neutral"
    return "neutral"


def with_parent_context(message: str | None, parent: str | None, parent_chars: int = 200) -> str:
    """Chữ đưa cho model: phản hồi đứng một mình thường không rõ nghĩa ("Coi bình thường nha ní"), nên đặt bình luận cha lên trước."""
    text = strip_placeholders(message)
    context = strip_placeholders(parent)[:parent_chars]
    return f"{context}\n{text}" if context else text


def is_tag_or_icon(message: str | None) -> bool:
    """Comment chỉ có tag tên, icon hoặc sticker: người vận hành coi là đang hóng (rủ bạn đi xem, thả cảm xúc), 05/11/2026.
    Comment rỗng, chỉ tiếng cười hay chữ quá ngắn không tính."""
    raw = message or ""
    if _only_brackets(raw):
        return True
    text = _URL.sub(" ", _PLACEHOLDER.sub(" ", raw)).strip()
    if not text:
        return bool(_PLACEHOLDER.search(raw))
    if _only_tags(text) or _only_plain_name_tags(text):
        return True
    without_mentions = _MENTION.sub(" ", text)
    if any(char.isalpha() for char in emoji.replace_emoji(without_mentions, " ")):
        return False
    return bool(_MENTION.search(text) or emoji.emoji_count(text))


CACHE_TTL = 30 * 24 * 3600


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
