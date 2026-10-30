"""Làm sạch comment trước khi tính embedding: bỏ những thứ khiến máy gom theo HÌNH THỨC (link, tag người, sticker,
emoji, từ cười) thay vì NỘI DUNG. Probe 26/10/2026: sau làm sạch + bỏ câu < 30 ký tự còn ~50% comment, và tỉ lệ cụm
rõ nghĩa tăng từ 40% lên 55%."""

from __future__ import annotations

import re

URL_RE = re.compile(r"https?://\S+|www\.\S+")
MENTION_RE = re.compile(r"@\S+")
TAG_RE = re.compile(r"\[[^\]]{1,40}\]")  # [Sticker], [Photo], [Cười chảy nước mắt]...
EMOJI_RE = re.compile(
    "[\U0001f000-\U0001faff\U0001d400-\U0001d7ff\u2190-\u21ff\u2300-\u27bf\u2b00-\u2bff\ufe0f\u200d\u2764♡♥]+"
)
LAUGH_RE = re.compile(
    r"(?<!\w)(?:k{2,}|ha(?:ha)+|hi(?:hi)+|he(?:he)+|hu(?:hu)+|kk+)(?!\w)|[=:;]+[)(dDpP]+|\)\)+|\(\(+", re.IGNORECASE
)
MIN_CHARS = 30


def normalize(text: str) -> str:
    for pattern in (URL_RE, MENTION_RE, TAG_RE, EMOJI_RE, LAUGH_RE):
        text = pattern.sub(" ", text)
    text = re.sub(r"([!?.,~])\1+", r"\1", text)  # "!!!!" -> "!"
    return " ".join(text.split())


def clean(rows: list[dict]) -> list[dict]:
    """Giữ comment đủ dài sau làm sạch, bỏ trùng nội dung; thêm khoá `embed_text`."""
    seen: set[str] = set()
    out = []
    for r in rows:
        norm = normalize(" ".join((r.get("message") or "").split()))
        key = norm.casefold()
        if len(norm) < MIN_CHARS or key in seen:
            continue
        seen.add(key)
        out.append({**r, "embed_text": norm})
    return out
