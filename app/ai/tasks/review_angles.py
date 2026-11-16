"""Review Angles: kiểm tra câu trả lời AI và các hàm thuần dùng khi gom góc.

Câu trả lời chỉ được nhận khi mọi dẫn chứng là đoạn nguyên văn của caption, postId có trong lô và mỗi bài thuộc
tối đa hai góc.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any

from app.ai.prompts.review_angles import REVIEW_ASPECTS


def fold(value: str) -> str:
    text = unicodedata.normalize("NFD", value)
    text = re.sub(r"[̀-ͯ]", "", text).replace("đ", "d").replace("Đ", "d").lower()
    return " ".join(text.split())


def verbatim_quote(quote: str, caption: str) -> str | None:
    """Đoạn nguyên văn của caption ứng với quote, hoặc None. Model hay gộp khoảng trắng / xuống dòng hoặc đổi nháy cong
    thành nháy thẳng - khi đó trả đúng đoạn trong caption (để tô sáng khớp) thay vì loại."""
    if quote in caption:
        return quote
    words = quote.split()
    if not words:
        return None
    quote_class = "\"“”'‘’"

    def token(word: str) -> str:
        return "".join(f"[{re.escape(quote_class)}]" if ch in quote_class else re.escape(ch) for ch in word)

    match = re.search(r"\s+".join(token(word) for word in words), caption)
    return match.group(0) if match else None


def parse_batch(raw: Any, posts: list[dict[str, Any]], catalog: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Phần hợp lệ của câu trả lời AI, hoặc None khi không đọc được (không phải JSON đúng dạng).

    Dẫn chứng phải là đoạn nguyên văn caption, postId có trong lô, mỗi bài tối đa 2 góc. Từ 16/11/2026 phần sai bị BỎ RIÊNG
    (dẫn chứng sai, góc sai khía cạnh / thiếu tên, bài thứ 3 trở đi của một bài) thay vì từ chối cả lô - trước đó một
    trích dẫn lệch là cả 24 bài bị đọc lại, sau 3 lần thì bị bỏ qua."""
    if not isinstance(raw, dict):
        return None
    summary, items = raw.get("summary"), raw.get("angles")
    if not isinstance(items, list):
        return None
    by_id = {post["id"]: post for post in posts}
    assignments: dict[str, set[str]] = {}
    angles = []
    for item in items[:16]:
        if not isinstance(item, dict):
            continue
        name, description, aspect, evidence = (
            item.get("name"),
            item.get("description"),
            item.get("aspect"),
            item.get("evidence"),
        )
        if (
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(aspect, str)
            or aspect not in REVIEW_ASPECTS
            or not isinstance(evidence, list)
        ):
            continue
        description = description.strip() if isinstance(description, str) and description.strip() else name.strip()
        angle_id = item.get("id") if isinstance(item.get("id"), str) and item.get("id") else None
        # id lạ (model bịa) hoặc id của góc khác khía cạnh: coi như góc mới / tìm theo tên.
        existing = (
            next((a for a in catalog if a["id"] == angle_id and a["aspect"] == aspect), None) if angle_id else None
        )
        if existing is None:
            existing = next((a for a in catalog if a["aspect"] == aspect and fold(a["name"]) == fold(name)), None)
        key = existing["id"] if existing else f"{aspect}|{fold(name)}"
        seen: set[str] = set()
        refs = []
        for ref in evidence:
            if not isinstance(ref, dict):
                continue
            post_id, quote = ref.get("postId"), ref.get("quote")
            if not isinstance(post_id, str) or post_id not in by_id or post_id in seen or not isinstance(quote, str):
                continue
            quote = verbatim_quote(quote.strip(), by_id[post_id]["caption"])
            if quote is None or len(quote.strip()) < 8 or len(quote) > 500:
                continue
            used = assignments.setdefault(post_id, set())
            if key not in used and len(used) >= 2:
                continue
            used.add(key)
            seen.add(post_id)
            refs.append({"postId": post_id, "quote": quote})
        if not refs:
            continue
        angles.append(
            {
                "id": existing["id"] if existing else None,
                "name": existing["name"] if existing else name.strip()[:150],
                "description": description.strip()[:1000],
                "aspect": aspect,
                "evidence": refs,
            }
        )
    return {"summary": summary.strip()[:2400] if isinstance(summary, str) else "", "angles": angles}


def has_text(content: str | None) -> bool:
    stripped = re.sub(r"https?://\S+|#\w+|@[\w.]+", " ", content or "")
    return bool(re.search(r"[^\W\d_]{2}", stripped))


def js_json(value: Any) -> str:
    """JSON.stringify của JS cho dict/chuỗi thường - để id góc băm giống bản TS."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def angle_id_of(aspect: str, name: str) -> str:
    return hashlib.sha256(js_json({"aspect": aspect, "name": fold(name)}).encode()).hexdigest()[:24]
