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


def parse_batch(raw: Any, posts: list[dict[str, Any]], catalog: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Câu trả lời AI hợp lệ hoặc None: dẫn chứng phải nguyên văn caption, postId có trong lô, mỗi bài tối đa 2 góc."""
    if not isinstance(raw, dict):
        return None
    summary, items = raw.get("summary"), raw.get("angles")
    if not isinstance(summary, str) or not summary.strip() or not isinstance(items, list) or len(items) > 16:
        return None
    by_id = {post["id"]: post for post in posts}
    assignments: dict[str, set[str]] = {}
    angles = []
    for item in items:
        if not isinstance(item, dict):
            return None
        name, description, aspect, evidence = (
            item.get("name"),
            item.get("description"),
            item.get("aspect"),
            item.get("evidence"),
        )
        if (
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(description, str)
            or not description.strip()
            or not isinstance(aspect, str)
            or aspect not in REVIEW_ASPECTS
            or not isinstance(evidence, list)
            or not evidence
        ):
            return None
        angle_id = item.get("id") if isinstance(item.get("id"), str) and item.get("id") else None
        if angle_id:
            existing = next((a for a in catalog if a["id"] == angle_id), None)
            if not existing:
                return None
        else:
            existing = next((a for a in catalog if a["aspect"] == aspect and fold(a["name"]) == fold(name)), None)
        if existing and existing["aspect"] != aspect:
            return None
        key = existing["id"] if existing else f"{aspect}|{fold(name)}"
        seen: set[str] = set()
        refs = []
        for ref in evidence:
            if not isinstance(ref, dict):
                return None
            post_id, quote = ref.get("postId"), ref.get("quote")
            if (
                not isinstance(post_id, str)
                or not isinstance(quote, str)
                or len(quote.strip()) < 8
                or len(quote) > 500
                or post_id not in by_id
                or quote not in by_id[post_id]["caption"]
            ):
                return None
            if post_id in seen:
                continue
            seen.add(post_id)
            used = assignments.setdefault(post_id, set())
            used.add(key)
            if len(used) > 2:
                return None
            refs.append({"postId": post_id, "quote": quote})
        angles.append(
            {
                "id": existing["id"] if existing else None,
                "name": existing["name"] if existing else name.strip()[:150],
                "description": description.strip()[:1000],
                "aspect": aspect,
                "evidence": refs,
            }
        )
    return {"summary": summary.strip()[:2400], "angles": angles}


def has_text(content: str | None) -> bool:
    stripped = re.sub(r"https?://\S+|#\w+|@[\w.]+", " ", content or "")
    return bool(re.search(r"[^\W\d_]{2}", stripped))


def js_json(value: Any) -> str:
    """JSON.stringify của JS cho dict/chuỗi thường - để id góc băm giống bản TS."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def angle_id_of(aspect: str, name: str) -> str:
    return hashlib.sha256(js_json({"aspect": aspect, "name": fold(name)}).encode()).hexdigest()[:24]
