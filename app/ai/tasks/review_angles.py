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

from app.ai.prompts.review_angles import REVIEW_ASPECTS, STANCES


def stance_of(value: Any) -> str | None:
    """'khen' | 'che' | 'trung_tinh', hoặc None khi model trả giá trị lạ / thiếu (trang tính là chưa rõ)."""
    text = str(value or "").strip().lower().replace(" ", "_")
    aliases = {"chê": "che", "trung_tính": "trung_tinh", "neutral": "trung_tinh", "positive": "khen", "negative": "che"}
    text = aliases.get(text, text)
    return text if text in STANCES else None


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
            refs.append({"postId": post_id, "quote": quote, "stance": stance_of(ref.get("stance"))})
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


# --- dọn góc cũ (scripts/tools/review_angles_cleanup.py) ---


def merge_groups(angles: list[dict[str, Any]], aspect: str, raw: Any) -> list[dict[str, Any]] | None:
    """Danh sách góc mới sau khi gộp các góc của `aspect` theo nhóm AI trả, hoặc None khi câu trả lời không dùng được.

    Nhóm giữ id của góc nhiều dẫn chứng nhất (link/bộ nhớ đệm trỏ tới id đó vẫn đúng), tên/mô tả theo AI; dẫn chứng gộp,
    bỏ trùng theo bài. Id AI bỏ sót giữ nguyên là góc riêng; id lạ hoặc id nằm ở hai nhóm bị bỏ qua. Góc khía cạnh khác
    không đổi, thứ tự giữ như cũ."""
    if not isinstance(raw, dict) or not isinstance(raw.get("groups"), list):
        return None
    by_id = {a["id"]: a for a in angles if a["aspect"] == aspect}
    used: set[str] = set()
    merged: dict[str, dict[str, Any]] = {}  # id góc giữ lại -> góc sau gộp
    absorbed: set[str] = set()
    for group in raw["groups"]:
        if not isinstance(group, dict) or not isinstance(group.get("ids"), list):
            continue
        ids = [i for i in dict.fromkeys(group["ids"]) if isinstance(i, str) and i in by_id and i not in used]
        if len(ids) < 2:
            used.update(ids)
            continue
        used.update(ids)
        members = sorted((by_id[i] for i in ids), key=lambda a: -len(a["refs"]))
        keep = members[0]
        refs: list[dict[str, Any]] = []
        seen: set[str] = set()
        for member in members:
            for ref in member["refs"]:
                if ref["id"] not in seen:
                    seen.add(ref["id"])
                    refs.append(ref)
        name = group.get("name")
        description = group.get("description")
        merged[keep["id"]] = {
            **keep,
            "name": name.strip()[:150] if isinstance(name, str) and name.strip() else keep["name"],
            "description": description.strip()[:1000]
            if isinstance(description, str) and description.strip()
            else keep.get("description", ""),
            "refs": refs,
        }
        absorbed.update(m["id"] for m in members[1:])
    if not merged:
        return None
    return [merged.get(a["id"], a) for a in angles if a["id"] not in absorbed]


def parse_stances(raw: Any, count: int) -> dict[int, str]:
    """{i: stance} hợp lệ từ câu trả lời gắn stance (mục thiếu / lạ bị bỏ, lần chạy sau thử lại)."""
    if not isinstance(raw, dict) or not isinstance(raw.get("stances"), list):
        return {}
    result: dict[int, str] = {}
    for item in raw["stances"]:
        if isinstance(item, dict) and isinstance(item.get("i"), int) and 0 <= item["i"] < count:
            stance = stance_of(item.get("stance"))
            if stance:
                result[item["i"]] = stance
    return result
