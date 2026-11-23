# app/ai/tasks/ask_comments.py  (hàm thuần, dễ test)
from __future__ import annotations

import re
from collections.abc import Sequence

from app.ai.retrieval.base import ScoredDocument

NO_DATA = "Chưa đủ dữ liệu để trả lời: không tìm thấy bình luận liên quan."
_CITATION = re.compile(r"\[(\d+)\]")


def fit_sources(results: Sequence[ScoredDocument], max_chars: int) -> list[ScoredDocument]:
    """Giữ các nguồn đầu vừa max_chars, tính đúng như build_context, để số [i] trong prompt khớp danh sách nguồn trả về UI."""
    kept, used = [], 0
    for index, item in enumerate(results, start=1):
        size = len(f"[{index}] {item.document.text.strip()}")
        if used + size > max_chars:
            break
        kept.append(item)
        used += size
    return kept


def build_user_prompt(question: str, context: str) -> str:
    return f"CÂU HỎI: {question.strip()}\n\nBÌNH LUẬN:\n{context}\n\nTrả lời đúng quy tắc, kèm số nguồn."


def cited_sources(answer: str, source_count: int) -> list[int]:
    seen: list[int] = []
    for match in _CITATION.finditer(answer):
        number = int(match.group(1))
        if 1 <= number <= source_count and number not in seen:
            seen.append(number)
    return seen


def strip_invalid_citations(answer: str, source_count: int) -> str:
    """Bỏ các [k] ngoài 1..source_count (model bịa số nguồn)."""
    return _CITATION.sub(lambda m: m.group(0) if 1 <= int(m.group(1)) <= source_count else "", answer).strip()
