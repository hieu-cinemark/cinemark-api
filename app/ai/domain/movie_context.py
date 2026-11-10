"""Khối "thông tin phim" đưa cho AI ở cả bước thu thập (phân loại độ liên quan của bài lúc ingest) lẫn bước phân
tích (gán cảm xúc/khía cạnh cho comment, report Social Topic) - để AI nhận ra tên đạo diễn, diễn viên, nhân vật trong
logline, và biết phim đã ra rạp hay chưa (bình luận về một phim chưa chiếu thì không thể là "đã xem").

Lấy từ các cột có sẵn của bảng movies (D1): director, `cast`, distributor, description (logline), released_at.
`cast` thường là mảng JSON (Worker cinemark ghi như vậy) nhưng dữ liệu cũ còn chuỗi "A, B" - cast_list đọc cả hai."""

from __future__ import annotations

import json
import re
from typing import Any

_SPLIT = re.compile(r"[,;\n]+")


def cast_list(raw: Any) -> list[str]:
    """Mảng JSON hoặc chuỗi "A, B; C" -> ["A", "B", "C"], bỏ "..." và phần tử rỗng, giữ thứ tự, không trùng."""
    if raw is None:
        return []
    if isinstance(raw, list):
        items: list[Any] = raw
    else:
        text = str(raw).strip()
        try:
            parsed = json.loads(text) if text else []
        except ValueError:
            parsed = None
        items = parsed if isinstance(parsed, list) else _SPLIT.split(text)
    result: list[str] = []
    for item in items:
        name = str(item).strip().strip(".…").strip()
        if name and name not in result:
            result.append(name)
    return result


def movie_context_block(movie: dict[str, Any], *, logline_chars: int = 400, max_cast: int = 12) -> str:
    """Các dòng "Key: value" mô tả phim, bỏ dòng nào không có dữ liệu. Dòng đầu luôn là tên phim."""
    lines = [f"TARGET FILM: {movie.get('title') or ''}"]
    if movie.get("released_at"):
        lines.append(f"Release date: {str(movie['released_at'])[:10]}")
    if movie.get("director"):
        lines.append(f"Director: {str(movie['director'])[:200]}")
    cast = cast_list(movie.get("cast"))
    if cast:
        more = f" (+{len(cast) - max_cast} more)" if len(cast) > max_cast else ""
        lines.append(f"Cast: {', '.join(cast[:max_cast])}{more}")
    if movie.get("distributor"):
        lines.append(f"Distributor: {str(movie['distributor'])[:200]}")
    logline = " ".join(str(movie.get("description") or "").split())
    if logline and logline_chars:
        if len(logline) > logline_chars:
            logline = logline[:logline_chars].rstrip() + "…"
        lines.append(f"Logline: {logline}")
    return "\n".join(lines)
