"""Bộ khía cạnh và giai đoạn khán giả cố định mà Kira gán cho từng comment, cùng lời gọi với nhãn cảm xúc
(app/ai/tasks/sentiment.py) - dùng chung cho bộ phân loại, phần tổng hợp của report
(app.services.d1.get_movie_aspect_stats) và scripts/accuracy_sample.py.

Cố định thay vì để AI tự đặt tên topic (2026-10-07): topic tự do mỗi lần chạy mỗi khác nên không so được giữa các
phim hay theo thời gian; một bộ khía cạnh cố định cho ra được % khen/chê, tỉ trọng nhắc tới và xu hướng theo tuần
cho từng khía cạnh. Đổi key là đổi dữ liệu đã lưu - chỉ thêm key mới, không đổi tên key cũ.

Không import gì trong app - để cả app.services.d1 lẫn app.ai.tasks.sentiment import được mà không vòng."""

from __future__ import annotations

from typing import Any

# key -> (nhãn hiển thị, mô tả cho prompt)
ASPECTS: dict[str, tuple[str, str]] = {
    "dien_xuat": ("Diễn xuất", "how the actors perform their roles"),
    "dien_vien": ("Dàn diễn viên, casting", "who was cast, actors' looks/fame/fit for the role - not their acting"),
    "kich_ban": ("Kịch bản, cốt truyện", "story, plot, pacing, ending, logic, dialogue"),
    "cam_xuc": ("Hài, cảm xúc", "whether it is funny, moving, scary, gripping - the feeling while watching"),
    "hinh_anh": ("Kỹ xảo, hình ảnh", "visuals, VFX, cinematography, costumes, sets, makeup"),
    "am_thanh": ("Âm nhạc, âm thanh", "soundtrack, music, sound"),
    "thong_diep": ("Thông điệp, tính chân thực", "message/meaning, historical or real-life accuracy"),
    "san_phim": ("Sạn, lỗi phim", "goofs, mistakes, continuity errors, things that break immersion"),
    "quang_ba": ("Trailer, quảng bá", "trailer, posters, marketing, promotional events"),
    "rap_ve": ("Giá vé, suất chiếu, rạp", "ticket prices, showtimes, theater availability, age rating"),
}

# Giai đoạn của người viết so với phim.
STAGES: dict[str, tuple[str, str]] = {
    "hong": ("Đang hóng", "has NOT watched yet: anticipating, curious, planning to watch or not to watch"),
    "da_xem": ("Đã xem", "has watched the movie and talks from that experience"),
    "khac": ("Khác", "unclear, news/facts, or not about watching the movie"),
}

POLARITIES = {"+": "khen", "-": "che"}


def parse_aspects(value: Any) -> list[str]:
    """Danh sách "key:+"/"key:-" hợp lệ, không trùng, theo thứ tự xuất hiện - bỏ âm thầm mọi thứ ngoài bộ
    ASPECTS (model đôi khi tự đặt key mới). Nhận cả dạng {"a": key, "p": "+"} lẫn chuỗi "key:+"."""
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if isinstance(item, dict):
            key, polarity = item.get("a") or item.get("aspect"), item.get("p") or item.get("polarity")
        elif isinstance(item, str) and ":" in item:
            key, polarity = item.rsplit(":", 1)
        else:
            continue
        key = str(key).strip().lower()
        polarity = str(polarity).strip()
        tag = f"{key}:{polarity}"
        if key in ASPECTS and polarity in POLARITIES and tag not in result:
            result.append(tag)
    return result


def parse_stage(value: Any) -> str:
    stage = str(value or "").strip().lower()
    return stage if stage in STAGES else "khac"


def aspect_prompt_lines() -> str:
    return "\n".join(f'- "{key}": {desc}' for key, (_label, desc) in ASPECTS.items())


def stage_prompt_lines() -> str:
    return "\n".join(f'- "{key}": {desc}' for key, (_label, desc) in STAGES.items())
