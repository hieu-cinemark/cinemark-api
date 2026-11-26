"""Luật loại video YouTube chỉ trùng tên phim (phim bộ, nhạc, truyện audio, văn nghệ) - 26/11/2026."""

import pytest

from app.services.relevance.relevance_rules import resolve_relevance, youtube_noise_reason

MOVIE = {"title": "Nhất Quỷ Nhì Ma", "director": "Đoàn Quang", "cast": '["Nam Cường"]'}


@pytest.mark.parametrize(
    "text",
    [
        "Nữ Binh Phong Hỏa - Tập 12 | Tình Đồng Đội",
        "Nhất quỷ nhì ma, thứ ba Takagi 3 - Tập 12 [Việt sub]",
        "[AUDIO] Toàn Cầu Phế Thổ - Phần 4 (Chương 160 - 209) | Review Truyện",
        "Nhất quỷ nhì ma - Lớp 8A5 - Hội thi Dân vũ năm 2025",
        "Mashup - CAO BỒI LANG | The Masked Singer Vietnam [Live Stage]",
    ],
)
def test_noise_detected(text):
    assert youtube_noise_reason(text, "youtube")


@pytest.mark.parametrize(
    "text",
    [
        "Thế Nào Là Đúng Official MV (OST Chị Chị Em Em 3)",  # nhạc phim vẫn là bài phim
        "MẸ MÌN | Special Trailer | DKKC: 27.11.2026",
        "Hậu trường Bò Sữa Bay: Ngọc Xuân kể về San",
        "Defining Love Provided to YouTube by 12 trái lê · Chị Chị Em Em 3 (Original Motion Picture Soundtrack)",
        "Ngọc nữ Ninh Dương Lan Ngọc tại thảm đỏ ra mắt phim Chị Chị Em Em 3 #music #remix",
        "Cửa Chốt Then Cài (From Chị Chị Em Em 3) Provided to YouTube by Lona",
        "CHỊ CHỊ EM EM 3 | BTS Tập 1 - Sự trở lại của Vũ trụ hồng nhan",
    ],
)
def test_film_videos_kept(text):
    assert youtube_noise_reason(text, "youtube") is None


def test_only_youtube():
    assert youtube_noise_reason("Nhất quỷ nhì ma remix", "tiktok") is None


def test_noise_overrides_rules_and_ai():
    text = "Nhất Quỷ Nhì Ma - Tập 3 [Việt sub] phim Nhất Quỷ Nhì Ma"
    assert resolve_relevance("related", text, MOVIE, has_keyword=True, strict=False, platform="youtube")[0] is False
    assert resolve_relevance("related", text, MOVIE, has_keyword=True, strict=False, platform="tiktok")[0] is True
