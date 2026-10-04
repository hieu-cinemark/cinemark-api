"""Các bài thật gặp trên production (2026-09-28) cho app/services/relevance_rules.py."""

from __future__ import annotations

import pytest

from app.services.relevance_rules import foreign_language_reason, mentions_other_film

TRACKED = ["Án Mạng Karaoke", "Án Mạng Xém Hoàn Hảo", "Trại Buôn Người", "Trại Giam Hạnh Phúc", "Út Lan", "Út Lan 2"]


@pytest.mark.parametrize(
    "text",
    [
        "que no ocurra eso por favor #fiesta #astonmartin #gerardromero #memin #f1",
        "Muy deliciosa la comida.. siempre me encanta ir ai a comer.. se los super recomiendo #galerias #MeMin",
        "Grateful for the people around and a big shoutout to the team keralastartupfest #startup #memin",
        "🚨  ఇంత దిక్కుమాలిన సినిమా ఎప్పుడూ చూడలేదు..   సంఘం శరత్ థియేటర్ లో టికెట్ #BongMaNhaHat",
    ],
)
def test_foreign_posts_are_flagged(text: str) -> None:
    assert foreign_language_reason(text) is not None


@pytest.mark.parametrize(
    "text",
    [
        "Phim hay qua di xem di moi nguoi oi #memin",  # tiếng Việt không dấu
        "𝐞𝐛𝐞́ 🍓 𝟖 𝐭𝐮𝐨̂̉𝐢 𝐜𝐮̉𝐚 𝐭𝐮𝐢 𝐯𝐚̀𝐨 𝐯𝐚𝐢 𝐇𝐨𝐚̀𝐧𝐠 𝐇𝐚̣̂𝐮",  # tiếng Việt bằng Unicode trang trí
        "One more time for cinetourrr😭 “ a memory to keep “ ❤️ #nghihesonghihuu",  # tiếng Anh nói về phim
        "#memin #fyp",  # chỉ có hashtag: không có phán quyết nếu không có ngôn ngữ của nền tảng
        "Pass vé ngày mai 100k 2 vé PHIM TRẠI BUÔN NGƯỜIIIII 22H30",
    ],
)
def test_vietnamese_or_undecidable_posts_are_kept(text: str) -> None:
    assert foreign_language_reason(text) is None


def test_platform_language_decides_hashtag_only_posts() -> None:
    assert foreign_language_reason("#memin #fyp", "es") == "platform_language=es"
    assert foreign_language_reason("#memin #fyp", "vi") is None
    # Dấu tiếng Việt được ưu tiên hơn tag ngôn ngữ sai của nền tảng.
    assert foreign_language_reason("Phim này hay quá #memin", "en") is None


def test_other_film_detected_when_target_absent() -> None:
    other = mentions_other_film(
        "Và đây là Lan Trinh trong Án Mạng Xém Hoàn Hảo", "Án Mạng Karaoke", "Án Mạng Karaoke", TRACKED
    )
    assert other == "Án Mạng Xém Hoàn Hảo"
    assert (
        mentions_other_film(
            "PASS VÉ PHIM RẠP CINESTAR... pass vé xem phim TRẠI BUÔN NGƯỜI",
            "Trại Giam Hạnh Phúc",
            "Trại Giam Hạnh Phúc",
            TRACKED,
        )
        == "Trại Buôn Người"
    )


def test_target_mention_wins_over_other_film() -> None:
    assert (
        mentions_other_film("Án Mạng Karaoke vs Án Mạng Xém Hoàn Hảo", "Án Mạng Karaoke", "#AnMangKaraoke", TRACKED)
        is None
    )
    # Dạng hashtag của từ khoá mục tiêu cũng được tính là có nhắc tới.
    assert (
        mentions_other_film(
            "xem #AnMangKaraoke rồi mới tới Án Mạng Xém Hoàn Hảo", "Án Mạng Karaoke", "#AnMangKaraoke", TRACKED
        )
        is None
    )


def test_nested_and_short_titles_are_ignored() -> None:
    assert mentions_other_film("Út Lan hay quá", "Út Lan 2", "#UtLan2", TRACKED) is None
