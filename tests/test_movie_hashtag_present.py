"""movie_hashtag_present cases from production (2026-09-28)."""

from __future__ import annotations

from app.repositories.d1.posts import RELEVANT_POST_SQL, movie_hashtag_present

SCOTTY = "SCOTTY: GIẢI CỨU HOÀNG THƯỢNG"


def test_hashtag_keyword_matches_its_own_tag() -> None:
    # Keyword stored with its "#": tags are extracted without it.
    post = "02.10 ra rạp quẩy cùng Scotty nha! 🎬 #ScottyGiaiCuuHoangThuong #Scotty #SkylineMedia #CGV"
    assert movie_hashtag_present(post, SCOTTY, "#ScottyGiaiCuuHoangThuong")


def test_title_with_punctuation_matches_its_tag() -> None:
    # No keyword: the title alone ("SCOTTY: ...") must fold to the tag form.
    post = "Mới xem trailer mà đã muốn cho 2 anh nhỏ đi coi rồi 😆 #ScottyGiaiCuuHoangThuong"
    assert movie_hashtag_present(post, SCOTTY, None)


def test_foreign_post_with_same_tag_still_rejected() -> None:
    post = "que no ocurra eso por favor #fiesta #astonmartin #memin #f1"
    assert not movie_hashtag_present(post, "Mẹ Mìn", "#memin")


def test_relevant_post_predicate_keeps_unlabeled_keyword_matches() -> None:
    assert "relevance_label IS NULL AND p.keyword_match > 0" in RELEVANT_POST_SQL
    assert "relevance_label = 'related'" in RELEVANT_POST_SQL
