"""Luật chọn bài phần cũ của scripts/tools/purge_old_part_posts.py: trước mốc + không nhắc dấu hiệu phần mới."""

from scripts.tools.purge_old_part_posts import OLD_PART_RULES, OldPartRule, fold, is_old_part

KIEN = next(r for r in OLD_PART_RULES if r.name.startswith("Thám Tử Kiên"))
SHAUN = next(r for r in OLD_PART_RULES if r.name.startswith("Shaun"))


def test_fold_removes_accents_and_case():
    assert fold("Lời Nguyền HOÀNG Kim Đạo") == "loi nguyen hoang kim dao"
    assert fold(None) == ""


def test_old_part_is_before_cutoff_and_not_mentioning_the_new_part():
    assert is_old_part("Review Thám Tử Kiên #kyankhongdau", "2025-05-04T10:00:00+00:00", KIEN)
    assert not is_old_part("Thám Tử Kiên Phần 2 được đầu tư khủng", "2025-11-26T10:00:00+00:00", KIEN)  # sau mốc
    assert not is_old_part("Hóng phần 2 quá", "2025-05-03T10:00:00+00:00", KIEN)  # trước mốc nhưng nhắc phần 2
    assert not is_old_part("Thám tử Kiên : Lời Nguyền Hoàng Kim", "2025-06-01T00:00:00+00:00", KIEN)
    assert not is_old_part("Victor Vũ mừng sinh nhật với Thám tử Kiên 2", "2025-04-01T00:00:00+00:00", KIEN)


def test_posts_on_or_after_the_cutoff_and_without_date_are_never_old():
    assert not is_old_part("shaunthesheepmovie", "2026-01-01T00:00:00+00:00", SHAUN)
    assert not is_old_part("shaunthesheepmovie", "2026-03-19T00:00:00+00:00", SHAUN)
    assert not is_old_part("shaunthesheepmovie", None, SHAUN)
    assert is_old_part("#shaunthesheepmovie #fullepisodes", "2023-06-16T00:00:00+00:00", SHAUN)


def test_shaun_new_film_markers_keep_early_trailer_posts():
    assert not is_old_part("Shaun the Sheep: The Beast of Mossy Bottom trailer", "2025-12-01T00:00:00+00:00", SHAUN)
    rule = OldPartRule("x", "m", "2026-01-01", ("abc",))
    assert is_old_part("zzz", "2020-01-01", rule)
