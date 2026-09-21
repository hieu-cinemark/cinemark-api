from app.services.d1 import post_mentions_movie


def test_full_title_in_post():
    assert post_mentions_movie("Tối nay đi xem Anh Hùng với bạn", "Anh Hùng")


def test_hashtag_without_spaces():
    assert post_mentions_movie("trailer #AnhHùng quá hay", "Anh Hùng")
    assert post_mentions_movie("trailer #anh_hùng quá hay", "Anh Hùng")


def test_rejects_unrelated_text():
    assert not post_mentions_movie("phim này hay quá", "Anh Hùng")
    assert not post_mentions_movie(None, "Anh Hùng")
    assert not post_mentions_movie("Anh Hùng", None)
