from app.services.d1 import movie_slug


def test_movie_slug_strips_vietnamese_and_punct():
    assert movie_slug("Đất Rừng Phương Nam!") == "dat-rung-phuong-nam"


def test_movie_slug_fallback_when_empty():
    assert movie_slug("   ") == "movie"
    assert movie_slug("!!!") == "movie"
