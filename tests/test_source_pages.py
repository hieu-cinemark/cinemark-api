import pytest

from app.core.errors import ValidationError
from app.services.crawl.source_pages import normalize_page_url


def test_normalize_page_url():
    assert normalize_page_url("https://www.facebook.com/traibuonnguoi.movie.vn/posts/pfbid0x?x=1") == (
        "https://www.facebook.com/traibuonnguoi.movie.vn"
    )
    assert normalize_page_url("GalaxyStudioVN") == "https://www.facebook.com/GalaxyStudioVN"
    assert normalize_page_url("@meminmovie") == "https://www.facebook.com/meminmovie"
    assert normalize_page_url("https://facebook.com/profile.php?id=123&sk=photos") == (
        "https://www.facebook.com/profile.php?id=123"
    )


@pytest.mark.parametrize(
    "url", ["", "https://www.facebook.com/groups/abc", "https://www.tiktok.com/@x", "https://www.facebook.com/watch/?v=1"]
)
def test_normalize_rejects_non_pages(url):
    with pytest.raises(ValidationError):
        normalize_page_url(url)
