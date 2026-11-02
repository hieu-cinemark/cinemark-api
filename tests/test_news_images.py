import httpx
import pytest

from app.services.news_images import HeadImages, allowed_article, publisher_image


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/page",
        "http://thanhnien.vn/a",
        "https://thanhnien.vn.evil.test/a",
        "https://user:pass@thanhnien.vn/a",
        "https://thanhnien.vn:8080/a",
    ],
)
def test_only_known_publishers_over_https(url):
    assert not allowed_article(url)


def test_parser_accepts_meta_attribute_order_and_entities():
    parser = HeadImages()
    parser.feed('<meta content="https://cdn.example/image.jpg?a=1&amp;b=2" property="og:image">')
    assert parser.images["og:image"] == "https://cdn.example/image.jpg?a=1&b=2"


async def test_extracts_original_image_from_html_head():
    def handle(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            text='<head><meta property="og:image" content="https://cdn.example/original.jpg"></head>',
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await publisher_image(client, "https://thanhnien.vn/review") == "https://cdn.example/original.jpg"


async def test_redirect_cannot_escape_publisher_allowlist():
    seen = []

    def handle(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://127.0.0.1/private"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await publisher_image(client, "https://thanhnien.vn/review") is None
    assert len(seen) == 1
