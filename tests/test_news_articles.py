import httpx

from app.services.news_articles import analysis_input, extract_article, parse_article


def html():
    paragraphs = "".join(
        f"<p>Đoạn {i}: Bộ phim có diễn xuất và hình ảnh đáng chú ý. Khán giả nhận xét kịch bản còn nhiều hạn chế nhưng cảm xúc chân thực và âm thanh được đầu tư. </p>"
        for i in range(15)
    )
    return f"""<html><head><title>Bài đánh giá phim</title>
    <meta property="article:published_time" content="2026-09-28T14:32:14+07:00">
    <meta property="article:modified_time" content="2026-11-02T10:00:00+07:00">
    <meta property="og:image" content="https://cdn.example/original.jpg">
    </head><body><article><h1>Bài đánh giá phim</h1>{paragraphs}</article>
    <aside><time>7 giờ trước</time>Bài liên quan</aside></body></html>"""


def test_uses_original_published_date_instead_of_modified_or_related_time():
    result = parse_article(html(), "https://znews.vn/review.html")
    assert result["published_at"] == "2026-09-28T14:32:14+07:00"
    assert result["extraction_status"] == "extracted"
    assert len(result["content"]) > 100
    assert result["image_url"] == "https://cdn.example/original.jpg"


async def test_redirect_cannot_fetch_private_host():
    calls = []

    def handle(request):
        calls.append(request.url)
        return httpx.Response(302, headers={"location": "https://127.0.0.1/private"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await extract_article(client, "https://znews.vn/review") is None
    assert len(calls) == 1


def test_prepared_input_caps_sources_and_marks_snippet_fallback():
    articles = [
        {
            "title": "Review",
            "content": "x" * 15000,
            "snippet": "Brief",
            "extraction_status": "extracted",
            "published_at": "2026-09-28",
        }
        for _ in range(12)
    ]
    data = analysis_input("Phim", articles)
    assert len(data["articles"]) == 10
    assert len(data["articles"][0]["content"]) == 8000
    assert data["articles"][0]["contentTruncated"]
    assert data["articles"][0]["publishedAt"] == "2026-09-28"
    data = analysis_input("Phim", [{"snippet": "Brief"}])
    assert data["articles"][0]["contentStatus"] == "snippet_only"
    assert data["articles"][0]["content"] == "Brief"


def test_analysis_preserves_global_numbers_for_balanced_selection():
    result = analysis_input("Phim", [{"source_index": 12, "snippet": "News"}])
    assert result["articles"][0]["sourceIndex"] == 12
