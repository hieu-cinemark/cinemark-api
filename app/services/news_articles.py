"""Trích nội dung tối đa 10 bài, ngày xuất bản gốc và ảnh; lưu D1, không gọi AI."""

import asyncio
import json
from datetime import datetime
from urllib.parse import urljoin

import httpx
import trafilatura

from app.services.news_images import HeadImages, allowed_article, image_url

MAX_ARTICLES = 10
MAX_HTML_BYTES = 2 * 1024 * 1024
MAX_CONTENT_CHARS = 12000


class ArticleHead(HeadImages):
    def __init__(self):
        super().__init__()
        self.published = None

    def handle_starttag(self, tag, attrs):
        super().handle_starttag(tag, attrs)
        if tag.lower() != "meta":
            return
        values = dict(attrs)
        kind = (values.get("property") or values.get("name") or "").lower()
        if kind in {"article:published_time", "og:published_time", "pubdate", "publishdate", "datepublished"}:
            self.published = self.published or values.get("content")


def valid_date(value) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        datetime.fromisoformat(value)
        return value
    except ValueError:
        return None


def parse_article(html: str, url: str) -> dict:
    head = ArticleHead()
    head.feed(html)
    raw = trafilatura.extract(
        html,
        url=url,
        output_format="json",
        with_metadata=True,
        include_comments=False,
        include_tables=False,
        date_extraction_params={"original_date": True, "extensive_search": False},
    )
    data = json.loads(raw) if raw else {}
    content = str(data.get("text") or "").strip()
    result = {
        "url": url,
        "extracted_title": data.get("title"),
        "author": data.get("author"),
        "published_at": valid_date(head.published) or valid_date(data.get("date")),
        "content": content[:MAX_CONTENT_CHARS],
        "content_truncated": len(content) > MAX_CONTENT_CHARS,
        "extraction_status": "extracted" if len(content) >= 100 else "snippet_only",
    }
    for key in ("og:image:secure_url", "og:image", "twitter:image"):
        value = head.images.get(key)
        image = image_url(value, url) if value else None
        if image:
            result["image_url"] = image
            break
    return result


async def extract_article(client: httpx.AsyncClient, url: str) -> dict | None:
    for _ in range(3):
        if not allowed_article(url):
            return None
        async with client.stream("GET", url) as response:
            if response.status_code in (301, 302, 303, 307, 308):
                url = urljoin(url, response.headers.get("location", ""))
                continue
            if response.status_code != 200 or "text/html" not in response.headers.get("content-type", ""):
                return None
            body = bytearray()
            async for chunk in response.aiter_bytes():
                if len(body) + len(chunk) > MAX_HTML_BYTES:
                    return None
                body.extend(chunk)
            return await asyncio.to_thread(
                parse_article, body.decode(response.encoding or "utf-8", errors="replace"), url
            )
    return None


async def enrich(articles: list[dict]) -> None:
    semaphore = asyncio.Semaphore(4)
    async with httpx.AsyncClient(
        timeout=8, follow_redirects=False, headers={"User-Agent": "Cinemark-PressPreview/1.0"}
    ) as client:

        async def one(article):
            article["extraction_status"] = "snippet_only"
            async with semaphore:
                try:
                    result = await asyncio.wait_for(extract_article(client, str(article.get("link") or "")), timeout=12)
                    if result:
                        article.update(result)
                except httpx.HTTPError, TimeoutError, ValueError:
                    pass

        await asyncio.gather(*(one(article) for article in articles[:MAX_ARTICLES]))


def analysis_input(title: str, articles: list[dict]) -> dict:
    return {
        "movieTitle": title,
        "articles": [
            {
                "sourceIndex": item.get("source_index", i + 1),
                "title": item.get("extracted_title") or item.get("title"),
                "source": item.get("source"),
                "link": item.get("link"),
                "author": item.get("author"),
                "publishedAt": item.get("published_at"),
                "contentStatus": item.get("extraction_status", "snippet_only"),
                "contentTruncated": bool(item.get("content_truncated")) or len(item.get("content") or "") > 8000,
                "content": (
                    (item.get("content") if item.get("extraction_status") == "extracted" else item.get("snippet")) or ""
                )[:8000],
            }
            for i, item in enumerate(articles[:MAX_ARTICLES])
        ],
    }
