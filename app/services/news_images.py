"""Đọc ảnh Open Graph từ các báo đã biết; không tải toàn văn hoặc fetch URL tuỳ ý."""

import asyncio
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import httpx

PUBLISHERS = {
    "thanhnien.vn",
    "vnexpress.net",
    "tuoitre.vn",
    "znews.vn",
    "zingnews.vn",
    "kenh14.vn",
    "vietnamnet.vn",
    "vtcnews.vn",
    "dantri.com.vn",
    "nld.com.vn",
    "thegioidienanh.vn",
    "saostar.vn",
    "tienphong.vn",
    "laodong.vn",
    "vov.vn",
    "antv.gov.vn",
    "anninhthudo.vn",
    "baomoi.com",
    "vietpress.vn",
    "phunuonline.com.vn",
}
MAX_HEAD_BYTES = 512 * 1024


def allowed_article(url: str) -> bool:
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        return (
            parsed.scheme == "https"
            and parsed.port in (None, 443)
            and not parsed.username
            and not parsed.password
            and any(host == domain or host == "www." + domain for domain in PUBLISHERS)
        )
    except ValueError:
        return False


def image_url(value: str, base: str) -> str | None:
    try:
        url = urljoin(base, value.strip())
        parsed = urlsplit(url)
        if parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password:
            return url
    except ValueError:
        pass
    return None


class HeadImages(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.images = {}

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "meta":
            return
        attrs = dict(attrs)
        kind = (attrs.get("property") or attrs.get("name") or "").lower()
        if kind in {"og:image", "og:image:secure_url", "twitter:image"} and attrs.get("content"):
            self.images.setdefault(kind, attrs["content"])


async def publisher_image(client: httpx.AsyncClient, url: str) -> str | None:
    for _ in range(3):
        if not allowed_article(url):
            return None
        async with client.stream("GET", url) as response:
            if response.status_code in (301, 302, 303, 307, 308):
                url = urljoin(url, response.headers.get("location", ""))
                continue  # mỗi redirect phải được kiểm tra allowlist lại
            if response.status_code != 200 or "text/html" not in response.headers.get("content-type", ""):
                return None
            head = bytearray()
            async for chunk in response.aiter_bytes():
                remaining = MAX_HEAD_BYTES - len(head)
                head.extend(chunk[:remaining])
                if len(head) >= MAX_HEAD_BYTES or b"</head>" in head.lower():
                    break
            parser = HeadImages()
            parser.feed(head.decode("utf-8", errors="replace"))
            for key in ("og:image:secure_url", "og:image", "twitter:image"):
                image = image_url(parser.images.get(key, ""), url) if parser.images.get(key) else None
                if image:
                    return image
            return None
    return None


async def enrich(articles: list[dict]) -> None:
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(
        timeout=6, follow_redirects=False, headers={"User-Agent": "Cinemark-PressPreview/1.0"}
    ) as client:

        async def one(article: dict) -> None:
            async with sem:
                try:
                    image = await asyncio.wait_for(publisher_image(client, str(article.get("link") or "")), timeout=10)
                    if image:
                        article["image_url"] = image
                except httpx.HTTPError, TimeoutError, ValueError:
                    pass  # ảnh lỗi không làm hỏng lượt lấy tin

        await asyncio.gather(*(one(article) for article in articles[:20]))
