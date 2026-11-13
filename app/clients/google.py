"""Google miễn phí, không key, không trình duyệt (13/11/2026): News RSS, giải link bài gốc, gợi ý tìm kiếm, Trends.

Cách gọi lấy từ omkarcloud/google-scraper (MIT, https://github.com/omkarcloud/google-scraper - news.py, autocomplete.py,
trends.py), viết lại bằng httpx async. Trang kết quả Google Search (SERP) cần trình duyệt + giải captcha nên vẫn đi SerpApi
(app/services/google_news.search_web).

- news_search: news.google.com/rss/search - tối đa ~100 bài, toán tử trong q: when:7d, site:, after:/before:.
- resolve_news_link: link RSS là news.google.com/rss/articles/<id>; trang đó có chữ ký data-n-a-sg/ts, gửi kèm id qua
  batchexecute rpc Fbv4je (garturlreq) để lấy URL bài gốc - news_articles.enrich cần URL gốc để đọc nội dung.
- autocomplete: www.google.com/complete/search?client=chrome - JSON [q, [gợi ý...], ...].
- trending: trends.google.com/trending/rss?geo=VN.
- interest_over_time / related_queries: /trends/api/explore -> widget (request + token) -> /trends/api/widgetdata/*.
  /trends/api trả 429 tới khi phiên có cookie NID mà trang /trends/explore đặt - gặp 429 thì mở trang đó rồi thử lại.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Any
from xml.etree import ElementTree

import httpx

from app.core.errors import UpstreamError

NEWS_SITE = "https://news.google.com"
TRENDS_SITE = "https://trends.google.com"
TIMEOUT_SECONDS = 20.0
HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}
_ARTICLE_ID_RE = re.compile(r"/(?:rss/)?articles/([A-Za-z0-9_\-]+)")
_SIG_RE = re.compile(r'data-n-a-sg="([^"]+)"[^>]*data-n-a-ts="([^"]+)"|data-n-a-ts="([^"]+)"[^>]*data-n-a-sg="([^"]+)"')
_TRENDS_NS = "{https://trends.google.com/trending/rss}"
_XSSI = ")]}'"


class GoogleBlocked(UpstreamError):
    """403/429: Google đang chặn IP này."""


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT_SECONDS, follow_redirects=True)


def _check(resp: httpx.Response, label: str) -> None:
    if resp.status_code in (403, 429):
        raise GoogleBlocked(f"Google chặn ({resp.status_code}) ở {label}.")
    if resp.status_code != 200:
        raise UpstreamError(f"Google trả HTTP {resp.status_code} ở {label}.")


def _strip_xssi(text: str) -> str:
    """Bỏ tiền tố chống XSSI: ")]}'" (explore, batchexecute) hoặc ")]}'," (widgetdata)."""
    return text[len(_XSSI) :].lstrip().removeprefix(",").lstrip() if text.startswith(_XSSI) else text


def _iso(pubdate: str | None) -> str | None:
    try:
        return parsedate_to_datetime(pubdate).astimezone(UTC).isoformat() if pubdate else None
    except TypeError, ValueError:
        return None


def _split_title(title: str) -> tuple[str, str | None]:
    """'Tiêu đề - Báo' -> ('Tiêu đề', 'Báo')."""
    head, sep, source = (title or "").rpartition(" - ")
    if sep and source and len(source) < 60:
        return head.strip(), source.strip()
    return (title or "").strip(), None


def article_id(link: str | None) -> str | None:
    match = _ARTICLE_ID_RE.search(link or "")
    return match.group(1) if match else None


def parse_news_rss(xml: bytes | str) -> list[dict[str, Any]]:
    try:
        root = ElementTree.fromstring(xml.encode() if isinstance(xml, str) else xml)
    except ElementTree.ParseError:
        raise UpstreamError("Google News trả RSS không hợp lệ.") from None
    channel = root.find("channel")
    items = []
    for item in channel.findall("item") if channel is not None else []:
        headline, source_from_title = _split_title(item.findtext("title") or "")
        source = item.find("source")
        link = (item.findtext("link") or "").strip()
        items.append(
            {
                "id": article_id(link),
                "title": headline or None,
                "google_link": link or None,
                "source": (source.text or "").strip() if source is not None and source.text else source_from_title,
                "source_url": source.get("url") if source is not None else None,
                "published_at": _iso(item.findtext("pubDate")),
            }
        )
    return items


async def news_search(
    q: str, *, hl: str = "vi", gl: str = "VN", when: str | None = None, client: httpx.AsyncClient | None = None
) -> list[dict[str, Any]]:
    """when: '7d', '1m'... (thêm 'when:<when>' vào q)."""
    gl = gl.upper()
    query = f"{q} when:{when}" if when else q
    params = {"q": query, "hl": hl, "gl": gl, "ceid": f"{gl}:{hl.split('-')[0]}"}
    async with _maybe(client) as c:
        resp = await c.get(f"{NEWS_SITE}/rss/search", params=params)
    _check(resp, "Google News RSS")
    return parse_news_rss(resp.content)


def _garturlreq(ident: str, sg: str, ts: str) -> str:
    payload = [
        "garturlreq",
        [
            ["X", "X", ["X", "X"], None, None, 1, 1, "US:en", None, 1, None, None, None, None, None, 0, 1],
            "X",
            "X",
            1,
            [1, 1, 1],
            1,
            1,
            None,
            0,
            0,
            None,
            0,
        ],
        ident,
        int(ts) if str(ts).isdigit() else ts,
        sg,
    ]
    rpc = [[["Fbv4je", json.dumps(payload, separators=(",", ":")), None, "generic"]]]
    return json.dumps(rpc, separators=(",", ":"))


def parse_batchexecute_link(text: str) -> str | None:
    for line in _strip_xssi(text).splitlines():
        line = line.strip()
        if not line.startswith("["):
            continue
        try:
            chunk = json.loads(line)
        except ValueError:
            continue
        for row in chunk:
            if isinstance(row, list) and len(row) > 2 and row[0] == "wrb.fr" and isinstance(row[2], str):
                try:
                    inner = json.loads(row[2])
                except ValueError:
                    continue
                if isinstance(inner, list) and len(inner) > 1 and inner[0] == "garturlres":
                    return inner[1]
    return None


async def resolve_news_link(link_or_id: str, *, client: httpx.AsyncClient | None = None) -> str | None:
    """URL bài gốc của một bài Google News, None khi không giải được."""
    ident = article_id(link_or_id) or link_or_id.strip()
    async with _maybe(client) as c:
        page = await c.get(f"{NEWS_SITE}/rss/articles/{ident}")
        if page.status_code != 200:
            return None
        match = _SIG_RE.search(page.text)
        if not match:
            return None
        sg, ts = (match.group(1), match.group(2)) if match.group(1) else (match.group(4), match.group(3))
        resp = await c.post(
            f"{NEWS_SITE}/_/DotsSplashUi/data/batchexecute",
            data={"f.req": _garturlreq(ident, sg, ts)},
            headers={"origin": NEWS_SITE, "referer": NEWS_SITE + "/"},
        )
    if resp.status_code != 200:
        return None
    return parse_batchexecute_link(resp.text)


async def resolve_links(items: list[dict[str, Any]], *, concurrency: int = 4) -> None:
    """Gán item['link'] = URL gốc (giữ link Google khi không giải được)."""
    sem = asyncio.Semaphore(concurrency)
    async with _client() as c:

        async def one(item: dict[str, Any]) -> None:
            async with sem:
                try:
                    url = await resolve_news_link(item.get("id") or item.get("google_link") or "", client=c)
                except httpx.HTTPError:
                    url = None
            item["link"] = url or item.get("google_link")

        await asyncio.gather(*(one(item) for item in items if item.get("id") or item.get("google_link")))


async def autocomplete(q: str, *, hl: str = "vi", gl: str = "vn") -> list[str]:
    async with _client() as c:
        resp = await c.get(
            "https://www.google.com/complete/search", params={"q": q, "client": "chrome", "hl": hl, "gl": gl.lower()}
        )
    _check(resp, "Google autocomplete")
    try:
        data = resp.json()
    except ValueError:
        raise UpstreamError("Google autocomplete trả JSON không hợp lệ.") from None
    texts = data[1] if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) else []
    return [str(t) for t in texts if isinstance(t, str) and t]


async def trending(geo: str = "VN", hl: str = "vi") -> list[dict[str, Any]]:
    async with _client() as c:
        resp = await c.get(f"{TRENDS_SITE}/trending/rss", params={"geo": geo.upper(), "hl": hl})
    _check(resp, "Google Trends RSS")
    try:
        root = ElementTree.fromstring(resp.content)
    except ElementTree.ParseError:
        raise UpstreamError("Google Trends trả RSS không hợp lệ.") from None
    out = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        out.append(
            {
                "title": title,
                "traffic": (item.findtext(f"{_TRENDS_NS}approx_traffic") or "").strip() or None,
                "published_at": _iso(item.findtext("pubDate")),
                "news": [
                    {
                        "title": (n.findtext(f"{_TRENDS_NS}news_item_title") or "").strip(),
                        "link": (n.findtext(f"{_TRENDS_NS}news_item_url") or "").strip() or None,
                        "source": (n.findtext(f"{_TRENDS_NS}news_item_source") or "").strip() or None,
                    }
                    for n in item.findall(f"{_TRENDS_NS}news_item")
                ],
            }
        )
    return out


async def _trends_api(c: httpx.AsyncClient, path: str, params: dict[str, Any], label: str) -> Any:
    url = f"{TRENDS_SITE}{path}"
    headers = {"referer": f"{TRENDS_SITE}/trends/explore"}
    resp = await c.get(url, params=params, headers=headers)
    if resp.status_code == 429:
        # Chưa có cookie NID: trang explore đặt cookie (chính nó cũng trả 429), rồi thử lại một lần.
        await c.get(f"{TRENDS_SITE}/trends/explore", params={"q": "google", "geo": "VN", "hl": "vi"})
        resp = await c.get(url, params=params, headers=headers)
    _check(resp, label)
    try:
        return json.loads(_strip_xssi(resp.text))
    except ValueError:
        raise UpstreamError(f"{label} trả JSON không hợp lệ.") from None


async def _explore(c: httpx.AsyncClient, keywords: list[str], geo: str, timeframe: str, hl: str) -> list[dict]:
    req = {
        "comparisonItem": [{"keyword": k, "geo": geo.upper(), "time": timeframe} for k in keywords],
        "category": 0,
        "property": "",
    }
    data = await _trends_api(
        c, "/trends/api/explore", {"hl": hl, "tz": 0, "req": json.dumps(req, separators=(",", ":"))}, "Trends explore"
    )
    widgets = data.get("widgets") if isinstance(data, dict) else None
    if not widgets:
        raise UpstreamError("Google Trends không trả widget nào.")
    return widgets


def _widget(widgets: list[dict], wid: str) -> dict | None:
    return next((w for w in widgets if w.get("id") == wid or str(w.get("id", "")).startswith(wid + "_")), None)


async def _widget_data(c: httpx.AsyncClient, widget: dict, endpoint: str, hl: str) -> Any:
    params = {
        "hl": hl,
        "tz": 0,
        "req": json.dumps(widget["request"], separators=(",", ":")),
        "token": widget["token"],
    }
    return await _trends_api(c, f"/trends/api/widgetdata/{endpoint}", params, f"Trends {endpoint}")


async def interest_over_time(
    keywords: list[str], *, geo: str = "VN", timeframe: str = "today 3-m", hl: str = "vi"
) -> dict[str, Any]:
    """Mức quan tâm tương đối 0-100 theo thời gian, tối đa 5 từ khoá cùng thang."""
    keywords = keywords[:5]
    async with _client() as c:
        widget = _widget(await _explore(c, keywords, geo, timeframe, hl), "TIMESERIES")
        if not widget:
            return {"keywords": keywords, "timeline": [], "averages": {}}
        data = await _widget_data(c, widget, "multiline", hl)
    default = data.get("default") or {}
    timeline = [
        {
            "time": int(point.get("time") or 0),
            "label": point.get("formattedTime"),
            "values": dict(zip(keywords, point.get("value") or [], strict=False)),
            "partial": bool(point.get("isPartial")),
        }
        for point in default.get("timelineData") or []
    ]
    return {
        "keywords": keywords,
        "timeframe": timeframe,
        "geo": geo,
        "timeline": timeline,
        "averages": dict(zip(keywords, default.get("averages") or [], strict=False)),
    }


async def related_queries(
    keyword: str, *, geo: str = "VN", timeframe: str = "today 3-m", hl: str = "vi"
) -> dict[str, list[dict[str, Any]]]:
    """Câu tìm liên quan: top (nhiều nhất) và rising (tăng nhanh)."""
    async with _client() as c:
        widget = _widget(await _explore(c, [keyword], geo, timeframe, hl), "RELATED_QUERIES")
        if not widget:
            return {"top": [], "rising": []}
        data = await _widget_data(c, widget, "relatedsearches", hl)
    lists = (data.get("default") or {}).get("rankedList") or []

    def rows(section: dict) -> list[dict[str, Any]]:
        return [
            {"query": r.get("query"), "value": r.get("value"), "formatted": r.get("formattedValue")}
            for r in section.get("rankedKeyword") or []
            if r.get("query")
        ]

    return {"top": rows(lists[0]) if lists else [], "rising": rows(lists[1]) if len(lists) > 1 else []}


class _maybe:
    """Dùng client của chỗ gọi nếu có, không thì mở client riêng và tự đóng."""

    def __init__(self, client: httpx.AsyncClient | None) -> None:
        self._given = client
        self._own: httpx.AsyncClient | None = None

    async def __aenter__(self) -> httpx.AsyncClient:
        if self._given is not None:
            return self._given
        self._own = _client()
        return self._own

    async def __aexit__(self, *exc: object) -> None:
        if self._own is not None:
            await self._own.aclose()
