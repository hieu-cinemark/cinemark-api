"""Google miễn phí (app/clients/google.py): parse RSS / batchexecute, RSS thay SerpApi cho Google News."""

import asyncio
import json

from app.clients import google as g
from app.schemas.research import GoogleNewsRequest
from app.services import google_news

RSS = """<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t</title>
<item><title>Review Mưa Đỏ: xúc động - VnExpress</title>
<link>https://news.google.com/rss/articles/CBMiAAA?oc=5</link>
<pubDate>Sun, 24 Aug 2025 07:00:00 GMT</pubDate><source url="https://vnexpress.net">VnExpress</source></item>
<item><title>Không nguồn - Báo X</title><link>https://news.google.com/rss/articles/CBMiBBB</link></item>
</channel></rss>"""


def test_parse_news_rss():
    items = g.parse_news_rss(RSS)
    assert items[0] == {
        "id": "CBMiAAA",
        "title": "Review Mưa Đỏ: xúc động",
        "google_link": "https://news.google.com/rss/articles/CBMiAAA?oc=5",
        "source": "VnExpress",
        "source_url": "https://vnexpress.net",
        "published_at": "2025-08-24T07:00:00+00:00",
    }
    assert items[1]["source"] == "Báo X" and items[1]["published_at"] is None


def test_parse_batchexecute_link():
    inner = json.dumps(["garturlres", "https://bao.vn/bai-1", 1])
    body = ")]}'\n\n123\n" + json.dumps([["wrb.fr", "Fbv4je", inner, None, None, None, "generic"]]) + "\n"
    assert g.parse_batchexecute_link(body) == "https://bao.vn/bai-1"
    assert g.parse_batchexecute_link(")]}'\n[]") is None


def test_strip_xssi_both_forms():
    assert g._strip_xssi(')]}\'\n{"a":1}') == '{"a":1}'
    assert g._strip_xssi(')]}\',\n{"a":1}') == '{"a":1}'
    assert g._strip_xssi('{"a":1}') == '{"a":1}'


def test_search_rss_pages_like_serpapi(monkeypatch):
    items = [
        {"id": f"id{i}", "title": f"t{i}", "google_link": f"https://news.google.com/rss/articles/id{i}", "source": "S"}
        for i in range(23)
    ]

    async def fake_search(q, **_):
        return [dict(item) for item in items]

    async def fake_resolve(page):
        for item in page:
            item["link"] = f"https://bao.vn/{item['id']}"

    monkeypatch.setattr(g, "news_search", fake_search)
    monkeypatch.setattr(g, "resolve_links", fake_resolve)
    first = asyncio.run(google_news.search_rss(GoogleNewsRequest(q="phim")))
    assert [r["position"] for r in first["news_results"]] == list(range(1, 11))
    assert first["news_results"][0]["link"] == "https://bao.vn/id0"
    assert first["search_metadata"]["status"] == "Success" and first["serpapi_pagination"]
    last = asyncio.run(google_news.search_rss(GoogleNewsRequest(q="phim", start=20)))
    assert len(last["news_results"]) == 3 and "serpapi_pagination" not in last
