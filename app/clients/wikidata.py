"""Wikidata SPARQL (query.wikidata.org), không key (25/11/2026). Dữ liệu có cấu trúc là CC0; ảnh (P18, Wikimedia Commons)
có giấy phép riêng từng ảnh nên không trả ở đây.

Giới hạn của Wikidata: mỗi truy vấn tối đa 60 giây, tối đa 5 truy vấn song song mỗi IP - truy vấn gộp toàn thế giới bị timeout,
nên luôn lọc theo một quốc gia. Danh sách lấy theo 2 bước: (1) trang id phim theo quốc gia + năm, (2) chi tiết các id đó
bằng VALUES - nhanh hơn nhiều so với gom mọi trường trong một truy vấn.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.core.errors import UpstreamError

SPARQL_URL = "https://query.wikidata.org/sparql"
# Wikimedia yêu cầu User-Agent mô tả được client; không gửi thông tin cá nhân.
USER_AGENT = "CinemarkMetadata/1.0 (movie metadata lookup; httpx)"
TIMEOUT_SECONDS = 65.0
FILM = "wd:Q11424"
# Wikidata cho tối đa 5 truy vấn song song mỗi IP; giữ dưới mức đó trong một tiến trình.
_slots = asyncio.Semaphore(3)


async def sparql(query: str) -> list[dict[str, Any]]:
    """Chạy một truy vấn SPARQL, trả danh sách binding đã rút gọn {tên biến: giá trị}."""
    async with _slots:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SECONDS)) as client:
                response = await client.get(
                    SPARQL_URL,
                    params={"query": query},
                    headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT},
                )
        except httpx.TimeoutException:
            raise UpstreamError("Wikidata query timed out") from None
        except httpx.HTTPError:
            raise UpstreamError("Could not reach Wikidata") from None
    if response.status_code == 429:
        raise UpstreamError("Wikidata is rate limiting requests, try again shortly")
    if response.status_code >= 500:
        raise UpstreamError("Wikidata query timed out or failed (narrow the country/year range)")
    if response.status_code >= 400:
        raise UpstreamError(f"Wikidata rejected the query (HTTP {response.status_code})")
    try:
        bindings = response.json()["results"]["bindings"]
    except ValueError, KeyError:
        raise UpstreamError("Wikidata returned an invalid response") from None
    return [{key: value.get("value") for key, value in row.items()} for row in bindings]


def _qid(uri: str | None) -> str | None:
    return uri.rsplit("/", 1)[-1] if uri else None


def _split(value: str | None) -> list[str]:
    return [item for item in (value or "").split("|") if item]


async def movies_by_country(
    country: str, *, year_from: int | None, year_to: int | None, limit: int, offset: int, lang: str
) -> list[dict[str, Any]]:
    """Phim của một quốc gia (mã ISO 3166-1 alpha-2, vd VN, US, KR), mới phát hành trước, kèm chi tiết."""
    filters = []
    if year_from:
        filters.append(f"YEAR(?d) >= {int(year_from)}")
    if year_to:
        filters.append(f"YEAR(?d) <= {int(year_to)}")
    date_filter = f"FILTER({' && '.join(filters)})" if filters else ""
    page = await sparql(
        f"""SELECT ?m (MIN(?d) AS ?released) WHERE {{
  ?c wdt:P297 "{country}" .
  ?m wdt:P495 ?c ; wdt:P31 {FILM} .
  OPTIONAL {{ ?m wdt:P577 ?d . }}
  {date_filter}
}} GROUP BY ?m ORDER BY DESC(?released) LIMIT {int(limit)} OFFSET {int(offset)}"""
    )
    ids = [_qid(row["m"]) for row in page if row.get("m")]
    if not ids:
        return []
    details = await movie_details(ids, lang=lang)
    by_id = {item["wikidata_id"]: item for item in details}
    return [by_id[i] for i in ids if i in by_id]


async def movie_details(ids: list[str], *, lang: str) -> list[dict[str, Any]]:
    """Chi tiết các phim theo QID: tên, tên gốc, ngày phát hành, quốc gia, thể loại, đạo diễn, diễn viên, thời lượng, ID ngoài."""
    values = " ".join(f"wd:{i}" for i in ids)
    langs = ",".join(part.strip() for part in lang.split(",") if part.strip()) or "en"

    def labels(prop: str, var: str) -> str:
        return f"""OPTIONAL {{ SELECT ?m (GROUP_CONCAT(DISTINCT ?{var}L; separator="|") AS ?{var}) WHERE {{
      VALUES ?m {{ {values} }} ?m wdt:{prop} ?{var}X .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{langs}". ?{var}X rdfs:label ?{var}L . }}
    }} GROUP BY ?m }}"""

    rows = await sparql(
        f"""SELECT ?m ?mLabel ?title ?released ?duration ?imdb ?tmdb ?countries ?genres ?directors ?cast ?companies WHERE {{
  VALUES ?m {{ {values} }}
  OPTIONAL {{ SELECT ?m (SAMPLE(?t) AS ?title) WHERE {{ VALUES ?m {{ {values} }} ?m wdt:P1476 ?t }} GROUP BY ?m }}
  OPTIONAL {{ SELECT ?m (MIN(?d) AS ?released) WHERE {{ VALUES ?m {{ {values} }} ?m wdt:P577 ?d }} GROUP BY ?m }}
  OPTIONAL {{ SELECT ?m (SAMPLE(?x) AS ?duration) WHERE {{ VALUES ?m {{ {values} }} ?m wdt:P2047 ?x }} GROUP BY ?m }}
  OPTIONAL {{ SELECT ?m (SAMPLE(?x) AS ?imdb) WHERE {{ VALUES ?m {{ {values} }} ?m wdt:P345 ?x }} GROUP BY ?m }}
  OPTIONAL {{ SELECT ?m (SAMPLE(?x) AS ?tmdb) WHERE {{ VALUES ?m {{ {values} }} ?m wdt:P4947 ?x }} GROUP BY ?m }}
  {labels("P495", "countries")}
  {labels("P136", "genres")}
  {labels("P57", "directors")}
  {labels("P161", "cast")}
  {labels("P272", "companies")}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{langs}". }}
}}"""
    )
    return [
        {
            "wikidata_id": _qid(row.get("m")),
            "name": row.get("mLabel"),
            "original_title": row.get("title"),
            "released_at": (row.get("released") or "")[:10] or None,
            "duration_minutes": round(float(row["duration"])) if row.get("duration") else None,
            "imdb_id": row.get("imdb"),
            "tmdb_id": row.get("tmdb"),
            "countries": _split(row.get("countries")),
            "genres": _split(row.get("genres")),
            "directors": _split(row.get("directors")),
            "cast": _split(row.get("cast")),
            "companies": _split(row.get("companies")),
        }
        for row in rows
    ]
