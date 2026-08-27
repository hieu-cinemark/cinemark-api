"""Shared GET/POST /<platform>/keywords routes - GET lists every enabled
keyword for a platform (with its movie's title) so the dashboard's
crawl-trigger form can offer a real keyword picker instead of only "every
enabled keyword"; POST lets that same form create a new one inline (typed
in the keyword select, not found in the list) without leaving the page.
Same shared-builder shape as platform_scraper.build_run_route /
token_refresh.build_token_refresh_routes."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.errors import UpstreamError, ValidationError
from app.schemas.scraper import KeywordOut
from app.services.d1 import get_or_create_keyword, list_keywords


class KeywordCreateRequest(BaseModel):
    movie_id: str
    keyword: str


def build_keyword_routes(router: APIRouter, platform: str) -> None:
    @router.get("/keywords", response_model=list[KeywordOut])
    async def keywords() -> list[KeywordOut]:
        rows = await list_keywords(platform)
        return [KeywordOut(**row) for row in rows]

    @router.post("/keywords", response_model=KeywordOut)
    async def create_keyword(payload: KeywordCreateRequest) -> KeywordOut:
        keyword = payload.keyword.strip()
        if not keyword:
            raise ValidationError("keyword must not be empty")
        row = await get_or_create_keyword(payload.movie_id, platform, keyword)
        if row is None:
            raise UpstreamError("Could not create keyword (D1 write failed or movie not found)")
        return KeywordOut(**row)
