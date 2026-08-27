"""Shared POST /<platform>/run trigger route - every platform-scoped router
(see app/api/routes/facebook.py) gets the exact same contract (trigger by
keyword_id, by movie_id, or "every enabled keyword for this platform") built
once here instead of copy-pasted per platform file. A platform file only
needs to call build_run_route(router, "<platform>") and add whatever
platform-specific extras it needs on top (see facebook.py's
refresh-token/token-status)."""

from __future__ import annotations

from fastapi import APIRouter

from app.core.errors import NotFoundError
from app.core.logging import get_logger
from app.schemas.scraper import RunScraperRequest, RunScraperResponse
from app.services.d1 import get_enabled_keywords, get_keyword
from app.services.kafka import publish_crawl_request

logger = get_logger(__name__)


def build_run_route(router: APIRouter, platform: str) -> None:
    @router.post("/run", response_model=RunScraperResponse)
    async def run_scraper(payload: RunScraperRequest = RunScraperRequest()) -> RunScraperResponse:
        if payload.keyword_id is not None:
            keyword = await get_keyword(payload.keyword_id, platform=platform)
            if keyword is None:
                raise NotFoundError(f"No enabled {platform} keyword {payload.keyword_id}")
            keywords = [keyword]
        else:
            keywords = await get_enabled_keywords(platform=platform, movie_id=payload.movie_id)

        if not keywords:
            logger.info(
                "scraper_run_no_keywords", platform=platform, keyword_id=payload.keyword_id, movie_id=payload.movie_id
            )
            return RunScraperResponse(requested=0, published=0)

        published = 0
        for keyword in keywords:
            ok = await publish_crawl_request(
                platform=platform,
                keyword=keyword["keyword"],
                keyword_id=keyword["id"],
                max_pages=payload.max_pages,
                start_date=payload.start_date,
                end_date=payload.end_date,
            )
            if ok:
                published += 1

        logger.info(
            "scraper_run_triggered",
            platform=platform,
            requested=len(keywords),
            published=published,
            keyword_id=payload.keyword_id,
            movie_id=payload.movie_id,
        )
        return RunScraperResponse(requested=len(keywords), published=published)
