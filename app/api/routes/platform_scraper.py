"""Shared POST /<platform>/run trigger route - every platform-scoped router
(see app/api/routes/facebook.py) gets the exact same contract (trigger by
keyword_id, by movie_id, or "every enabled keyword for this platform") built
once here instead of copy-pasted per platform file. A platform file only
needs to call build_run_route(router, "<platform>") and add whatever
platform-specific extras it needs on top (see facebook.py's
refresh-token/token-status).

build_comments_run_route below is the same idea for POST
/<platform>/posts/{post_id}/comments/run - shared by every platform in
spider-hub's own COMMENTS_SPIDER_BY_PLATFORM (facebook, threads, tiktok -
a platform not in it simply doesn't call this builder at all)."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.core.errors import NotFoundError, UpstreamError
from app.core.logging import get_logger
from app.schemas.scraper import JobStatus, RunCommentsResponse, RunScraperRequest, RunScraperResponse, StopScraperResponse
from app.services.crawl_jobs import cancel_job, get_running_job, is_platform_draining, request_stop
from app.services.d1 import get_enabled_keywords, get_keyword, get_post
from app.services.kafka import DEFAULT_COMMENTS_MAX_PAGES, publish_comments_crawl_request, publish_crawl_request

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
                bfs_depth=payload.bfs_depth if payload.keyword_id is not None else None,
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

    @router.get("/job-status", response_model=JobStatus)
    async def job_status() -> JobStatus:
        if await is_platform_draining(platform):
            return JobStatus(running=False)
        job = await get_running_job(platform)
        if job is None:
            return JobStatus(running=False)
        return JobStatus(
            running=True,
            keyword=job.get("keyword"),
            keyword_id=job.get("keyword_id"),
            started_at=job.get("started_at"),
            type=job.get("type"),
            account=job.get("account"),
            post_id=job.get("post_id"),
            username=job.get("username"),
        )

    @router.post("/stop", response_model=StopScraperResponse)
    async def stop_scraper() -> StopScraperResponse:
        stopped = await request_stop(platform)
        logger.info("scraper_stop_requested", platform=platform, stopped=stopped)
        return StopScraperResponse(stopped=stopped)

    @router.post("/jobs/{run_id}/stop", response_model=StopScraperResponse)
    async def stop_job(run_id: str) -> StopScraperResponse:
        stopped = await cancel_job(platform, run_id)
        logger.info("scraper_job_stop_requested", platform=platform, run_id=run_id, stopped=stopped)
        return StopScraperResponse(stopped=stopped)


def build_comments_run_route(router: APIRouter, platform: str) -> None:
    """POST /<platform>/posts/{post_id}/comments/run - triggers spider-hub's
    comments spider for one post (D1 id, not the platform's own post id -
    see get_post). Only call this for a platform spider-hub actually has a
    comments spider for (see crawl_request_consumer.py's
    COMMENTS_SPIDER_BY_PLATFORM)."""

    @router.post("/posts/{post_id}/comments/run", response_model=RunCommentsResponse)
    async def run_comments(
        post_id: str,
        max_pages: int = Query(default=DEFAULT_COMMENTS_MAX_PAGES, ge=1, le=500),
        bypass_drain: bool = Query(
            default=True,
            description="True (default): a one-off single-post trigger the platform's Stop "
            "button must not silently swallow. The dashboard's bulk row-selection action "
            "passes false instead - see publish_comments_crawl_request's own docstring for "
            "why publishing MANY of these at once needs to be cancellable, unlike a single "
            "deliberate click.",
        ),
    ) -> RunCommentsResponse:
        post = await get_post(post_id)
        if post is None:
            raise NotFoundError(f"No post {post_id}")
        if post["platform"] != platform:
            raise UpstreamError(f"Post {post_id} is not a {platform} post")
        if not post.get("url"):
            raise UpstreamError(f"Post {post_id} has no stored url - can't bootstrap a comments crawl without one")
        # Used to skip this call when the stored reply_count was 0 - removed
        # because every platform mapper (app/services/platforms.py) coalesces
        # a missing/uncaptured count to 0 the same as a real zero
        # (`payload.get(...) or 0`), so a 0 here can't be trusted to mean
        # "this post structurally has no comments" rather than "the count
        # just wasn't captured at scrape time". Skipping on it risked
        # permanently never fetching comments for posts that actually have
        # them.
        published = await publish_comments_crawl_request(
            platform=platform,
            post_external_id=post["external_id"],
            post_url=post["url"],
            max_pages=max_pages,
            bypass_drain=bypass_drain,
        )
        logger.info(
            "comments_run_triggered",
            platform=platform,
            post_id=post_id,
            max_pages=max_pages,
            bypass_drain=bypass_drain,
            published=published,
        )
        return RunCommentsResponse(published=published)
