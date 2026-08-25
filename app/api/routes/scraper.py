"""Triggers a scrape crawl by publishing to Kafka (see app/services/kafka.py)
- spider-hub's crawl_request_consumer.py picks these up and runs the actual
`scrapy crawl`. One endpoint serves both the manual "run" button (FE passes
keyword_id or movie_id) and the daily cron job (scripts/trigger_daily_crawl.sh
calls it with an empty body) - same code path, same contract, no separate
"scheduled run" logic to keep in sync with the manual one.

A single-keyword call (keyword_id set - the only shape the Platforms page's
"Chạy tìm kiếm" button sends) also opens a ScrapeRun row and threads its id
through the Kafka payload: spider-hub writes each subprocess stdout line
straight to scrape_run_logs against that id and flips the run to
completed/failed when the subprocess exits (see crawl_request_consumer.py),
so GET /scraper/runs/{id}/logs below can serve cinemark-web a poll-based
near-live tail without a second Kafka topic/consumer.

Multi-keyword calls (movie_id-only, or the daily cron's empty body) don't
get a ScrapeRun: each keyword becomes its own Kafka message, processed one
at a time by spider-hub, and there is no single moment "the run" finishes -
spider-hub would end up flipping one shared run to completed the instant
the *first* keyword's subprocess exits, while the rest are still running."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import NotFoundError
from app.db.session import get_db
from app.models.enums import Platform, RunStatus, RunTrigger
from app.models.keyword import Keyword
from app.models.scrape_run import ScrapeRun
from app.schemas.scraper import (
    ActiveRunResponse,
    FacebookTokenStatus,
    RunScraperRequest,
    RunScraperResponse,
    ScrapeRunLogsResponse,
    TriggerTokenRefreshResponse,
)
from app.services.facebook_token import get_facebook_token_status
from app.services.kafka import publish_crawl_request, publish_token_refresh_request
from app.services.keywords import KeywordRepository
from app.services.scrape_runs import ScrapeRunRepository

router = APIRouter(prefix="/scraper", tags=["scraper"])


@router.post("/run", response_model=RunScraperResponse)
async def run_scraper(
    payload: RunScraperRequest = RunScraperRequest(), db: AsyncSession = Depends(get_db)
) -> RunScraperResponse:
    repo = KeywordRepository(db)

    keywords: list[Keyword]
    if payload.keyword_id is not None:
        keyword = await repo.get(payload.keyword_id)
        if keyword is None or not keyword.enabled:
            raise NotFoundError(f"No enabled keyword {payload.keyword_id}")
        keywords = [keyword]
    else:
        keywords = await repo.list_enabled(movie_id=payload.movie_id)

    if not keywords:
        return RunScraperResponse(requested=0, published=0)

    run_repo = ScrapeRunRepository(db)
    run = None
    if payload.keyword_id is not None:
        run = await run_repo.create(
            status=RunStatus.RUNNING,
            triggered_by=RunTrigger.MANUAL,
            keyword=keywords[0].keyword,
            platform=keywords[0].platform,
        )

    published = 0
    for keyword in keywords:
        ok = await publish_crawl_request(
            platform=keyword.platform,
            keyword=keyword.keyword,
            keyword_id=keyword.id,
            run_id=run.id if run is not None else None,
            max_pages=payload.max_pages,
            start_date=payload.start_date,
            end_date=payload.end_date,
        )
        if ok:
            published += 1

    if run is not None and published == 0:
        # Nothing actually made it to Kafka - spider-hub will never touch
        # this run, so it must not sit at RUNNING forever with a FE polling
        # its logs and never seeing a terminal status.
        await run_repo.update(
            run,
            status=RunStatus.FAILED,
            finished_at=datetime.now(timezone.utc),
            error="Không publish được yêu cầu nào lên Kafka.",
        )

    return RunScraperResponse(requested=len(keywords), published=published, run_id=run.id if run is not None else None)


@router.post("/refresh-token", response_model=TriggerTokenRefreshResponse)
async def refresh_facebook_token(db: AsyncSession = Depends(get_db)) -> TriggerTokenRefreshResponse:
    """Manual trigger for the same Facebook session-token refresh that
    already runs on spider-hub's 4h cron (scripts/refresh_token.sh) - for
    when the cached token expires between ticks and someone doesn't want to
    wait or SSH in to run the script by hand."""
    run_repo = ScrapeRunRepository(db)
    run = await run_repo.create(
        status=RunStatus.RUNNING, triggered_by=RunTrigger.MANUAL, platform=Platform.FACEBOOK
    )

    published = await publish_token_refresh_request(run_id=run.id)
    if not published:
        await run_repo.update(
            run,
            status=RunStatus.FAILED,
            finished_at=datetime.now(timezone.utc),
            error="Không publish được yêu cầu nào lên Kafka.",
        )

    return TriggerTokenRefreshResponse(run_id=run.id)


@router.get("/facebook-token-status", response_model=FacebookTokenStatus)
async def facebook_token_status() -> FacebookTokenStatus:
    """Whether spider-hub's cached Facebook session (see graphql_client.py's
    FacebookGraphQLClient there) is still usable - the cache key's own
    Redis TTL already encodes this (bootstrap.py sets it equal to
    CACHE_MAX_AGE_SECONDS), so "does the key still exist" *is* "is it
    valid", no separate age calculation needed."""
    account, ttl = await get_facebook_token_status()
    return FacebookTokenStatus(valid=ttl is not None, account=account, expires_in_seconds=ttl)


@router.get("/active-run", response_model=ActiveRunResponse)
async def get_active_run(db: AsyncSession = Depends(get_db)) -> ActiveRunResponse:
    """The most recently started ScrapeRun still at status=running, if any -
    lets any page (not just the one that happened to trigger it) find and
    tail whatever crawl is currently in flight."""
    stmt = (
        select(ScrapeRun)
        .where(ScrapeRun.status == RunStatus.RUNNING)
        .order_by(ScrapeRun.started_at.desc())
        .limit(1)
    )
    run = (await db.execute(stmt)).scalar_one_or_none()
    if run is None:
        return ActiveRunResponse(run_id=None)
    return ActiveRunResponse(run_id=run.id, keyword=run.keyword, platform=run.platform)


@router.get("/runs/{run_id}/logs", response_model=ScrapeRunLogsResponse)
async def get_run_logs(
    run_id: uuid.UUID, after: datetime | None = None, db: AsyncSession = Depends(get_db)
) -> ScrapeRunLogsResponse:
    run_repo = ScrapeRunRepository(db)
    run = await run_repo.get_or_404(run_id)
    logs = await run_repo.list_logs(run_id, after=after)
    return ScrapeRunLogsResponse(status=run.status, keyword=run.keyword, platform=run.platform, logs=logs)
