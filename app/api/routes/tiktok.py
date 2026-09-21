"""TikTok scraper routes: POST /tiktok/run (shared trigger contract - see
platform_scraper.py), GET/POST /tiktok/keywords (see keywords.py), the
restore-session / import-cookies / token-status trio (see token_refresh.py
- same dashboard flow as Facebook/Threads, but spider-hub recaptures
device_id/odin_id instead of a GraphQL token cache),
POST /tiktok/posts/{post_id}/comments/run, and
POST /tiktok/channels/run (channel_videos spider for one @username).
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_comments_run_route, build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes
from app.core.logging import get_logger
from app.schemas.scraper import RunChannelVideosRequest, RunChannelVideosResponse
from app.services.kafka import publish_channel_videos_request

logger = get_logger(__name__)

router = APIRouter(prefix="/tiktok", tags=["tiktok"])
build_run_route(router, "tiktok")
build_keyword_routes(router, "tiktok")
build_token_refresh_routes(router, "tiktok")
build_comments_run_route(router, "tiktok")


@router.post("/channels/run", response_model=RunChannelVideosResponse)
async def run_channel_videos(body: RunChannelVideosRequest) -> RunChannelVideosResponse:
    published = await publish_channel_videos_request(
        username=body.username,
        max_pages=body.max_pages,
        keyword_id=body.keyword_id,
    )
    logger.info(
        "channel_videos_run_triggered",
        username=body.username.lstrip("@").strip(),
        max_pages=body.max_pages,
        published=published,
    )
    return RunChannelVideosResponse(published=published)
