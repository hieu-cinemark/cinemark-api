"""Các route scraper TikTok: POST /tiktok/run (hợp đồng kích hoạt dùng chung - xem
platform_scraper.py), GET/POST /tiktok/keywords (xem keywords.py), bộ ba
restore-session / import-cookies / token-status (xem token_refresh.py - cùng luồng
dashboard với Facebook/Threads, nhưng spider-hub lấy lại device_id/odin_id thay vì
cache token GraphQL), POST /tiktok/posts/{post_id}/comments/run, và
POST /tiktok/channels/run (spider channel_videos cho một @username).
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_comments_run_route, build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes
from app.clients.kafka import publish_channel_videos_request
from app.core.logging import get_logger
from app.schemas.scraper import RunChannelVideosRequest, RunChannelVideosResponse

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
