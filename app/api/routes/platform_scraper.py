"""Route kích hoạt POST /<platform>/run dùng chung - mọi router theo nền tảng (xem
app/api/routes/facebook.py) có đúng cùng hợp đồng (kích hoạt theo keyword_id, theo
movie_id, hoặc "mọi từ khoá đang bật của nền tảng này"), dựng một lần ở đây thay vì
chép lại cho từng file nền tảng. File nền tảng chỉ cần gọi
build_run_route(router, "<platform>") rồi thêm phần riêng của nền tảng nếu cần (xem
refresh-token/token-status trong facebook.py).

build_comments_run_route bên dưới cùng ý tưởng cho POST
/<platform>/posts/{post_id}/comments/run - dùng chung cho mọi nền tảng trong
COMMENTS_SPIDER_BY_PLATFORM của spider-hub (facebook, threads, tiktok - nền tảng nào
không có trong đó thì đơn giản là không gọi builder này)."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.clients.kafka import DEFAULT_COMMENTS_MAX_PAGES, publish_comments_crawl_request, publish_crawl_request
from app.core.errors import NotFoundError, UpstreamError
from app.core.logging import get_logger
from app.schemas.scraper import (
    JobStatus,
    RunCommentsResponse,
    RunScraperRequest,
    RunScraperResponse,
    StopScraperResponse,
)
from app.services.crawl_jobs import cancel_job, get_running_job, request_stop
from app.services.d1 import get_enabled_keywords, get_keyword, get_post

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
        # Không bị ẩn khi đang drain - một lần kích hoạt có chủ đích (nurture, comments) vẫn
        # chạy tiếp sau khi bấm Dừng và phải còn hiện ra để dừng được.
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
    """POST /<platform>/posts/{post_id}/comments/run - kích hoạt spider comment của
    spider-hub cho một bài (id trong D1, không phải id bài của nền tảng - xem
    get_post). Chỉ gọi cho nền tảng mà spider-hub thực sự có spider comment (xem
    COMMENTS_SPIDER_BY_PLATFORM trong crawl_request_consumer.py)."""

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
        # Trước đây bỏ qua lời gọi này khi reply_count đang lưu bằng 0 - đã bỏ vì mọi mapper
        # nền tảng (app/services/platforms.py) đều quy số đếm bị thiếu/không lấy được về 0
        # giống như số 0 thật (`payload.get(...) or 0`), nên số 0 ở đây không đáng tin là
        # "bài này thực sự không có comment" thay vì "lúc crawl chỉ là không lấy được số
        # đếm". Bỏ qua theo nó có nguy cơ mãi mãi không lấy comment của những bài thật ra có
        # comment.
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
