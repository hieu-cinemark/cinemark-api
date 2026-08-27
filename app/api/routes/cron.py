"""Read-only reference for the cron jobs that drive this pipeline - they
live scattered across a crontab (spider-hub's scripts/refresh_token.sh),
another crontab (cinemark-api's scripts/trigger_scheduled_crawl.sh), and
cinemark-scraper's own Cloudflare Cron Triggers (wrangler.toml) - nothing
here is stored in a database, so this is just a static list plus, where a
local log file exists, that file's last-modified time as a "last run"
signal. Not editable from here - see each job's `source` to change it."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from app.core.config import settings
from app.schemas.settings import CronJob

router = APIRouter(prefix="/cron", tags=["cron"])


def _mtime(path: Path) -> datetime | None:
    if not path.is_file():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


@router.get("/jobs", response_model=list[CronJob])
async def cron_jobs() -> list[CronJob]:
    spider_hub_root = Path(settings.spider_hub_consumer_log_path).parent
    refresh_token_log = spider_hub_root / "scripts" / "refresh_token.log"

    return [
        CronJob(
            name="Facebook token refresh",
            schedule="every 4h (0 */4 * * *)",
            source="spider-hub/scripts/refresh_token.sh",
            description="Headlessly refreshes the Facebook session token cache before CACHE_MAX_AGE_SECONDS expires.",
            last_run_at=_mtime(refresh_token_log),
        ),
        CronJob(
            name="Scheduled crawl trigger",
            schedule="every 6h (0 */6 * * *)",
            source="cinemark-api/scripts/trigger_scheduled_crawl.sh",
            description="Publishes crawl_requests for every enabled Facebook keyword via POST /facebook/run.",
        ),
        CronJob(
            name="Threads scrape",
            schedule="every 6h at :30 (30 */6 * * *)",
            source="cinemark-scraper Worker - Cloudflare Cron Trigger (wrangler.toml)",
            description="runScrapeJob(db, env, 'cron', { platform: 'threads' }) inside the Worker's scheduled() handler.",
        ),
        CronJob(
            name="TikTok scrape",
            schedule="every 8h (0 */8 * * *)",
            source="cinemark-scraper Worker - Cloudflare Cron Trigger (wrangler.toml)",
            description="runScrapeJob(db, env, 'cron', { platform: 'tiktok' }) inside the Worker's scheduled() handler.",
        ),
    ]
