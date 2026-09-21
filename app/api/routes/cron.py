"""Read-only reference for the few jobs still driven by a plain OS crontab
(spider-hub's scripts/refresh_token.sh) - nothing here is stored in a
database, so this is just a static list plus, where a local log file
exists, that file's last-modified time as a "last run" signal. Not editable
from here - see each job's `source` to change it.

Platform crawl scheduling (Facebook/Threads/TikTok - previously a mix of
cinemark-api's own crontab and cinemark-scraper's Cloudflare Cron Triggers,
none of it editable without a deploy) moved to app/services/scheduler.py,
an in-process daily scheduler driven by the dashboard's "Crawl schedule"
card (GET/PUT /settings/crawl-schedule) - that one's a real DB-backed,
dashboard-editable resource, not a fixed list like this file, so it isn't
listed here."""

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
            name="Account health / volume anomaly / AI topic reports",
            schedule="every 6h (0 */6 * * *)",
            source="cinemark-api/scripts/trigger_scheduled_crawl.sh",
            description="No longer triggers crawls (see app/services/scheduler.py) - just the account health "
            "check (every cycle), volume-anomaly check (hour 18), and AI topic report regeneration (hour 20).",
        ),
    ]
