"""Danh sách tham khảo (chỉ đọc) các job còn chạy bằng crontab của hệ điều hành
(scripts/refresh_token.sh của spider-hub) - không có gì ở đây được lưu trong
database, nên đây chỉ là một danh sách cố định cộng với, nếu có file log local,
thời điểm sửa đổi cuối của file đó làm tín hiệu "lần chạy gần nhất". Không sửa được
từ đây - xem `source` của từng job để biết chỗ sửa.

Lịch crawl theo nền tảng (Facebook/Threads/TikTok - trước đây lẫn lộn giữa crontab
riêng của cinemark-api và Cloudflare Cron Triggers của cinemark-scraper, chỗ nào
cũng phải deploy mới sửa được) đã chuyển sang app/services/scheduler.py, một bộ lập
lịch hằng ngày chạy trong tiến trình, điều khiển bằng thẻ "Crawl schedule" trên
dashboard (GET/PUT /settings/crawl-schedule) - đó là tài nguyên thật lưu trong DB và
sửa được từ dashboard, không phải danh sách cố định như file này, nên không liệt kê
ở đây."""

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
