"""Compares today's post count per platform against the median of the
previous 7 days - catches "the crawl ran and reported success, but the
platform's response shape silently changed and extraction now returns way
less/more than normal" cases that the error-based alerts elsewhere in this
project can't see (no exception is ever raised in that scenario).

Run daily via cron, right after the scheduled crawl trigger:
    python -m scripts.check_volume_anomaly
"""

from __future__ import annotations

import asyncio
import statistics
from collections import defaultdict
from datetime import date

from app.core.logging import get_logger
from app.services.d1 import get_post_timeseries
from app.services.telegram import send_telegram_message

logger = get_logger(__name__)

BASELINE_DAYS = 7
# Below this fraction of the baseline median -> "did this platform quietly
# break". Above this multiple -> "is this a spam/bug spike". Both are just
# starting points - tune once you've seen a week of real data.
LOW_RATIO = 0.3
HIGH_RATIO = 3.0
MIN_BASELINE_TO_ALERT = 5  # a platform with 1-2 posts/day is too noisy to alert on ratio alone


async def check() -> None:
    rows = await get_post_timeseries(BASELINE_DAYS + 1)
    by_platform: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        by_platform[row["platform"]][row["day"]] = row["count"]

    today = date.today().isoformat()

    for platform, counts_by_day in by_platform.items():
        today_count = counts_by_day.get(today, 0)
        baseline_counts = [c for day, c in counts_by_day.items() if day != today]
        if len(baseline_counts) < 3:
            continue  # not enough history yet to judge

        baseline = statistics.median(baseline_counts)
        if baseline < MIN_BASELINE_TO_ALERT:
            continue

        ratio = today_count / baseline
        if ratio < LOW_RATIO:
            await send_telegram_message(
                f"⚠️ Volume anomaly: {platform} got only {today_count} posts today "
                f"vs a {baseline:.0f}/day baseline ({ratio:.0%}) - check if extraction silently broke."
            )
        elif ratio > HIGH_RATIO:
            await send_telegram_message(
                f"⚠️ Volume anomaly: {platform} got {today_count} posts today "
                f"vs a {baseline:.0f}/day baseline ({ratio:.0%}) - check for spam/duplicate crawls."
            )
        logger.info("volume_check", platform=platform, today=today_count, baseline=baseline, ratio=round(ratio, 2))


if __name__ == "__main__":
    asyncio.run(check())
