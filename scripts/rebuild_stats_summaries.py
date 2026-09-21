"""One-shot rebuild of stats_platform_daily / stats_keyword_daily from
existing posts + comments on the configured D1 (remote). Run after first
deploy of the rollup tables, or anytime counts look drifted:

    cd cinemark-api && source .venv/bin/activate
    python -m scripts.rebuild_stats_summaries
"""

from __future__ import annotations

import asyncio
import time

from app.core.config import settings
from app.core.logging import get_logger
from app.services.stats_summary import rebuild_from_source

logger = get_logger(__name__)


async def main() -> None:
    print(f"db_mode={settings.db_mode}")
    t0 = time.perf_counter()
    counts = await rebuild_from_source()
    elapsed = time.perf_counter() - t0
    print(f"rebuilt platform_days={counts['platform_days']} keyword_days={counts['keyword_days']} in {elapsed:.1f}s")
    logger.info("stats_summaries_rebuilt", **counts, elapsed_seconds=round(elapsed, 1))


if __name__ == "__main__":
    asyncio.run(main())
