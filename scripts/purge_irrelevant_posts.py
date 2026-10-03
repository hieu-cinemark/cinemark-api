"""Runs the daily irrelevant-post purge by hand - the same cleanup.run_purge
the API's scheduler calls at the dashboard's run_time: dashboard-stored
grace_hours, a cleanup_run_history row, and the shared lock, so it never
overlaps a scheduled run.

    .venv/bin/python -m scripts.purge_irrelevant_posts --dry-run
    .venv/bin/python -m scripts.purge_irrelevant_posts
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.services.cleanup import purge_irrelevant_posts, resolve_cleanup_settings, run_purge


async def _main(dry_run: bool) -> dict | None:
    if dry_run:
        cfg = await resolve_cleanup_settings()
        return await purge_irrelevant_posts(dry_run=True, grace_hours=cfg["grace_hours"])
    return await run_purge(triggered_by="script")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="only count what would be deleted")
    args = parser.parse_args()
    result = asyncio.run(_main(args.dry_run))
    if result is None:
        print("Another purge run holds the lock - nothing done.")
        return
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
