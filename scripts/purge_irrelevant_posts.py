"""Chạy tay lượt dọn bài không liên quan hằng ngày - đúng cleanup.run_purge mà scheduler
của API gọi lúc run_time trên dashboard: grace_hours lưu trên dashboard, một dòng
cleanup_run_history, và khoá dùng chung, nên không bao giờ chạy chồng lên lượt theo
lịch.

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
    return await run_purge(triggered_by="manual")


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
