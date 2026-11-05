"""Gán audience_stage='hong' cho comment cũ chỉ có tag tên hoặc icon (2026-11-05), như ingest giờ làm cho comment mới.

Chỉ đụng dòng có sentiment_source='rule' mà giai đoạn còn trống hoặc 'khac', không đè nhãn 'hong'/'da_xem' đã có. Tiếng cười
và câu quá ngắn giữ nguyên. Đọc theo id, con trỏ lưu trong file nên dừng rồi chạy lại được. Chạy giờ vắng.

    python -m scripts.tools.backfill_tag_icon_stage --dry-run
    python -m scripts.tools.backfill_tag_icon_stage
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from app.ai.tasks.sentiment_rules import is_tag_or_icon
from app.repositories.d1.store import d1_query

CURSOR_FILE = Path(__file__).resolve().parents[2] / "data" / "backfill_tag_icon_stage.cursor"
PAGE_SIZE = 2000
UPDATE_CHUNK = 90  # D1 tối đa 100 tham số mỗi câu
PAUSE_SECONDS = 1.0
OVERLOAD_PAUSE_SECONDS = 60.0


def ids_to_update(rows: list[dict[str, Any]]) -> list[str]:
    return [row["id"] for row in rows if row.get("audience_stage") in (None, "", "khac") and is_tag_or_icon(row.get("message"))]


async def main(args: argparse.Namespace) -> None:
    last_id = CURSOR_FILE.read_text().strip() if CURSOR_FILE.exists() else ""
    read = updated = 0
    while True:
        rows = await d1_query(
            "SELECT id, message, audience_stage FROM comments WHERE id > ? AND sentiment_source = 'rule' ORDER BY id LIMIT ?",
            [last_id, PAGE_SIZE],
            timeout=60.0,
            quiet=True,
        )
        if rows is None:
            print(f"đọc lỗi, nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
            await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)
            continue
        if not rows:
            break
        ids = ids_to_update(rows)
        if args.dry_run:
            print(f"lô đầu: {len(rows)} dòng rule, {len(ids)} dòng sẽ thành 'hong'")
            for row in rows[:12]:
                print(row["id"] in ids, repr((row.get("message") or "")[:50]))
            return
        for i in range(0, len(ids), UPDATE_CHUNK):
            chunk = ids[i : i + UPDATE_CHUNK]
            sql = f"UPDATE comments SET audience_stage = 'hong' WHERE id IN ({','.join('?' * len(chunk))})"
            while await d1_query(sql, chunk, timeout=60.0, quiet=True) is None:
                print(f"ghi lỗi, nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
                await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)
        last_id = rows[-1]["id"]
        CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)
        CURSOR_FILE.write_text(last_id)
        read += len(rows)
        updated += len(ids)
        await asyncio.sleep(PAUSE_SECONDS)
    print(f"xong: {read} dòng rule, {updated} dòng thành 'hong'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    asyncio.run(main(parser.parse_args()))
