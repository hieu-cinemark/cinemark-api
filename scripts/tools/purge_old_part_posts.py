"""Xoá bài của PHẦN CŨ khỏi các phim nhiều phần (19/11/2026, người vận hành yêu cầu): Shaun the Sheep (các phim trước), Thám Tử Kiên
(phần 1 "Kỳ án không đầu"). Dữ liệu cũ làm sai Heat, Social Topic, số bài.

Quy tắc mỗi phim (OLD_PART_RULES): bài đăng TRƯỚC mốc `cutoff` VÀ không nhắc dấu hiệu của phần mới (`markers`, so khớp không dấu,
không phân biệt hoa thường) thì là phần cũ. Bài trước mốc nhưng đã nhắc phần mới (tin báo phần 2, trailer, quay phim...) được giữ.
Mốc chọn theo dữ liệu: Thám Tử Kiên phần 2 được công bố khoảng 11/2025; Shaun phim mới ra trailer từ 03/2026.

Xoá cùng cách purge_irrelevant_posts: SAO LƯU lên R2 trước (backups/purge_old_part/...), xoá bảng con rồi bài, trừ số thống kê ngày.
Mặc định chỉ in kế hoạch + mẫu (dry-run). Phải có R2 cấu hình, nếu không từ chối xoá.

    .venv/bin/python -m scripts.tools.purge_old_part_posts              # dry-run
    .venv/bin/python -m scripts.tools.purge_old_part_posts --apply
    .venv/bin/python -m scripts.tools.purge_old_part_posts --apply --movie movie_5cc7343d-cbf5-4f1d-a742-a1febbb0390e
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, func, literal_column, select

from app.clients.lake import lake_configured, put_object
from app.repositories.d1.sql import json_values
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import posts as posts_table
from app.services.relevance.cleanup import _BACKUP_TABLE_OBJECTS, _BACKUP_TABLES, _run_stmt
from app.services.stats.stats_reconcile import subtract_deleted

# Lô nhỏ + nghỉ giữa các lô: một bài Thám Tử Kiên có ~36 bình luận, lô 500 bài xoá cả chục nghìn dòng trong một câu - dễ quá 60 giây
# hoặc làm D1 báo quá tải (7429). 150 bài mỗi lô (~5.000 bình luận) và nghỉ 1,5 giây.
BATCH_SIZE = 150
BATCH_PAUSE_SECONDS = 1.5


@dataclass(frozen=True)
class OldPartRule:
    name: str
    movie_id: str
    cutoff: str  # 'YYYY-MM-DD': bài đăng trước ngày này mới có thể là phần cũ
    markers: tuple[str, ...]  # dấu hiệu của phần MỚI, viết thường không dấu


OLD_PART_RULES = (
    OldPartRule(
        "Shaun the Sheep: Quái Lạ Ghé Nhà",
        "movie_f963d1b5-49a5-4fc4-89fb-dd51fca4030a",
        "2026-01-01",
        ("quai la", "ghe nha", "mossy", "beast of"),
    ),
    OldPartRule(
        "Thám Tử Kiên: Lời Nguyền Hoàng Kim",
        "movie_5cc7343d-cbf5-4f1d-a742-a1febbb0390e",
        "2025-11-01",
        ("loi nguyen", "hoang kim", "phan 2", "kien 2", "thamtukien2"),
    ),
)


def fold(text: str | None) -> str:
    """Chữ thường, bỏ dấu (đ -> d) - để bắt cả 'Lời Nguyền' lẫn 'loi nguyen'."""
    value = unicodedata.normalize("NFD", (text or "").replace("đ", "d").replace("Đ", "d"))
    return "".join(ch for ch in value if unicodedata.category(ch) != "Mn").lower()


def is_old_part(content: str | None, posted_at: str | None, rule: OldPartRule) -> bool:
    """Phần cũ: có ngày đăng, đăng trước mốc, và không nhắc dấu hiệu phần mới."""
    if not posted_at or posted_at[:10] >= rule.cutoff:
        return False
    text = fold(content)
    return not any(marker in text for marker in rule.markers)


async def find_old_posts(rule: OldPartRule) -> tuple[list[dict[str, Any]], int]:
    """(các bài phần cũ, số bài trước mốc được giữ vì nhắc phần mới)."""
    rows = await _run_stmt(
        select(
            posts_table.c.id,
            posts_table.c.platform,
            posts_table.c.posted_at,
            func.substr(posts_table.c.content, 1, 1500).label("content"),
        ).where(posts_table.c.movie_id == rule.movie_id, posts_table.c.posted_at < rule.cutoff)
    )
    old = [row for row in rows if is_old_part(row.get("content"), row.get("posted_at"), rule)]
    return old, len(rows) - len(old)


async def count_comments(ids: list[str]) -> int:
    total = 0
    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start : start + BATCH_SIZE]
        rows = await _run_stmt(
            select(func.count().label("n"))
            .select_from(comments_table)
            .where(comments_table.c.post_id.in_(json_values(batch)))
        )
        total += rows[0]["n"]
    return total


async def delete_with_backup(ids: list[str], run_key: str, offset: int) -> dict[str, int]:
    """Sao lưu rồi xoá các bài (cùng bảng con) theo lô; trả số đã xoá."""
    totals = {"posts": 0, "comments": 0, "batches": 0}
    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start : start + BATCH_SIZE]
        backup: dict[str, list[dict[str, Any]]] = {}
        index = offset + totals["batches"]
        for table, column in _BACKUP_TABLES:
            obj = _BACKUP_TABLE_OBJECTS[table]
            backup[table] = await _run_stmt(
                select(literal_column("*")).select_from(obj).where(obj.c[column].in_(json_values(batch)))
            )
            body = gzip.compress("\n".join(json.dumps(r, ensure_ascii=False) for r in backup[table]).encode())
            await put_object(f"{run_key}/{index:03d}-{table}.jsonl.gz", body)
        for table, column in _BACKUP_TABLES[1:]:
            obj = _BACKUP_TABLE_OBJECTS[table]
            deleted = len(
                await _run_stmt(
                    delete(obj).where(obj.c[column].in_(json_values(batch))).returning(literal_column("1").label("x"))
                )
            )
            if table == "comments":
                totals["comments"] += deleted
        totals["posts"] += len(
            await _run_stmt(
                delete(posts_table).where(posts_table.c.id.in_(json_values(batch))).returning(posts_table.c.id)
            )
        )
        totals["batches"] += 1
        try:
            await subtract_deleted(backup["posts"], backup["comments"])
        except Exception as exc:  # noqa: BLE001 - dữ liệu đã xoá; số thống kê lệch chỉ được báo
            print(f"  cảnh báo: trừ số thống kê lỗi ({str(exc)[:80]})")
        print(f"  lô {index}: xoá {len(batch)} bài", flush=True)
        await asyncio.sleep(BATCH_PAUSE_SECONDS)
    return totals


async def run(apply: bool, only: str | None) -> None:
    if apply and not lake_configured():
        raise SystemExit("Chưa cấu hình R2 - không sao lưu được nên từ chối xoá.")
    run_key = f"backups/purge_old_part/{datetime.now(UTC):%Y-%m-%d}/{uuid.uuid4().hex[:8]}"
    offset = 0
    for rule in OLD_PART_RULES:
        if only and rule.movie_id != only:
            continue
        old, kept = await find_old_posts(rule)
        ids = [row["id"] for row in old]
        by_platform: dict[str, int] = {}
        for row in old:
            by_platform[row["platform"]] = by_platform.get(row["platform"], 0) + 1
        print(f"\n== {rule.name} | mốc {rule.cutoff}")
        print(f"   phần cũ: {len(ids)} bài {by_platform} | giữ (trước mốc nhưng nhắc phần mới): {kept}")
        for row in old[:: max(1, len(old) // 6)][:6]:
            print(f"     mẫu {row['posted_at'][:10]} {row['platform']}: {(row['content'] or '')[:80]!r}")
        if not apply:
            print(f"   bình luận đi kèm: {await count_comments(ids)}")
            continue
        totals = await delete_with_backup(ids, run_key, offset)
        offset += totals["batches"]
        print(f"   ĐÃ XOÁ {totals['posts']} bài, {totals['comments']} bình luận")
    if apply:
        print(f"\nSao lưu R2: {run_key}")
    else:
        print("\nChạy thử - chưa xoá gì. Thêm --apply để xoá (có sao lưu R2).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--movie", help="chỉ một phim (id)")
    args = parser.parse_args()
    asyncio.run(run(args.apply, args.movie))
