"""Dọn raw_json cũ trên D1 (2026-11-05): chép nguyên bản lên R2 trước, kiểm tra đã có trên R2, rồi mới bỏ khỏi D1.

D1 đã dùng ~7,6 GB, trong đó raw_json của comment ~1,65 GB và của bài ~0,65 GB (đo mẫu 05/11). Comment không có chỗ nào đọc
raw_json trừ tệp đính kèm (trim_comment_raw) nên chỉ giữ phần đó; bài chỉ giữ các khoá đang được đọc (app.repositories.d1.posts.RAW_JSON_KEEP). Bản gốc nằm ở
archive/raw_json/<bảng>/ trên bucket lake, mỗi lô một file gzip NDJSON {"id", "raw_json"}.

Chạy giờ vắng vì mỗi lô là vài lệnh D1. Con trỏ lưu ở file local nên dừng rồi chạy lại được; D1 quá tải thì nghỉ rồi thử lại.

    python -m scripts.tools.trim_raw_json --table comments --dry-run
    python -m scripts.tools.trim_raw_json --table comments --max-batches 5
    python -m scripts.tools.trim_raw_json --table posts
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from pathlib import Path
from typing import Any

import boto3

from app.core.config import settings
from app.repositories.d1.comments import trim_comment_raw
from app.repositories.d1.posts import trim_post_raw
from app.repositories.d1.store import d1_query

BATCH_SIZE = 500
UPDATE_CHUNK = 90  # D1 tối đa 100 tham số mỗi câu
PAUSE_SECONDS = 1.0
OVERLOAD_PAUSE_SECONDS = 60.0
ROOT = Path(__file__).resolve().parents[2]


def cursor_file(table: str) -> Path:
    return ROOT / "data" / f"trim_raw_json_{table}.cursor"


def build_updates(table: str, rows: list[dict[str, Any]]) -> list[tuple[str, list[Any]]]:
    """Các câu UPDATE cho một lô. Bài đã gọn sẵn thì không cần ghi lại."""
    if table == "comments":
        empty = []
        media = []
        for row in rows:
            kept = trim_comment_raw(json.loads(row["raw_json"]))
            if kept:
                media.append(("UPDATE comments SET raw_json = ? WHERE id = ?", [json.dumps(kept), row["id"]]))
            else:
                empty.append(row["id"])
        return [
            (
                f"UPDATE comments SET raw_json = NULL WHERE id IN ({','.join('?' * len(empty[i : i + UPDATE_CHUNK]))})",
                empty[i : i + UPDATE_CHUNK],
            )
            for i in range(0, len(empty), UPDATE_CHUNK)
        ] + media
    updates = []
    for row in rows:
        trimmed = trim_post_raw(json.loads(row["raw_json"]))
        text = json.dumps(trimmed) if trimmed else None
        if text != row["raw_json"]:
            updates.append(("UPDATE posts SET raw_json = ? WHERE id = ?", [text, row["id"]]))
    return updates


async def archive(client: Any, table: str, rows: list[dict[str, Any]]) -> str:
    """Trả về key đã xác nhận có trên R2. Lỗi thì ném ra, lô này không bị đụng tới."""
    key = f"archive/raw_json/{table}/{rows[0]['id']}__{rows[-1]['id']}.ndjson.gz"
    lines = "".join(json.dumps({"id": row["id"], "raw_json": row["raw_json"]}, ensure_ascii=False) + "\n" for row in rows)
    body = gzip.compress(lines.encode("utf-8"))
    await asyncio.to_thread(client.put_object, Bucket=settings.lake_bucket, Key=key, Body=body)
    head = await asyncio.to_thread(client.head_object, Bucket=settings.lake_bucket, Key=key)
    if head["ContentLength"] != len(body):
        raise RuntimeError(f"archive_size_mismatch:{key}")
    return key


async def main(args: argparse.Namespace) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
    )
    path = cursor_file(args.table)
    last_id = path.read_text().strip() if path.exists() else ""
    batches = rows_done = bytes_seen = 0
    while not args.max_batches or batches < args.max_batches:
        rows = await d1_query(
            f"SELECT id, raw_json FROM {args.table} WHERE id > ? AND raw_json IS NOT NULL ORDER BY id LIMIT ?",
            [last_id, BATCH_SIZE],
            timeout=60.0,
            quiet=True,
        )
        if rows is None:
            print(f"đọc lỗi (D1 có thể quá tải), nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
            await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)
            continue
        if not rows:
            break
        bytes_seen += sum(len(row["raw_json"]) for row in rows)
        updates = build_updates(args.table, rows)
        if args.dry_run:
            print(f"lô {len(rows)} dòng, {len(updates)} lệnh ghi, {bytes_seen / 1e6:.1f} MB raw_json", flush=True)
            return
        await archive(client, args.table, rows)
        for sql, params in updates:
            while await d1_query(sql, params, timeout=60.0, quiet=True) is None:
                print(f"ghi lỗi, nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
                await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)
        last_id = rows[-1]["id"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(last_id)
        batches += 1
        rows_done += len(rows)
        if batches % 20 == 0:
            print(f"{rows_done} dòng, {bytes_seen / 1e6:.0f} MB", flush=True)
        await asyncio.sleep(PAUSE_SECONDS)
    print(f"xong lượt này: {rows_done} dòng, {bytes_seen / 1e6:.0f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", choices=("comments", "posts"), required=True)
    parser.add_argument("--dry-run", action="store_true", help="đọc một lô rồi dừng, không ghi gì")
    parser.add_argument("--max-batches", type=int, default=0)
    asyncio.run(main(parser.parse_args()))
