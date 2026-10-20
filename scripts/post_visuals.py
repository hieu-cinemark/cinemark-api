"""Ảnh bìa bài viết -> R2 + nhãn ảnh (app/vision/cinema_frames.py): chụp màn hình rạp / có mặt ở rạp / sự kiện / khác.

Link ảnh bìa của TikTok/Facebook/Threads là link ký có hạn - sau vài ngày chỉ còn khoảng một nửa tải được, nên chạy
thường xuyên (systemd timer, deploy/systemd/cinemark-visuals.*) trên bài vừa thu. Chỉ xét bài liên quan tới phim, đăng
từ 1 ngày trước ngày khởi chiếu trở đi (cảnh phim / minh chứng đã xem chỉ có sau khi phim chiếu, kể cả suất sớm).

Mỗi bài một dòng trong bảng post_visuals (D1 scraper):
  - status 'ok': ảnh JPEG cạnh dài 384px ở bucket lake, key media/covers/<movie_id>/<post_id>.jpg; label + confidence;
    embedding (float16, base64) chỉ lưu cho screen/venue - dùng gom ảnh cùng cảnh / bỏ ảnh đăng lại.
  - status 'unavailable': link hết hạn hoặc không phải ảnh - không thử lại.

Cách dùng:
    python -m scripts.post_visuals                       # bài thu trong 6 giờ qua (timer)
    python -m scripts.post_visuals --hours 72 --limit 5000
    python -m scripts.post_visuals --from-dir ~/cinemark-ml/cinema_frames/raw   # ảnh đã tải sẵn, tên <post_id>.jpg
    python -m scripts.post_visuals --dry-run --limit 20
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx
import numpy as np

from app.clients.lake import lake_configured, put_object
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query
from app.vision.cinema_frames import MODEL_TAG, CinemaFrameClassifier, shrink_jpeg

logger = get_logger(__name__)

R2_PREFIX = "media/covers"
# Nhãn cần vector để gom cảnh (screen) / tìm ảnh đăng lại (venue); nhãn khác bỏ cho nhẹ bảng.
EMBED_LABELS = {"screen", "venue"}
DOWNLOAD_CONCURRENCY = 16
UPLOAD_CONCURRENCY = 8
CHUNK = 64  # ảnh mỗi lượt tải -> chấm -> lưu
SAVE_CHUNK = 80  # dòng mỗi câu INSERT (~2KB embedding/dòng, qua một tham số JSON)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36"
)

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS post_visuals (
  post_id TEXT PRIMARY KEY,
  movie_id TEXT NOT NULL,
  platform TEXT,
  status TEXT NOT NULL,
  label TEXT,
  confidence REAL,
  probs TEXT,
  r2_key TEXT,
  embedding TEXT,
  model TEXT,
  scored_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)"""
INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_post_visuals_movie_label ON post_visuals(movie_id, label, confidence)"

COVER_SQL = "json_extract(p.raw_json, '$.cover_url')"
RELEASED_SQL = "m.released_at IS NOT NULL AND date(p.posted_at, '+7 hours') >= date(m.released_at, '-1 day')"


async def ensure_table() -> None:
    await d1_query(TABLE_SQL)
    await d1_query(INDEX_SQL)


async def recent_candidates(hours: float, limit: int) -> list[dict[str, Any]]:
    since = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - hours * 3600))
    rows = await d1_query(
        f"""SELECT p.id AS post_id, p.movie_id, p.platform, {COVER_SQL} AS cover
        FROM posts p JOIN movies m ON m.id = p.movie_id
        LEFT JOIN post_visuals v ON v.post_id = p.id
        WHERE p.scraped_at >= ?1 AND v.post_id IS NULL AND {COVER_SQL} IS NOT NULL
          AND {RELEASED_SQL} AND {RELEVANT_POST_SQL}
        ORDER BY p.scraped_at DESC LIMIT ?2""",
        [since, limit],
        timeout=60.0,
    )
    return rows or []


async def rows_for_ids(post_ids: list[str]) -> list[dict[str, Any]]:
    """Bài (chưa có trong post_visuals) cho các ảnh đã tải sẵn ở máy."""
    out: list[dict[str, Any]] = []
    for start in range(0, len(post_ids), 500):
        rows = await d1_query(
            """SELECT p.id AS post_id, p.movie_id, p.platform FROM posts p
            JOIN (SELECT value AS id FROM json_each(?1)) j ON j.id = p.id
            LEFT JOIN post_visuals v ON v.post_id = p.id WHERE v.post_id IS NULL""",
            [json.dumps(post_ids[start : start + 500])],
            timeout=60.0,
        )
        out.extend(rows or [])
    return out


async def download(client: httpx.AsyncClient, sem: asyncio.Semaphore, url: str) -> bytes | None:
    async with sem:
        try:
            response = await client.get(url)
            if response.status_code != 200 or not response.headers.get("content-type", "").startswith("image"):
                return None
            return response.content
        except httpx.HTTPError:
            return None


def _encode_embedding(vector: np.ndarray) -> str:
    return base64.b64encode(vector.astype(np.float16).tobytes()).decode()


async def save(rows: list[dict[str, Any]]) -> bool:
    for start in range(0, len(rows), SAVE_CHUNK):
        result = await d1_query(
            """INSERT OR REPLACE INTO post_visuals (post_id, movie_id, platform, status, label, confidence, probs, r2_key, embedding, model)
            SELECT value ->> 'post_id', value ->> 'movie_id', value ->> 'platform', value ->> 'status', value ->> 'label',
                   value ->> 'confidence', value ->> 'probs', value ->> 'r2_key', value ->> 'embedding', value ->> 'model'
            FROM json_each(?1)""",
            [json.dumps(rows[start : start + SAVE_CHUNK])],
            timeout=60.0,
        )
        if result is None:
            logger.warning("post_visuals_save_failed", count=len(rows[start : start + SAVE_CHUNK]))
            return False
    return True


async def process_chunk(
    clf: CinemaFrameClassifier, chunk: list[dict[str, Any]], images: list[bytes | None], dry_run: bool
) -> Counter[str]:
    tally: Counter[str] = Counter()
    ready: list[tuple[dict[str, Any], bytes, Any]] = []
    out_rows: list[dict[str, Any]] = []
    for row, data in zip(chunk, images, strict=True):
        shrunk = None
        if data:
            try:
                shrunk = shrink_jpeg(data)
            except Exception:  # noqa: BLE001 - ảnh hỏng / không phải ảnh
                shrunk = None
        if shrunk is None:
            tally["unavailable"] += 1
            out_rows.append({**_base(row), "status": "unavailable"})
            continue
        ready.append((row, shrunk[0], shrunk[1]))

    labels = clf.classify([image for _, _, image in ready]) if ready else []
    upload_sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)

    async def upload(row: dict[str, Any], jpeg: bytes) -> str | None:
        key = f"{R2_PREFIX}/{row['movie_id']}/{row['post_id']}.jpg"
        if dry_run:
            return key
        async with upload_sem:
            try:
                await put_object(key, jpeg, content_type="image/jpeg")
                return key
            except Exception as exc:  # noqa: BLE001 - lần chạy sau thử lại (không ghi dòng)
                logger.warning("post_visuals_upload_failed", post_id=row["post_id"], error=str(exc)[:200])
                return None

    keys = await asyncio.gather(*(upload(row, jpeg) for row, jpeg, _ in ready))
    for (row, _, _), label, key in zip(ready, labels, keys, strict=True):
        if key is None:
            tally["upload_failed"] += 1
            continue
        tally[label.label] += 1
        out_rows.append(
            {
                **_base(row),
                "status": "ok",
                "label": label.label,
                "confidence": round(label.confidence, 4),
                "probs": json.dumps(label.probs),
                "r2_key": key,
                "embedding": _encode_embedding(label.embedding) if label.label in EMBED_LABELS else None,
                "model": MODEL_TAG,
            }
        )
    if dry_run:
        for item in out_rows[:10]:
            print(item["post_id"], item["status"], item.get("label"), item.get("confidence"))
    elif out_rows and not await save(out_rows):
        tally["save_failed"] += len(out_rows)
    return tally


def _base(row: dict[str, Any]) -> dict[str, Any]:
    return {"post_id": row["post_id"], "movie_id": row["movie_id"], "platform": row.get("platform")}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hours", type=float, default=6, help="bài thu trong chừng này giờ qua")
    parser.add_argument("--limit", type=int, default=3000)
    parser.add_argument("--from-dir", type=Path, help="ảnh đã tải sẵn (<post_id>.jpg) thay vì tải từ link")
    parser.add_argument("--dry-run", action="store_true", help="không ghi R2/D1")
    args = parser.parse_args()

    if not args.dry_run and not lake_configured():
        raise SystemExit("Thiếu R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY")
    await ensure_table()
    if args.from_dir:
        files = {f.stem: f for f in Path(args.from_dir).expanduser().glob("*.jpg")}
        rows = (await rows_for_ids(sorted(files)))[: args.limit]
    else:
        rows = await recent_candidates(args.hours, args.limit)
    logger.info("post_visuals_start", candidates=len(rows), source=str(args.from_dir or f"{args.hours}h"))
    if not rows:
        return

    started = time.monotonic()
    clf = CinemaFrameClassifier()
    totals: Counter[str] = Counter()
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        for start in range(0, len(rows), CHUNK):
            chunk = rows[start : start + CHUNK]
            if args.from_dir:
                images = [files[row["post_id"]].read_bytes() for row in chunk]
            else:
                images = await asyncio.gather(*(download(client, sem, row["cover"]) for row in chunk))
            totals += await process_chunk(clf, chunk, list(images), args.dry_run)
            if (start // CHUNK) % 10 == 0:
                logger.info("post_visuals_progress", done=start + len(chunk), total=len(rows), counts=dict(totals))
    logger.info(
        "post_visuals_finished",
        minutes=round((time.monotonic() - started) / 60, 1),
        device=clf.device,
        counts=dict(totals),
    )


if __name__ == "__main__":
    asyncio.run(main())
