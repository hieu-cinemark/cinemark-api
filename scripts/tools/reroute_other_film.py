"""Chuyển bài về đúng phim: bài tìm ra bằng từ khoá phim A nhưng nói về phim B đang theo dõi.

Ingest từ 13/11/2026 tự chuyển bài như vậy (app/workers/ingest_consumer/main.py, _reroute_to_other_film). Script này làm
lại cho dữ liệu CŨ, cùng luật relevance_rules.find_film:

  --source hidden   bài đang ẩn trong D1 (relevance_label not_related / uncertain): chuyển sang phim B, gắn nhãn related.
  --source backup   bản sao lưu đợt xoá bài not_related (scripts/tools/purge_irrelevant_posts.py, mặc định
                    ~/cinemark-ml/purge_backup): khôi phục bài của phim B cùng bình luận, lịch sử tương tác, nhận diện ảnh.

Mặc định chỉ in kế hoạch (không ghi gì). --apply mới ghi. Từ khoá của phim B lấy đúng nền tảng của bài; phim B chưa có
từ khoá trên nền tảng đó thì tạo một từ khoá TẮT (chỉ để gắn bài, không cào). Chạy theo lô nhỏ, nghỉ giữa các lô vì D1
chạy câu lần lượt (quá tải trả 7429).

    python -m scripts.tools.reroute_other_film --source hidden
    python -m scripts.tools.reroute_other_film --source hidden --apply
    python -m scripts.tools.reroute_other_film --source backup --apply

Sau khi ghi: dựng lại số theo ngày (python -m scripts.jobs.rebuild_stats_summaries và bảng tổng hợp bình luận) để
dashboard thấy bài mới chuyển.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from app.clients.d1 import d1_query
from app.core.config import settings
from app.repositories.d1.sql import d1_exec, json_values
from app.repositories.d1.store import add_keyword
from app.repositories.d1.tables import keywords, movies, posts
from app.services.relevance.relevance_rules import find_film as _find_film

PAGE = 400
WRITE_CHUNK = 150
PAUSE_SECONDS = 1.0
MAX_JSON_BYTES = 400_000  # mỗi câu ghi một tham số JSON; giữ nhỏ cho D1
HIDDEN_LABELS = ("not_related", "uncertain")
BACKUP_DIR = Path.home() / "cinemark-ml" / "purge_backup"


async def load_movies() -> dict[str, dict[str, Any]]:
    """Giống ingest _tracked_movies: mọi phim có tên, kèm từ khoá đang bật."""
    movie_rows = await d1_exec(select(movies.c.id, movies.c.title, movies.c.released_at, movies.c.slug))
    keyword_rows = await d1_exec(select(keywords.c.movie_id, keywords.c.keyword).where(keywords.c.enabled == 1))
    if movie_rows is None or keyword_rows is None:
        raise SystemExit("Không đọc được danh sách phim/từ khoá từ D1")
    out = {r["id"]: {**r, "keywords": []} for r in movie_rows if r.get("title")}
    for row in keyword_rows:
        if row["movie_id"] in out:
            out[row["movie_id"]]["keywords"].append(row["keyword"])
    return out


class KeywordPicker:
    """Từ khoá cùng nền tảng của phim đích; thiếu thì tạo từ khoá tắt (chỉ khi --apply)."""

    def __init__(self, film: dict[str, dict[str, Any]], apply: bool) -> None:
        self.film, self.apply, self.cache, self.created = film, apply, {}, []

    async def load(self) -> None:
        rows = await d1_exec(
            select(keywords.c.id, keywords.c.movie_id, keywords.c.platform, keywords.c.keyword, keywords.c.enabled)
        )
        for row in sorted(rows or [], key=lambda r: (not r["enabled"],)):
            title = self.film.get(row["movie_id"], {}).get("title")
            key = (row["movie_id"], row["platform"])
            if key not in self.cache or row["keyword"] == title:
                self.cache[key] = row["id"]

    async def get(self, movie_id: str, platform: str) -> str | None:
        key = (movie_id, platform)
        if key not in self.cache:
            title = str(self.film[movie_id].get("title") or "")
            created = await add_keyword(movie_id, platform, title, enabled=False) if self.apply else "(tạo khi --apply)"
            self.created.append((self.film[movie_id]["title"], platform))
            self.cache[key] = created
        return self.cache[key]


def find_film(content: Any, film: Any, posted_at: Any, *, exclude_movie_id: Any) -> dict[str, Any] | None:
    """Cùng luật chặt với ingest khi chuyển bài sang phim khác."""
    return _find_film(
        content,
        film,
        posted_at,
        exclude_movie_id=exclude_movie_id,
        strict=True,
        strict_slugs=settings.strict_relevance_movies,
        ambiguous_slugs=settings.ambiguous_title_movies,
    )


def report(moves: list[dict[str, Any]], film: dict[str, dict[str, Any]], scanned: int) -> None:
    pairs = Counter(
        (film.get(m["from"], {}).get("title", m["from"]), film[m["to"]]["title"], m["platform"]) for m in moves
    )
    print(f"\nĐã xét {scanned} bài, chuyển được {len(moves)}:")
    for (src, dst, platform), n in pairs.most_common(40):
        print(f"  {n:6d}  {platform:9s} {src}  ->  {dst}")
    for m in moves[:12]:
        print(f"    · {film[m['to']]['title']}: {' '.join(str(m['content'] or '').split())[:110]}")


# --- nguồn 1: bài đang ẩn trong D1 ------------------------------------------------------------------------------------


async def hidden_moves(film: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    moves, scanned, last_id = [], 0, ""
    while True:
        page = await d1_exec(
            select(posts.c.id, posts.c.movie_id, posts.c.platform, posts.c.content, posts.c.posted_at)
            .where(posts.c.relevance_label.in_(HIDDEN_LABELS), posts.c.id > last_id)
            .order_by(posts.c.id)
            .limit(PAGE),
            timeout=90.0,
        )
        if page is None:
            raise SystemExit(f"D1 lỗi khi đọc bài ẩn sau id {last_id!r} - chạy lại sau ít phút")
        if not page:
            return moves, scanned
        scanned += len(page)
        for row in page:
            target = find_film(row["content"], film.values(), row["posted_at"], exclude_movie_id=row["movie_id"])
            if target:
                moves.append({**row, "from": row["movie_id"], "to": target["id"]})
        last_id = page[-1]["id"]
        print(f"  ... {scanned} bài ẩn đã xét, {len(moves)} cần chuyển", flush=True)
        await asyncio.sleep(PAUSE_SECONDS)


async def apply_hidden(moves: list[dict[str, Any]], picker: KeywordPicker) -> int:
    now = datetime.now(tz=UTC).isoformat()
    rows = [{"id": m["id"], "m": m["to"], "k": await picker.get(m["to"], m["platform"])} for m in moves]
    rows = [r for r in rows if r["k"]]
    done = 0
    for start in range(0, len(rows), WRITE_CHUNK):
        chunk = json.dumps(rows[start : start + WRITE_CHUNK])
        # json_each: cả lô một tham số (giới hạn 100 tham số của D1).
        ok = await d1_query(
            "UPDATE posts SET movie_id = j.m, keyword_id = j.k, relevance_label = 'related', keyword_match = 1, "
            "relevance_confidence = NULL, relevance_labeled_at = ?2 "
            "FROM (SELECT value ->> 'id' AS id, value ->> 'm' AS m, value ->> 'k' AS k FROM json_each(?1)) AS j "
            "WHERE posts.id = j.id",
            [chunk, now],
            timeout=60.0,
        )
        await d1_query(
            "UPDATE post_visuals SET movie_id = j.m "
            "FROM (SELECT value ->> 'id' AS id, value ->> 'm' AS m FROM json_each(?1)) AS j WHERE post_visuals.post_id = j.id",
            [chunk],
            timeout=60.0,
            quiet=True,
        )
        if ok is None:
            raise SystemExit(f"D1 lỗi khi ghi lô bắt đầu ở {start} - đã ghi {done}; chạy lại để làm tiếp")
        done += len(rows[start : start + WRITE_CHUNK])
        print(f"  ... đã chuyển {done}/{len(rows)}", flush=True)
        await asyncio.sleep(PAUSE_SECONDS)
    return done


# --- nguồn 2: bản sao lưu đợt xoá -------------------------------------------------------------------------------------


def _read(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


async def backup_moves(film: dict[str, dict[str, Any]], backup_dir: Path) -> tuple[list[dict[str, Any]], int]:
    rows = _read(backup_dir / "posts.jsonl.gz")
    moves = []
    for row in rows:
        target = find_film(
            row.get("content"), film.values(), row.get("posted_at"), exclude_movie_id=row.get("movie_id")
        )
        if target:
            moves.append({**row, "from": row.get("movie_id"), "to": target["id"]})
    # Bài đã được cào lại sau đợt xoá (cùng platform + external_id) thì đã có trong D1: bỏ qua.
    present: set[tuple[str, str]] = set()
    for platform in {m["platform"] for m in moves}:
        ids = [m["external_id"] for m in moves if m["platform"] == platform]
        for start in range(0, len(ids), 2000):
            found = await d1_exec(
                select(posts.c.external_id).where(
                    posts.c.platform == platform, posts.c.external_id.in_(json_values(ids[start : start + 2000]))
                ),
                timeout=90.0,
            )
            present |= {(platform, r["external_id"]) for r in found or []}
    fresh = [m for m in moves if (m["platform"], m["external_id"]) not in present]
    print(f"  {len(moves) - len(fresh)} bài đã có lại trong D1 (cào lại sau đợt xoá) - bỏ qua")
    return fresh, len(rows)


async def _table_columns(table: str) -> list[str]:
    rows = await d1_query(f"PRAGMA table_info({table})")
    if not rows:
        raise SystemExit(f"Không đọc được cột của bảng {table}")
    return [r["name"] for r in rows]


async def _insert_rows(table: str, rows: list[dict[str, Any]]) -> int:
    """INSERT OR IGNORE theo lô, mỗi lô một tham số JSON không quá MAX_JSON_BYTES."""
    if not rows:
        return 0
    columns = [c for c in await _table_columns(table) if c in rows[0]]
    select_list = ", ".join(f"value ->> '{c}'" for c in columns)
    sql = f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) SELECT {select_list} FROM json_each(?1)"
    written, batch, size = 0, [], 0
    for row in [*rows, None]:
        item = None if row is None else json.dumps({c: row.get(c) for c in columns}, ensure_ascii=False)
        if batch and (row is None or size + len(item) > MAX_JSON_BYTES):
            if await d1_query(sql, ["[" + ",".join(batch) + "]"], timeout=90.0) is None:
                raise SystemExit(f"D1 lỗi khi ghi {table} - đã ghi {written}; chạy lại để làm tiếp (INSERT OR IGNORE)")
            written += len(batch)
            batch, size = [], 0
            await asyncio.sleep(PAUSE_SECONDS)
        if item is not None:
            batch.append(item)
            size += len(item)
    return written


async def apply_backup(moves: list[dict[str, Any]], picker: KeywordPicker, backup_dir: Path) -> dict[str, int]:
    now = datetime.now(tz=UTC).isoformat()
    post_rows = []
    for m in moves:
        keyword_id = await picker.get(m["to"], m["platform"])
        if keyword_id:
            row = {k: v for k, v in m.items() if k not in ("from", "to")}
            post_rows.append({
                **row, "movie_id": m["to"], "keyword_id": keyword_id, "relevance_label": "related",
                "keyword_match": 1, "relevance_confidence": None, "relevance_labeled_at": now,
            })  # fmt: skip
    ids = {r["id"] for r in post_rows}
    stats = {"posts": await _insert_rows("posts", post_rows)}
    for table in ("comments", "post_engagement_snapshots", "post_visuals", "post_comment_value"):
        children = [r for r in _read(backup_dir / f"{table}.jsonl.gz") if r.get("post_id") in ids]
        if table == "post_visuals":
            target = {r["id"]: r["movie_id"] for r in post_rows}
            children = [{**r, "movie_id": target[r["post_id"]]} for r in children]
        stats[table] = await _insert_rows(table, children)
    return stats


async def main(args: argparse.Namespace) -> None:
    film = await load_movies()
    picker = KeywordPicker(film, args.apply)
    await picker.load()
    if args.source == "hidden":
        moves, scanned = await hidden_moves(film)
    else:
        moves, scanned = await backup_moves(film, args.backup_dir)
    report(moves, film, scanned)
    if not args.apply:
        missing = {
            (film[m["to"]]["title"], m["platform"]) for m in moves if (m["to"], m["platform"]) not in picker.cache
        }
        if missing:
            print(
                f"\nSẽ tạo {len(missing)} từ khoá TẮT (phim chưa có từ khoá trên nền tảng đó): {sorted(missing)[:10]}"
            )
        print("\nChỉ in kế hoạch. Chạy lại với --apply để ghi.")
        return
    if args.source == "hidden":
        print(f"\nĐã chuyển {await apply_hidden(moves, picker)} bài.")
    else:
        print(f"\nĐã khôi phục: {await apply_backup(moves, picker, args.backup_dir)}")
    if picker.created:
        print(f"Đã tạo {len(picker.created)} từ khoá tắt: {picker.created[:10]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=("hidden", "backup"), required=True)
    parser.add_argument("--backup-dir", type=Path, default=BACKUP_DIR)
    parser.add_argument("--apply", action="store_true", help="ghi vào D1 (mặc định chỉ in kế hoạch)")
    asyncio.run(main(parser.parse_args()))
