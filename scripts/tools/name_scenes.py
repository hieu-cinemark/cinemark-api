"""Đặt tên "Cảnh được bàn nhiều" của một phim bằng Haiku (2026-11-09): mỗi cảnh một lần gọi, kèm ảnh tiêu biểu (bucket lake,
media/covers/...), chú thích các bài dùng cảnh đó và bình luận nhiều like của những bài này. Không đủ căn cứ thì ghi
"Chưa rõ cảnh" chứ không đoán. Nhãn gắn theo bài đại diện của cảnh (rep_post_id) ở bảng movie_scene_labels - bảng
movie_scenes được job post_visuals viết lại mỗi giờ nên không lưu chung. Cảnh đã có nhãn thì bỏ qua (trừ --force).

    python -m scripts.tools.name_scenes --movie-id <id> --dry-run
    python -m scripts.tools.name_scenes --movie-id <id>
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert

from app.ai.providers.client import _client_for, load_provider
from app.clients.lake import get_object
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.store import d1_query
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import movie_scene_labels as movie_scene_labels_table
from app.repositories.d1.tables import movie_scenes as movie_scenes_table
from app.repositories.d1.tables import movies as movies_table

MAX_SCENES = 8

SYSTEM_PROMPT = """Bạn giúp bộ phận truyền thông của công ty phát hành phim Việt Nam nhận ra một cảnh phim mà khán giả chụp lại
từ màn hình rạp và bàn tán trên mạng xã hội. Bạn được xem ảnh chụp cảnh đó (nếu có), thông tin phim, chú thích các bài đăng
dùng ảnh này và một số bình luận dưới các bài đó.

Viết tên cảnh ngắn (3-10 chữ) luôn mô tả điều nhìn thấy hoặc điều khán giả bàn về cảnh đó (ví dụ "Người vợ khóc nói về bổn
phận im lặng", "Người phụ nữ áo tím bên bếp lửa"), và một câu mô tả vì sao cảnh được bàn tán. Chỉ ghi tên nhân vật / diễn viên
khi chú thích hoặc bình luận đi kèm có nhắc; không đoán tên từ khuôn mặt. chac_chan = "cao" khi biết rõ đó là cảnh nào / ai
trong cảnh, "thap" khi chỉ mô tả được điều nhìn thấy. Chỉ ghi "Chưa rõ cảnh" khi ảnh không xem được và chữ đi kèm cũng không
nói gì về cảnh.

Trả về đúng một object JSON, không thêm gì khác (không dùng nháy kép trong chuỗi):
{"ten": "...", "mo_ta": "1 câu", "chac_chan": "cao" | "thap"}"""


async def scene_context(movie_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    movie = (
        await d1_exec(
            select(movies_table.c.title, movies_table.c.cast.label("cast_json")).where(movies_table.c.id == movie_id),
            quiet=True,
        )
        or [{}]
    )[0]
    scenes = await d1_exec(
        select(
            movie_scenes_table.c.rank,
            movie_scenes_table.c.rep_post_id,
            movie_scenes_table.c.r2_key,
            movie_scenes_table.c.posts,
            movie_scenes_table.c.discussions,
            movie_scenes_table.c.samples,
        )
        .where(movie_scenes_table.c.movie_id == movie_id)
        .order_by(movie_scenes_table.c.rank)
        .limit(MAX_SCENES),
        quiet=True,
    )
    return movie, scenes or []


async def top_comments(post_ids: list[str]) -> list[str]:
    if not post_ids:
        return []
    c = comments_table.alias("c")
    stmt = (
        select(c.c.message)
        .where(
            c.c.post_id.in_(post_ids),
            c.c.parent_external_id.is_(None),
            func.length(c.c.message) >= 12,
        )
        .order_by(c.c.reactions_count.desc())
        .limit(12)
    )
    rows = await d1_exec(stmt, quiet=True)
    return [" ".join(str(row["message"]).split())[:200] for row in rows or []]


def build_text(movie: dict[str, Any], scene: dict[str, Any], comments: list[str]) -> str:
    samples = json.loads(scene["samples"] or "[]")
    lines = [
        f"Phim: {movie.get('title')}",
        f"Diễn viên: {movie.get('cast_json') or 'không rõ'}",
        f"Cảnh này có {scene['posts']} bài đăng dùng.",
    ]
    lines.append("\nChú thích bài đăng:")
    lines.extend(f"- [{s.get('platform')}] {' '.join((s.get('text') or '').split())[:240]}" for s in samples[:10])
    if comments:
        lines.append("\nBình luận nhiều like dưới các bài này:")
        lines.extend(f"- {c}" for c in comments)
    return "\n".join(lines)


async def ask(text: str, image: bytes | None) -> dict[str, Any] | None:
    cfg = await load_provider("bee")
    client = _client_for(cfg)
    content: Any = text
    if image:
        content = [
            {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + base64.b64encode(image).decode()}},
            {"type": "text", "text": text},
        ]

    def call(body: Any) -> str:
        reply = client.chat.completions.create(
            model=cfg.model,
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": body}],
            max_tokens=1500,
        )
        return reply.choices[0].message.content or ""

    try:
        raw = await asyncio.to_thread(call, content)
    except Exception as exc:
        if not image:
            raise
        print(f"  gửi ảnh lỗi ({str(exc)[:120]}), thử chỉ chữ")
        raw = await asyncio.to_thread(call, text)
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    try:
        return json.loads(match.group(0)) if match else None
    except json.JSONDecodeError:
        return None


async def run(args: argparse.Namespace) -> None:
    movie, scenes = await scene_context(args.movie_id)
    if not args.dry_run:
        # Giữ SQL thuần: CREATE TABLE IF NOT EXISTS là DDL.
        await d1_query(
            """CREATE TABLE IF NOT EXISTS movie_scene_labels (
                movie_id TEXT NOT NULL, rep_post_id TEXT NOT NULL, label TEXT NOT NULL, description TEXT, confident INTEGER,
                created_at TEXT NOT NULL, PRIMARY KEY (movie_id, rep_post_id))""",
            timeout=60.0,
        )
    done = set()
    if not args.force and not args.dry_run:
        rows = await d1_exec(
            select(movie_scene_labels_table.c.rep_post_id).where(movie_scene_labels_table.c.movie_id == args.movie_id),
            quiet=True,
        )
        done = {row["rep_post_id"] for row in rows or []}
    for scene in scenes:
        if scene["rep_post_id"] in done:
            continue
        samples = json.loads(scene["samples"] or "[]")
        comments = await top_comments([s["postId"] for s in samples if s.get("postId")][:20])
        image = await get_object(scene["r2_key"]) if scene.get("r2_key") else None
        result = await ask(build_text(movie, scene, comments), image)
        if not result:
            print(f"cảnh {scene['rank']}: AI không trả JSON")
            continue
        print(
            f"cảnh {scene['rank']} ({scene['posts']} bài, {scene['discussions']} thảo luận) [{result.get('chac_chan')}] {result.get('ten')} - {result.get('mo_ta')}"
        )
        if args.dry_run:
            continue
        values = {
            "movie_id": args.movie_id,
            "rep_post_id": scene["rep_post_id"],
            "label": result.get("ten") or "Chưa rõ cảnh",
            "description": result.get("mo_ta"),
            "confident": int(result.get("chac_chan") == "cao"),
            "created_at": datetime.now(tz=UTC).isoformat(),
        }
        stmt = insert(movie_scene_labels_table).values(values)
        await d1_exec(
            stmt.on_conflict_do_update(
                index_elements=["movie_id", "rep_post_id"],
                set_={k: v for k, v in values.items() if k not in ("movie_id", "rep_post_id")},
            ),
            timeout=30.0,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    asyncio.run(run(parser.parse_args()))
