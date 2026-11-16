"""Lấy bù transcript (phụ đề) cho bài YouTube CŨ và xét lại nhãn liên quan của phim "chặt" (16/11/2026).

Từ 16/11/2026 spider-hub tự lấy transcript cho video mới (công tắc youtube_transcripts) và ingest dùng nó khi lọc - xem
app/repositories/d1/transcripts.py. Bài đã lưu trước đó chưa có. Script này, với bài YouTube trong --days ngày gần nhất
(đang hiện, hoặc bị ẩn 'uncertain') chưa có dòng post_transcripts:
  - lấy transcript (IP máy chạy, --concurrency luồng; thử 16/11/2026 IP VPS lấy được);
  - lưu post_transcripts;
  - phim "chặt" trên YouTube (settings.is_strict_movie): có tín hiệu phim (tiêu đề/mô tả hoặc đoạn đầu transcript) ->
    'related', không có -> 'uncertain' (ẩn, không xoá). Phim khác chỉ lưu transcript, không đổi nhãn.

Mặc định chạy thử: lấy --sample video, in kết quả, không ghi gì. --apply mới chạy hết và ghi.

    python -m scripts.tools.backfill_youtube_transcripts
    python -m scripts.tools.backfill_youtube_transcripts --apply --days 45
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import UTC, datetime

from app.clients.d1 import d1_query
from app.core.config import settings
from app.repositories.d1.store import MOVIE_CONTEXT_FIELDS
from app.repositories.d1.transcripts import ensure_table, save_transcript, transcript_has_film_context
from app.services.relevance.relevance_rules import has_film_context

# Bị chặn (429 / RequestBlocked) liên tiếp chừng này video thì dừng - chạy lại sau hoặc từ máy khác.
MAX_CONSECUTIVE_ERRORS = 15


def _fetch(video_id: str) -> dict | None:
    """Như spider-hub social_crawler/spiders/youtube/transcript/transcript.py (vi, en, rồi phụ đề đầu tiên)."""
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled, VideoUnavailable

    try:
        listing = YouTubeTranscriptApi().list(video_id)
        try:
            chosen = listing.find_transcript(["vi", "en"])
        except NoTranscriptFound:
            chosen = next(iter(listing), None)
        if chosen is None:
            return None
        segments = chosen.fetch()
    except TranscriptsDisabled, NoTranscriptFound, VideoUnavailable:
        return None
    text = " ".join(" ".join(s.text.split()) for s in segments).strip()
    if not text:
        return None
    return {
        "language": chosen.language_code,
        "generated": chosen.is_generated,
        "text": text[:30000],
        "char_count": len(text),
    }


async def _candidates(days: int, limit: int) -> list[dict]:
    await ensure_table()
    return (
        await d1_query(
            "SELECT p.id, p.external_id, p.movie_id, p.content, p.relevance_label FROM posts p"
            " WHERE p.platform = 'youtube' AND COALESCE(p.first_seen_at, p.scraped_at) >= datetime('now', ?1)"
            " AND (p.relevance_label IN ('related', 'uncertain') OR (p.relevance_label IS NULL AND p.keyword_match > 0))"
            " AND NOT EXISTS (SELECT 1 FROM post_transcripts t WHERE t.platform = 'youtube'"
            " AND t.external_id = p.external_id)"
            " ORDER BY COALESCE(p.first_seen_at, p.scraped_at) DESC LIMIT ?2",
            [f"-{days} days", limit],
            timeout=90.0,
        )
        or []
    )


async def _movies() -> dict[str, dict]:
    fields = ", ".join(("id", "slug", *MOVIE_CONTEXT_FIELDS))
    return {row["id"]: row for row in await d1_query(f"SELECT {fields} FROM movies") or []}


def _new_label(row: dict, movie: dict, transcript: dict | None) -> str | None:
    """Nhãn mới cho phim "chặt", hoặc None khi giữ nguyên."""
    if not movie or not settings.is_strict_movie(movie.get("slug"), "youtube"):
        return None
    ok = has_film_context(row["content"], movie) or transcript_has_film_context({"transcript": transcript}, movie)
    label = "related" if ok else "uncertain"
    return label if label != (row["relevance_label"] or "related") else None


async def run(args: argparse.Namespace) -> None:
    movies = await _movies()
    rows = await _candidates(args.days, args.limit if args.apply else args.sample)
    print(f"{len(rows)} bài YouTube chưa có transcript")
    sem = asyncio.Semaphore(args.concurrency)
    stats: Counter[str] = Counter()
    errors = 0

    async def one(row: dict) -> None:
        nonlocal errors
        if errors >= MAX_CONSECUTIVE_ERRORS:
            return
        async with sem:
            try:
                transcript = await asyncio.to_thread(_fetch, row["external_id"])
                errors = 0
            except Exception as exc:  # noqa: BLE001 - bị chặn / mạng: đếm rồi bỏ qua bài này
                errors += 1
                stats["lỗi"] += 1
                print(f"  lỗi {row['external_id']}: {type(exc).__name__}: {str(exc).splitlines()[0][:100]}")
                return
        stats["có transcript" if transcript else "không có phụ đề"] += 1
        movie = movies.get(row["movie_id"]) or {}
        label = _new_label(row, movie, transcript)
        title = " ".join(str(row["content"] or "").split())[:70]
        if label:
            stats[f"-> {label}"] += 1
            print(f"  {movie.get('title')}: {row['relevance_label']} -> {label} | {title}")
        if not args.apply:
            if transcript:
                print(f"  [{transcript['language']} {transcript['char_count']}] {title} :: {transcript['text'][:90]}")
            return
        if transcript:
            await save_transcript("youtube", row["external_id"], transcript)
        if label:
            await d1_query(
                "UPDATE posts SET relevance_label = ?1, relevance_labeled_at = ?2 WHERE id = ?3",
                [label, datetime.now(tz=UTC).isoformat(), row["id"]],
            )

    await asyncio.gather(*(one(row) for row in rows))
    if errors >= MAX_CONSECUTIVE_ERRORS:
        print(f"Dừng sớm: {MAX_CONSECUTIVE_ERRORS} lỗi liên tiếp (YouTube có thể đang chặn IP này).")
    print(dict(stats))
    if not args.apply:
        print("Chạy thử - chưa ghi gì. Thêm --apply để lấy hết và ghi.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--sample", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=3)
    asyncio.run(run(parser.parse_args()))
