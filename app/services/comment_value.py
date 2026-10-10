"""Kira chấm "bài này có đáng cào comment không" (từ 2026-10-10), cho comment_planner.

Hai lớp cũ (bài nóng theo tốc độ tương tác, mẫu phân tầng) không nhìn nội dung bài: bài minigame "comment tag 3 người
bạn nhận vé" có hàng nghìn comment rác nên luôn đứng đầu lớp bài nóng, trong khi một bài review gây tranh cãi vừa phải
có thể không bao giờ được chọn. Ở đây Kira đọc caption của các bài có nhiều comment chưa được chấm và cho điểm 0-100
theo giá trị comment cho việc hiểu khán giả nghĩ gì về phim:
- cao: review/cảm nhận, tranh luận khen chê, so sánh, hỏi đáp có nội dung, tin gây tranh cãi về phim/diễn viên;
- thấp: minigame/giveaway/tag bạn bè, quảng cáo/đặt vé/khuyến mãi, bài không thật sự về phim.

Mỗi bài chỉ chấm một lần (bảng post_comment_value, D1 scraper). Kira tắt/lỗi thì bài chưa có điểm và planner chạy
như cũ."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.ai.kira import call_kira, parse_json_response
from app.core.logging import get_logger
from app.services.d1 import d1_query

logger = get_logger(__name__)

TASK = "comment_value"
# Chỉ chấm bài có ít nhất chừng này comment nền tảng báo (bài ít comment cào rẻ, không cần AI).
MIN_REPLIES = 10
BATCH_PER_MOVIE = 25
MAX_JUDGE_PER_ROUND = 150

SYSTEM_PROMPT = """Bạn chọn bài mạng xã hội đáng thu thập BÌNH LUẬN cho hệ thống phân tích khán giả nghĩ gì về một bộ phim chiếu rạp.
Mỗi bài có caption, tác giả, nền tảng và số bình luận. Cho mỗi bài điểm 0-100 = khả năng phần bình luận chứa ý kiến THẬT của khán giả về phim:
- 80-100: review/cảm nhận sau khi xem, tranh luận khen chê, so sánh với phim khác, hỏi "có nên xem không", tin gây tranh cãi về phim/diễn viên/đạo diễn.
- 50-79: trailer/teaser/hậu trường/phỏng vấn - bình luận có thể bàn về kỳ vọng, diễn viên.
- 20-49: tin thông báo lịch chiếu, doanh thu, poster - bình luận phần lớn tag bạn, hỏi rạp.
- 0-19: minigame/giveaway/"comment tag bạn bè nhận vé", quảng cáo đặt vé/khuyến mãi/combo bắp nước, bài không thật sự về phim.
Chỉ dựa vào nội dung được đưa. Không chắc thì cho điểm giữa (40-60).

Trả về JSON hợp lệ, không bọc code block:
{"results":[{"i":<số thứ tự>,"score":0-100,"kind":"review|discussion|promo_content|announcement|giveaway|ad|off_topic","reason":"vài chữ tiếng Việt"}]}"""

_table_ready = False


async def _ensure_table() -> None:
    global _table_ready
    if _table_ready:
        return
    await d1_query(
        """CREATE TABLE IF NOT EXISTS post_comment_value (
            post_id TEXT PRIMARY KEY,
            score INTEGER NOT NULL,
            kind TEXT,
            reason TEXT,
            judged_at TEXT NOT NULL
        )""",
        quiet=True,
    )
    _table_ready = True


async def load_scores(post_ids: list[str]) -> dict[str, int]:
    await _ensure_table()
    scores: dict[str, int] = {}
    for start in range(0, len(post_ids), 90):  # D1: tối đa 100 tham số mỗi câu
        chunk = post_ids[start : start + 90]
        rows = await d1_query(
            f"SELECT post_id, score FROM post_comment_value WHERE post_id IN ({','.join('?' * len(chunk))})",
            chunk,
            quiet=True,
        )
        scores.update({row["post_id"]: int(row["score"]) for row in rows or []})
    return scores


async def _judge_movie(title: str, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lines = []
    for i, row in enumerate(rows, 1):
        caption = " ".join(str(row.get("content") or "").split())[:400]
        lines.append(
            f'{i}. [{row.get("platform")}] tác giả "{row.get("author") or "?"}", {row.get("reply_count") or 0} bình luận: {caption}'
        )
    user = f"Phim: {title}\n\nCác bài:\n" + "\n".join(lines)
    raw = await call_kira(task=TASK, system_prompt=SYSTEM_PROMPT, user_prompt=user, max_tokens=6000)
    parsed = parse_json_response(raw)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: dict[str, dict[str, Any]] = {}
    for entry in results if isinstance(results, list) else []:
        try:
            index = int(entry.get("i")) - 1
            score = max(0, min(100, int(float(entry.get("score")))))
        except AttributeError, TypeError, ValueError:
            continue
        if 0 <= index < len(rows):
            out[rows[index]["id"]] = {
                "score": score,
                "kind": str(entry.get("kind") or "")[:30],
                "reason": str(entry.get("reason") or "")[:200],
            }
    return out


async def judge_new_posts(rows: list[dict[str, Any]], scores: dict[str, int]) -> dict[str, int]:
    """Chấm các bài trong `rows` (cần id, movie_id, movie_title, platform, author, content, reply_count) có từ MIN_REPLIES
    comment mà chưa có điểm - nhiều comment nhất trước. Ghi bảng và trả về điểm mới. Lỗi của một phim không chặn phim
    khác."""
    pending = [
        row
        for row in rows
        if row["id"] not in scores and int(row.get("reply_count") or 0) >= MIN_REPLIES and row.get("content")
    ]
    pending.sort(key=lambda row: -int(row.get("reply_count") or 0))
    pending = pending[:MAX_JUDGE_PER_ROUND]
    by_movie: dict[str, list[dict[str, Any]]] = {}
    for row in pending:
        by_movie.setdefault(row["movie_id"], []).append(row)
    fresh: dict[str, int] = {}
    for movie_rows in by_movie.values():
        for start in range(0, len(movie_rows), BATCH_PER_MOVIE):
            batch = movie_rows[start : start + BATCH_PER_MOVIE]
            try:
                judged = await _judge_movie(batch[0].get("movie_title") or "", batch)
            except Exception as exc:  # noqa: BLE001 - Kira tắt/lỗi: các bài này chấm ở lượt sau
                logger.warning("comment_value_kira_failed", movie_id=batch[0]["movie_id"], error=str(exc)[:300])
                break
            now = datetime.now(tz=UTC).isoformat()
            for post_id, verdict in judged.items():
                await d1_query(
                    "INSERT OR REPLACE INTO post_comment_value (post_id, score, kind, reason, judged_at) VALUES (?, ?, ?, ?, ?)",
                    [post_id, verdict["score"], verdict["kind"], verdict["reason"], now],
                    quiet=True,
                )
                fresh[post_id] = verdict["score"]
    if fresh:
        logger.info("comment_value_judged", count=len(fresh), high=sum(1 for s in fresh.values() if s >= 70))
    return fresh
