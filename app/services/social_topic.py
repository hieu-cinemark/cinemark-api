"""Tạo report social-listening "top 10 topic" bằng AI cho một phim - logic theo từng
phim dùng chung giữa lượt quét theo lô/cron của scripts/generate_social_topic_reports.py
và nút "Tạo report" bấm tay trên dashboard (POST /movies/{id}/generate-report trong
app/api/routes/movies.py).

Với một phim:
1. Lấy một mẫu có giới hạn, xếp theo tương tác, gồm các comment đã phân loại cảm xúc
   của phim (get_comment_sample_for_movie) cho lời gọi gom topic.
2. Riêng ra, đếm MỌI comment đã phân loại của phim theo cảm xúc
   (get_movie_sentiment_counts) - tỉ lệ của overall_sentiment được tính từ đây, không
   phải từ mẫu có giới hạn, nên số liệu trên màn hình phản ánh đúng toàn bộ tập
   comment kể cả khi nó lớn hơn mẫu mà LLM thấy.
3. Bỏ qua nếu có ít hơn MIN_COMMENTS_FOR_REPORT comment đã phân loại - không đủ tín
   hiệu để gom topic có ý nghĩa.
4. Hai lời gọi AI (Kira mặc định, hoặc Bee nếu chọn trong Settings): topics +
   verbatims (trên mẫu), rồi một đoạn narrative được cấp tỉ lệ thật từ bước 2.
5. Ghép đúng dạng ReportData['dashboard_data'] mà frontend cần rồi upsert.
"""

from __future__ import annotations

import json
from typing import Literal

from app.ai.client import load_provider
from app.ai.tasks.report import generate_narrative, generate_topics_and_verbatims
from app.core.logging import get_logger
from app.services.d1 import (
    MIN_COMMENTS_FOR_REPORT,
    d1_query,
    get_comment_sample_for_movie,
    get_movie_sentiment_counts,
    upsert_social_topic_report,
)

logger = get_logger(__name__)

ReportResult = Literal["generated", "insufficient_data", "topics_failed", "upsert_failed"]


def _percent(count: int, total: int) -> float:
    return round((count / total) * 100, 1) if total else 0.0


def _norm_comment_text(value: str | None) -> str:
    return " ".join((value or "").split()).casefold()


def _source_fields(comment: dict) -> dict:
    return {
        "author_name": comment.get("author_name"),
        "author_url": comment.get("author_url"),
        "author_profile_picture": comment.get("author_profile_picture"),
        "post_url": comment.get("post_url"),
        "post_content": comment.get("post_content"),
        "post_author": comment.get("post_author"),
        "platform": comment.get("platform"),
    }


def _lookup_sample_comment(item: dict, by_id: dict[str, dict], by_text: dict[str, dict]) -> dict | None:
    comment_id = item.get("id")
    if isinstance(comment_id, str) and comment_id in by_id:
        return by_id[comment_id]
    return by_text.get(_norm_comment_text(item.get("text") or item.get("message")))


def hydrate_report_comments(topics_result: dict, sample: list[dict]) -> dict:
    """Gắn thêm các trường tác giả + bài cha mà dashboard hiển thị cạnh evidence/verbatims.
    LLM chỉ thấy id/message/likes; phần còn lại được ghép lại từ chính mẫu đó sau lời
    gọi, để các report đã có cũng có thể được bổ sung lúc đọc bằng cùng cách khớp text."""
    by_id = {c["id"]: c for c in sample if c.get("id")}
    by_text: dict[str, dict] = {}
    for comment in sample:
        key = _norm_comment_text(comment.get("message"))
        if not key:
            continue
        previous = by_text.get(key)
        if previous is None or (comment.get("reactions_count") or 0) > (previous.get("reactions_count") or 0):
            by_text[key] = comment

    topics = []
    for topic in topics_result.get("top_10_topics") or []:
        evidence = []
        for item in topic.get("evidence_comments") or []:
            if not isinstance(item, dict):
                continue
            match = _lookup_sample_comment(item, by_id, by_text)
            evidence.append({**item, **(_source_fields(match) if match else {})})
        topics.append({**topic, "evidence_comments": evidence})

    verbatims = []
    for item in topics_result.get("top_10_verbatims") or []:
        if not isinstance(item, dict):
            continue
        match = _lookup_sample_comment(item, by_id, by_text)
        verbatims.append({**item, **(_source_fields(match) if match else {})})

    return {**topics_result, "top_10_topics": topics, "top_10_verbatims": verbatims}


async def get_movie_for_report(movie_id: str) -> dict | None:
    rows = await d1_query("SELECT id, title FROM movies WHERE id = ?", [movie_id])
    return rows[0] if rows else None


async def generate_report_for_movie(movie: dict, *, dry_run: bool = False) -> ReportResult:
    movie_id, movie_title = movie["id"], movie["title"]

    counts = await get_movie_sentiment_counts(movie_id)
    total_classified = sum(counts.values())
    if total_classified < MIN_COMMENTS_FOR_REPORT:
        logger.info("report_skip_insufficient_data", movie_id=movie_id, classified=total_classified)
        return "insufficient_data"

    sample = await get_comment_sample_for_movie(movie_id)
    post_count = len({c["post_id"] for c in sample})

    topics_result = await generate_topics_and_verbatims(movie_title, sample)
    if topics_result is None:
        logger.warning("report_skip_topics_failed", movie_id=movie_id)
        return "topics_failed"

    topics_result = hydrate_report_comments(topics_result, sample)

    positive_percent = _percent(counts.get("positive", 0), total_classified)
    negative_percent = _percent(counts.get("negative", 0), total_classified)
    neutral_percent = _percent(counts.get("neutral", 0), total_classified)
    topic_names = [t.get("topic_name", "") for t in topics_result.get("top_10_topics", [])[:5]]

    analysis = await generate_narrative(
        movie_title,
        positive_percent=positive_percent,
        negative_percent=negative_percent,
        neutral_percent=neutral_percent,
        topic_names=topic_names,
    )

    dashboard_data = {
        "report_title": f"Báo cáo mạng xã hội · {movie_title}",
        "overall_sentiment": {
            "positive_percent": positive_percent,
            "negative_percent": negative_percent,
            "neutral_percent": neutral_percent,
            "analysis": analysis or "",
        },
        "top_10_topics": topics_result.get("top_10_topics", []),
        "top_10_verbatims": topics_result.get("top_10_verbatims", []),
    }

    if dry_run:
        logger.info(
            "report_dry_run_would_upsert",
            movie_id=movie_id,
            comment_count=len(sample),
            post_count=post_count,
            topics=len(dashboard_data["top_10_topics"]),
            verbatims=len(dashboard_data["top_10_verbatims"]),
        )
        return "generated"

    # Model thực sự đã viết topics: model của provider đó trong ai_providers (hoặc chính
    # key của provider nếu dòng đó không có model).
    provider = topics_result.get("provider") or "kira"
    provider_cfg = await load_provider(provider)
    model = (provider_cfg.model if provider_cfg else "") or provider

    ok = await upsert_social_topic_report(
        movie_id=movie_id,
        dashboard_data_json=json.dumps(dashboard_data, ensure_ascii=False),
        comment_count=len(sample),
        post_count=post_count,
        # Cột vẫn tên là kira_model (schema có từ trước khi có Bee) - lưu model nào đã viết
        # report.
        kira_model=model,
    )
    if not ok:
        logger.warning("report_upsert_failed", movie_id=movie_id)
        return "upsert_failed"
    logger.info("report_generated", movie_id=movie_id, comment_count=len(sample), post_count=post_count)
    return "generated"
