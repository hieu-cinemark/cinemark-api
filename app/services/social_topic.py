"""Tạo report social-listening "top 10 topic" bằng AI cho một phim - logic theo từng
phim dùng chung giữa lượt quét theo lô/cron của scripts/generate_social_topic_reports.py
và nút "Tạo report" bấm tay trên dashboard (POST /movies/{id}/generate-report trong
app/api/routes/movies.py).

Với một phim:
1. Rút ngẫu nhiên một mẫu có giới hạn gồm comment đã phân loại cảm xúc và bài viết của
   khán giả (get_report_sample_for_movie) cho lời gọi gom topic - AI dùng bài viết vừa làm
   ngữ cảnh cho comment vừa làm bằng chứng riêng (trừ bài của kênh/studio).
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
    get_movie_aspect_stats,
    get_movie_sentiment_counts,
    get_report_sample_for_movie,
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


# Nội dung đầy đủ của bài viết làm bằng chứng được cắt tới bấy nhiêu ký tự - AI chỉ thấy bản cắt ngắn
# (REPORT_POST_TEXT_CHARS), dashboard hiện bản dài hơn này.
_POST_EVIDENCE_TEXT_CHARS = 1500


def _lookup_sample_comment(item: dict, by_id: dict[str, dict], by_text: dict[str, dict]) -> dict | None:
    comment_id = item.get("id")
    if isinstance(comment_id, str) and comment_id in by_id:
        return by_id[comment_id]
    return by_text.get(_norm_comment_text(item.get("text") or item.get("message")))


def _hydrate_item(item: dict, comments: tuple[dict, dict], posts_by_id: dict[str, dict]) -> dict:
    item_id = item.get("id")
    post = posts_by_id.get(item_id) if isinstance(item_id, str) else None
    if post is not None and (item.get("source") == "post" or item_id not in comments[0]):
        text = (post.get("post_content") or "").strip()
        return {
            **item,
            "source": "post",
            "text": text[:_POST_EVIDENCE_TEXT_CHARS] if text else item.get("text"),
            "author_name": post.get("post_author"),
            "author_url": None,
            "author_profile_picture": None,
            "post_url": post.get("post_url"),
            "post_content": None,
            "post_author": post.get("post_author"),
            "platform": post.get("platform"),
        }
    match = _lookup_sample_comment(item, *comments)
    if match is None:
        return {**item, "source": "comment"}
    # AI chỉ thấy bản cắt ngắn của comment dài - hiện lại nguyên văn.
    return {**item, "source": "comment", "text": match.get("message") or item.get("text"), **_source_fields(match)}


def hydrate_report_comments(topics_result: dict, sample: list[dict], posts: list[dict] | None = None) -> dict:
    """Gắn thêm các trường tác giả + bài cha mà dashboard hiển thị cạnh evidence/verbatims.
    LLM chỉ thấy id/message/likes; phần còn lại được ghép lại từ chính mẫu đó sau lời
    gọi, để các report đã có cũng có thể được bổ sung lúc đọc bằng cùng cách khớp text.
    Bằng chứng là bài viết (source="post", tra theo id trong `posts` hoặc bài cha của một
    comment trong mẫu) lấy tác giả/link từ chính bài đó và hiện nội dung đầy đủ hơn bản
    AI thấy."""
    posts_by_id: dict[str, dict] = {}
    for comment in sample:
        if comment.get("post_id") and comment.get("post_content"):
            posts_by_id.setdefault(comment["post_id"], comment)
    for post in posts or []:
        posts_by_id[post["id"]] = post

    by_id = {c["id"]: c for c in sample if c.get("id")}
    by_text: dict[str, dict] = {}
    for comment in sample:
        key = _norm_comment_text(comment.get("message"))
        if not key:
            continue
        previous = by_text.get(key)
        if previous is None or (comment.get("reactions_count") or 0) > (previous.get("reactions_count") or 0):
            by_text[key] = comment

    comments = (by_id, by_text)
    topics = []
    for topic in topics_result.get("top_10_topics") or []:
        evidence = [
            _hydrate_item(item, comments, posts_by_id)
            for item in topic.get("evidence_comments") or []
            if isinstance(item, dict)
        ]
        topics.append({**topic, "evidence_comments": evidence})

    verbatims = [
        _hydrate_item(item, comments, posts_by_id)
        for item in topics_result.get("top_10_verbatims") or []
        if isinstance(item, dict)
    ]

    return {**topics_result, "top_10_topics": topics, "top_10_verbatims": verbatims}


async def _latest_accuracy() -> dict | None:
    """Lần chấm độ chính xác gần nhất (ai_accuracy_runs), hoặc None - report vẫn tạo được khi Supabase lỗi."""
    try:
        from app.services.platform_config_db import get_latest_accuracy_run

        run = await get_latest_accuracy_run()
    except Exception as exc:  # noqa: BLE001 - thiếu ghi chú độ chính xác không được chặn cả report
        logger.warning("report_accuracy_unavailable", error=str(exc))
        return None
    if not run:
        return None
    return {"labeled_at": str(run["labeled_at"]), "sample_size": run["sample_size"], **run["metrics"]}


async def get_movie_for_report(movie_id: str) -> dict | None:
    rows = await d1_query(
        "SELECT id, title, director, `cast`, distributor, released_at, description FROM movies WHERE id = ?", [movie_id]
    )
    return rows[0] if rows else None


async def generate_report_for_movie(movie: dict, *, dry_run: bool = False) -> ReportResult:
    movie_id, movie_title = movie["id"], movie["title"]

    counts = await get_movie_sentiment_counts(movie_id)
    total_classified = sum(counts.values())
    if total_classified < MIN_COMMENTS_FOR_REPORT:
        logger.info("report_skip_insufficient_data", movie_id=movie_id, classified=total_classified)
        return "insufficient_data"

    report_sample = await get_report_sample_for_movie(movie_id)
    sample, posts = report_sample["comments"], report_sample["posts"]
    post_count = len({c["post_id"] for c in sample} | {p["id"] for p in posts})

    topics_result = await generate_topics_and_verbatims(movie_title, sample, posts, movie=movie)
    if topics_result is None:
        logger.warning("report_skip_topics_failed", movie_id=movie_id)
        return "topics_failed"

    topics_result = hydrate_report_comments(topics_result, sample, posts)
    aspect_stats = await get_movie_aspect_stats(movie_id)
    accuracy = await _latest_accuracy()

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
        aspect_stats=aspect_stats,
        movie=movie,
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
        # Đếm thẳng trên mọi comment đã gán khía cạnh (không phải mẫu) - xem get_movie_aspect_stats.
        "aspects": aspect_stats,
        # Để người đọc biết topic/verbatim đến từ mẫu nào, và máy chấm đúng bao nhiêu (scripts/accuracy_sample.py).
        "sampling": {
            "classified_comments": total_classified,
            "sample_comments": len(sample),
            "sample_posts": len(posts),
            "method": "random",
        },
        "accuracy": accuracy,
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
