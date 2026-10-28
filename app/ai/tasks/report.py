"""Các lời gọi LLM đứng sau scripts/jobs/generate_social_topic_reports.py - cùng cách tách
hai lời gọi và cùng prompt dù chạy trên provider nào (xem docstring module của
app/ai/prompts/report.py để biết vì sao tách hai lời gọi). Cả hai đều fail open
(trả về None khi có bất kỳ lỗi nào), cùng quy ước với mọi bộ phân loại trong
codebase - bên gọi tự quyết "lần này không có report" nghĩa là gì.

Bee viết report (Kira đã bỏ từ 2026-10-21). Lời gọi lỗi hoặc trả về thứ không dùng được (JSON bị cắt hoặc sai cấu
trúc) thì thử lại Bee một lần với temperature lệch đi một chút - Beeknoee cache theo (model, messages, temperature), giữ
nguyên temperature thì lần thử lại chỉ nhận lại đúng câu trả lời hỏng đã cache."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any, TypeVar

from app.ai.bee import call_bee
from app.ai.client import JSON_FENCE_RE
from app.ai.movie_context import movie_context_block
from app.ai.prompts.report import (
    MERGE_DATA_PROMPT,
    MERGE_SYSTEM_PROMPT,
    NARRATIVE_DATA_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    TOPICS_BATCH_NOTE,
    TOPICS_DATA_PROMPT,
    TOPICS_SYSTEM_PROMPT,
)
from app.ai.runtime import parse_json_response
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import REPORT_POST_TEXT_CHARS

logger = get_logger(__name__)

T = TypeVar("T")


async def _generate(
    *, task: str, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float, parse: Callable[[str], T]
) -> tuple[T, str]:
    """Bee viết, thử lại một lần khi lỗi. `parse` kiểm tra câu trả lời và raise khi không dùng được, nên câu trả lời bị
    cắt/sai cấu trúc cũng được thử lại, không chỉ khi lời gọi lỗi. Trả về (kết quả đã parse, "bee"); raise lỗi cuối cùng
    khi cả hai lần đều lỗi."""
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            response = await call_bee(
                task=task,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
                temperature=temperature + 0.1 * attempt,
                model=settings.bee_report_model.strip() or None,
            )
            return parse(response), "bee"
        except Exception as exc:
            last_error = exc
            logger.warning("report_provider_failed", task=task, provider="bee", attempt=attempt + 1, error=str(exc))
    assert last_error is not None
    raise last_error


def _first_json_object(response: str) -> Any:
    """Như parse_json_response, nhưng model viết thêm chữ / một object thứ hai sau JSON (26/10/2026: "Extra data")
    thì vẫn lấy object đầu tiên."""
    try:
        return parse_json_response(response)
    except json.JSONDecodeError as exc:
        if exc.msg != "Extra data":
            raise
        text = JSON_FENCE_RE.sub("", response.strip())
        return json.JSONDecoder().raw_decode(text)[0]


def _parse_topics(response: str) -> dict[str, Any]:
    parsed = _first_json_object(response)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("top_10_topics"), list) or not parsed["top_10_topics"]:
        raise ValueError(f"unexpected topics shape: {json.dumps(parsed)[:200]!r}")
    # Thiếu verbatim (một phần của mẫu lớn, 26/10/2026) vẫn dùng được chủ đề; lời gọi gộp chọn verbatim từ các phần khác.
    if not isinstance(parsed.get("top_10_verbatims"), list):
        parsed["top_10_verbatims"] = []
    return parsed


def _parse_narrative(response: str) -> str:
    parsed = parse_json_response(response)
    analysis = parsed.get("analysis") if isinstance(parsed, dict) else None
    if not isinstance(analysis, str) or not analysis.strip():
        raise ValueError(f"unexpected narrative shape: {json.dumps(parsed)[:200]!r}")
    return analysis.strip()


# Comment dài hơn bấy nhiêu ký tự được cắt bớt trong prompt (dashboard vẫn hiện bản đầy đủ - xem
# hydrate_report_comments) - vài comment dài cả nghìn chữ không được chiếm chỗ của hàng chục comment khác.
_COMMENT_TEXT_CHARS = 500
# Bài chỉ có mặt vì chứa comment trong mẫu (không thuộc mẫu bài của khán giả): đủ để biết comment đang trả lời bài gì.
# Mẫu 2.000 comment kéo theo ~420 bài như vậy - để 400 ký tự thì riêng chúng chiếm ~40% prompt (đo 20/10/2026).
_CONTEXT_POST_TEXT_CHARS = 160


def _clip(text: str | None, limit: int) -> str:
    text = " ".join((text or "").split())
    return text[:limit].rstrip() + "…" if len(text) > limit else text


def build_topics_payload(
    comments: list[dict[str, Any]], posts: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, tuple[str, str]]]:
    """(bài viết, comment, bảng id ngắn -> (source, id thật)) đưa cho AI. Bài viết: các bài của khán giả
    (`posts`, từ get_report_sample_for_movie) trước, rồi các bài chứa comment trong mẫu mà chưa có trong đó - để
    mọi post_id của comment đều tra được bài cha. Id thật (UUID ~40 ký tự, lặp ở cả id lẫn post_id của từng
    comment) được thay bằng "p1"/"c1": đo trên D1 thật 2026-10-07, riêng id đã chiếm gần nửa prompt 800 comment."""
    aliases: dict[str, tuple[str, str]] = {}
    post_alias: dict[str, str] = {}
    posts_payload: list[dict[str, Any]] = []

    def add_post(post_id: str, row: dict[str, Any], text_chars: int = REPORT_POST_TEXT_CHARS) -> None:
        alias = f"p{len(posts_payload) + 1}"
        post_alias[post_id] = alias
        aliases[alias] = ("post", post_id)
        posts_payload.append(
            {
                "id": alias,
                "platform": row.get("platform"),
                "author": row.get("post_author"),
                # "channel": studio/trang showbiz/fanpage của phim - chỉ là ngữ cảnh, prompt cấm làm bằng chứng.
                "kind": "channel" if row.get("is_channel") else "audience",
                "text": _clip(row.get("post_content"), text_chars),
                "likes": row.get("post_likes") or 0,
                "comments": row.get("post_comments") or 0,
                "shares": row.get("post_shares") or 0,
            }
        )

    for post in posts:
        if post["id"] not in post_alias:
            add_post(post["id"], post)
    for comment in comments:
        post_id = comment.get("post_id")
        if post_id and post_id not in post_alias and comment.get("post_content"):
            add_post(post_id, comment, _CONTEXT_POST_TEXT_CHARS)

    comments_payload: list[dict[str, Any]] = []
    for index, comment in enumerate(comments, start=1):
        alias = f"c{index}"
        aliases[alias] = ("comment", comment["id"])
        comments_payload.append(
            {
                "id": alias,
                "post_id": post_alias.get(comment.get("post_id") or ""),
                "message": _clip(comment["message"], _COMMENT_TEXT_CHARS),
                "likes": comment.get("reactions_count") or 0,
                "sentiment": comment["sentiment"],
            }
        )
    return posts_payload, comments_payload, aliases


def _restore_ids(parsed: dict[str, Any], aliases: dict[str, tuple[str, str]]) -> dict[str, Any]:
    """Đổi id ngắn AI trả về ("c12"/"p3") lại thành id thật và đặt "source" theo đúng loại của id đó."""

    def restore(item: Any) -> Any:
        if not isinstance(item, dict) or item.get("id") not in aliases:
            return item
        source, real_id = aliases[item["id"]]
        return {**item, "id": real_id, "source": source}

    topics = [
        {**topic, "evidence_comments": [restore(i) for i in topic.get("evidence_comments") or []]}
        if isinstance(topic, dict)
        else topic
        for topic in parsed.get("top_10_topics") or []
    ]
    verbatims = [restore(i) for i in parsed.get("top_10_verbatims") or []]
    return {**parsed, "top_10_topics": topics, "top_10_verbatims": verbatims}


# Mẫu lớn chia phần: mỗi phần tối đa TOPICS_BATCH_COMMENTS comment một lời gọi, rồi một lời gọi gộp. Đo 26/10/2026
# với Trại Buôn Người: gửi cả 2.000 comment + ~570 bài trong một lời gọi thì Bee (claude-haiku-5-5) trả về dở dang
# (690 token, JSON thiếu top_10_verbatims) hoặc lỗi "temperature is deprecated" (bị chuyển sang backend khác).
TOPICS_BATCH_COMMENTS = 500
_BATCH_CONCURRENCY = 2
_BATCH_TOPICS = 8
_BATCH_VERBATIMS = 6
_MERGE_EVIDENCE_CHARS = 220


def _parse_merge(response: str) -> dict[str, Any]:
    parsed = parse_json_response(response)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("top_10_topics"), list):
        raise TypeError(f"unexpected merge shape: {json.dumps(parsed)[:200]!r}")
    return parsed


async def _topics_for_part(
    movie_title: str,
    movie: dict[str, Any] | None,
    comments: list[dict[str, Any]],
    posts: list[dict[str, Any]],
    note: str = "",
) -> dict[str, Any]:
    """Một lời gọi TOPICS trên một phần mẫu; id đã đổi lại thành id thật. Raise khi lỗi."""
    posts_payload, comments_payload, aliases = build_topics_payload(comments, posts)
    prompt = TOPICS_DATA_PROMPT.format(
        movie_info=movie_context_block(movie or {"title": movie_title}),
        posts_json=json.dumps(posts_payload, ensure_ascii=False),
        comments_json=json.dumps(comments_payload, ensure_ascii=False),
    )
    # max_tokens 150000 / temperature 0.3: xem lịch sử ở git blame (Sonnet 5 bị cắt ở 8000; Beeknoee cache theo
    # (model, messages, temperature) nên temperature khác 0 tránh cache vĩnh viễn một câu trả lời hỏng).
    parsed, _provider = await _generate(
        task="topics",
        system_prompt=TOPICS_SYSTEM_PROMPT,
        user_prompt=prompt + note,
        max_tokens=150000,
        temperature=0.3,
        parse=_parse_topics,
    )
    return _restore_ids(parsed, aliases)


async def _merge_parts(movie_title: str, movie: dict[str, Any] | None, parts: list[dict[str, Any]]) -> dict[str, Any]:
    """Gộp kết quả các phần: AI chọn bằng chứng / verbatim theo khoá (e1, v1...), không chép lại chữ - giữ nguyên id
    thật và nội dung gốc để dashboard mở được bài."""
    evidence: dict[str, dict[str, Any]] = {}
    verbatims: dict[str, dict[str, Any]] = {}
    topics_payload: list[dict[str, Any]] = []
    verbatims_payload: list[dict[str, Any]] = []
    for part_no, part in enumerate(parts, start=1):
        for topic in part.get("top_10_topics") or []:
            if not isinstance(topic, dict):
                continue
            keys = []
            for item in topic.get("evidence_comments") or []:
                if not isinstance(item, dict):
                    continue
                key = f"e{len(evidence) + 1}"
                evidence[key] = item
                keys.append(
                    {
                        "key": key,
                        "text": _clip(item.get("text"), _MERGE_EVIDENCE_CHARS),
                        "likes": item.get("likes") or 0,
                    }
                )
            topics_payload.append(
                {
                    "part": part_no,
                    "topic_name": topic.get("topic_name"),
                    "sentiment": topic.get("sentiment"),
                    "insight_summary": topic.get("insight_summary"),
                    "evidence": keys,
                }
            )
        for item in part.get("top_10_verbatims") or []:
            if not isinstance(item, dict):
                continue
            key = f"v{len(verbatims) + 1}"
            verbatims[key] = item
            verbatims_payload.append(
                {
                    "key": key,
                    "text": _clip(item.get("text"), _MERGE_EVIDENCE_CHARS),
                    "likes": item.get("likes") or 0,
                    "why_it_matters": item.get("why_it_matters"),
                }
            )
    prompt = MERGE_DATA_PROMPT.format(
        movie_info=movie_context_block(movie or {"title": movie_title}),
        parts=len(parts),
        topics_json=json.dumps(topics_payload, ensure_ascii=False),
        verbatims_json=json.dumps(verbatims_payload, ensure_ascii=False),
    )
    merged, _provider = await _generate(
        task="topics_merge",
        system_prompt=MERGE_SYSTEM_PROMPT,
        user_prompt=prompt,
        max_tokens=30000,
        temperature=0.3,
        parse=_parse_merge,
    )
    topics = []
    for topic in merged.get("top_10_topics") or []:
        if not isinstance(topic, dict):
            continue
        items = [evidence[k] for k in topic.get("evidence_keys") or [] if isinstance(k, str) and k in evidence]
        if not items:
            continue  # chủ đề không trỏ được bằng chứng thật nào thì bỏ
        topics.append(
            {
                "topic_name": topic.get("topic_name"),
                "sentiment": topic.get("sentiment"),
                "insight_summary": topic.get("insight_summary"),
                "evidence_comments": items[:4],
            }
        )
    picked = []
    for item in merged.get("top_10_verbatims") or []:
        key = item.get("key") if isinstance(item, dict) else None
        if key in verbatims:
            picked.append(
                {**verbatims[key], "why_it_matters": item.get("why_it_matters") or verbatims[key].get("why_it_matters")}
            )
    if not topics:
        raise ValueError("merge returned no topic with valid evidence")
    return {"top_10_topics": topics[:10], "top_10_verbatims": picked[:10]}


async def generate_topics_and_verbatims(
    movie_title: str,
    comments: list[dict[str, Any]],
    posts: list[dict[str, Any]] | None = None,
    *,
    movie: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """`comments` là các dòng của get_report_sample_for_movie()["comments"] (id/post_id/message/
    reactions_count/sentiment + trường của bài cha), `posts` là get_report_sample_for_movie()["posts"]
    (bài của khán giả). AI đọc cả hai: bài viết vừa là ngữ cảnh cho comment vừa là
    bằng chứng riêng. Trả về {"top_10_topics": [...], "top_10_verbatims": [...]} đúng cấu
    trúc ReportData của frontend (mỗi bằng chứng có "source": "post" | "comment" và id thật),
    kèm "provider" ("bee"), hoặc None khi lỗi.

    Mẫu nhỏ (<= TOPICS_BATCH_COMMENTS comment): một lời gọi như trước. Mẫu lớn: chia phần (comment rải đều theo
    like - mẫu đã xếp theo like giảm dần - bài chia vòng tròn), mỗi phần một lời gọi, rồi một lời gọi gộp. Một vài
    phần lỗi vẫn gộp từ các phần còn lại; chỉ một phần thành công thì dùng thẳng phần đó."""
    posts = posts or []
    try:
        parts_count = max(1, -(-len(comments) // TOPICS_BATCH_COMMENTS))
        if parts_count == 1:
            return {**await _topics_for_part(movie_title, movie, comments, posts), "provider": "bee"}

        semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)

        async def run_part(index: int) -> dict[str, Any] | None:
            note = TOPICS_BATCH_NOTE.format(
                part=index + 1, parts=parts_count, max_topics=_BATCH_TOPICS, max_verbatims=_BATCH_VERBATIMS
            )
            async with semaphore:
                try:
                    return await _topics_for_part(
                        movie_title, movie, comments[index::parts_count], posts[index::parts_count], note
                    )
                except Exception as exc:
                    logger.warning("report_topics_part_failed", movie_title=movie_title, part=index + 1, error=str(exc))
                    return None

        parts = [p for p in await asyncio.gather(*(run_part(i) for i in range(parts_count))) if p]
        logger.info("report_topics_parts_done", movie_title=movie_title, ok=len(parts), total=parts_count)
        if not parts:
            raise RuntimeError("every topics part failed")
        if len(parts) == 1:
            return {**parts[0], "provider": "bee"}
        return {**await _merge_parts(movie_title, movie, parts), "provider": "bee"}
    except Exception as exc:
        logger.warning("report_topics_failed", movie_title=movie_title, error=str(exc))
        return None


def _aspect_prompt_lines(stats: dict[str, Any]) -> tuple[str, str]:
    aspects = [
        f"- {a['label']}: {a['positive']}% praise, mentioned in {a['mention_share']}% of comments"
        + (
            f" (slipping: {a['trend']['from']}% -> {a['trend']['to']}% over the last {stats['trend_days']} days)"
            if a["slipping"]
            else ""
        )
        for a in stats.get("aspects") or []
        if a.get("positive") is not None
    ]
    stage = stats.get("stage") or {}
    watched, waiting = stage.get("da_xem", 0), stage.get("hong", 0)
    stage_lines = (
        [f"- Has watched: {watched} comments; not yet watched: {waiting} comments"] if watched + waiting else []
    )
    expectation = stats.get("expectation")
    if expectation:
        stage_lines.append(
            f"- Praise share before watching {expectation['hong_positive']}% vs after watching "
            f"{expectation['da_xem_positive']}%"
        )
    return "\n".join(aspects) or "N/A (not enough data)", "\n".join(stage_lines) or "N/A (not enough data)"


async def generate_narrative(
    movie_title: str,
    *,
    positive_percent: float,
    negative_percent: float,
    neutral_percent: float,
    topic_names: list[str],
    aspect_stats: dict[str, Any] | None = None,
    movie: dict[str, Any] | None = None,
) -> str | None:
    """Trả về đoạn "analysis", hoặc None khi lỗi - bên gọi nên dùng chuỗi rỗng thay vì
    chặn cả report. `aspect_stats` là get_movie_aspect_stats() - số thật đưa cho AI diễn giải."""
    aspect_lines, stage_lines = _aspect_prompt_lines(aspect_stats or {})
    prompt = NARRATIVE_DATA_PROMPT.format(
        movie_info=movie_context_block(movie or {"title": movie_title}, logline_chars=200),
        positive_percent=positive_percent,
        negative_percent=negative_percent,
        neutral_percent=neutral_percent,
        topic_names="\n".join(f"- {name}" for name in topic_names) or "N/A",
        aspect_lines=aspect_lines,
        stage_lines=stage_lines,
    )
    try:
        # Cùng lý do temperature=0.3 như lời gọi của generate_topics_and_verbatims phía trên
        # (tránh cache vĩnh viễn một câu trả lời hỏng/bị cắt ở temperature=0). max_tokens
        # được nâng so với ngân sách cũ cũng vì lý do reasoning token ăn mất ngân sách,
        # dù prompt ngắn này chưa từng thấy cần tới.
        analysis, _provider = await _generate(
            task="narrative",
            system_prompt=NARRATIVE_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=4000,
            temperature=0.3,
            parse=_parse_narrative,
        )
        return analysis
    except Exception as exc:
        logger.warning("report_narrative_failed", movie_title=movie_title, error=str(exc))
        return None
