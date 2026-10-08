"""Các lời gọi LLM đứng sau scripts/generate_social_topic_reports.py - cùng cách tách
hai lời gọi và cùng prompt dù chạy trên provider nào (xem docstring module của
app/ai/prompts/report.py để biết vì sao tách hai lời gọi). Cả hai đều fail open
(trả về None khi có bất kỳ lỗi nào), cùng quy ước với mọi bộ phân loại trong
codebase - bên gọi tự quyết "lần này không có report" nghĩa là gì.

Mặc định Kira viết report (Bee hết số dư từ 2026-10-03); tab AI settings trên
dashboard chọn provider (active_report_provider() trong app/ai/kira.py, mặc định
"kira"). Chọn "bee" thì Bee viết trước, và khi lời gọi Bee lỗi hoặc trả về thứ
không dùng được (lỗi, JSON bị cắt hoặc sai cấu trúc) thì cùng prompt đó được gửi
sang Kira. force=True của call_kira bỏ qua nút bật/tắt của các bộ phân loại ingest,
giống các lời gọi do người vận hành bấm trong app/ai/tasks/import_parser.py - mỗi
lần chạy report là một yêu cầu tường minh."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, TypeVar

from app.ai.bee import call_bee
from app.ai.kira import active_report_provider, call_kira, parse_json_response
from app.ai.movie_context import movie_context_block
from app.ai.prompts.report import (
    NARRATIVE_DATA_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    TOPICS_DATA_PROMPT,
    TOPICS_SYSTEM_PROMPT,
)
from app.core.logging import get_logger
from app.services.d1 import REPORT_POST_TEXT_CHARS

logger = get_logger(__name__)

T = TypeVar("T")


async def _call_provider(
    provider: str, *, task: str, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float
) -> str:
    if provider == "kira":
        return await call_kira(
            task=task,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            force=True,
        )
    return await call_bee(
        task=task,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )


async def _generate(
    *, task: str, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float, parse: Callable[[str], T]
) -> tuple[T, str]:
    """Mặc định chỉ dùng Kira; khi dashboard chọn Bee cho report thì Bee trước, Kira dự
    phòng. `parse` kiểm tra câu trả lời và raise khi không dùng được, nên câu trả lời
    bị cắt/sai cấu trúc cũng được chuyển sang dự phòng, không chỉ khi lời gọi lỗi. Trả
    về (kết quả đã parse, provider đã tạo ra nó); raise lỗi cuối cùng khi mọi provider
    đều lỗi."""
    providers = ["kira"] if await active_report_provider() == "kira" else ["bee", "kira"]
    last_error: Exception | None = None
    for provider in providers:
        try:
            response = await _call_provider(
                provider,
                task=task,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return parse(response), provider
        except Exception as exc:
            last_error = exc
            logger.warning("report_provider_failed", task=task, provider=provider, error=str(exc))
    assert last_error is not None
    raise last_error


def _parse_topics(response: str) -> dict[str, Any]:
    parsed = parse_json_response(response)
    if not isinstance(parsed, dict) or "top_10_topics" not in parsed or "top_10_verbatims" not in parsed:
        raise ValueError(f"unexpected topics shape: {json.dumps(parsed)[:200]!r}")
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

    def add_post(post_id: str, row: dict[str, Any]) -> None:
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
                "text": _clip(row.get("post_content"), REPORT_POST_TEXT_CHARS),
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
            add_post(post_id, comment)

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
    kèm "provider" ("bee" hoặc "kira", bên nào đã viết), hoặc None khi cả hai đều lỗi."""
    posts_payload, comments_payload, aliases = build_topics_payload(comments, posts or [])
    prompt = TOPICS_DATA_PROMPT.format(
        movie_info=movie_context_block(movie or {"title": movie_title}),
        posts_json=json.dumps(posts_payload, ensure_ascii=False),
        comments_json=json.dumps(comments_payload, ensure_ascii=False),
    )
    try:
        # Đã xác nhận thực tế: 8000 (ngân sách cũ của bản Kira trước đây) làm Sonnet 5 bị
        # cắt giữa chừng JSON (finish_reason="length") với một phim 146 comment - đầu ra của
        # Sonnet cho cùng cấu trúc tối đa 10 topic x evidence_comments cộng tối đa 10
        # verbatim dài dòng hơn hẳn model mà Kira từng chạy. Sonnet 5 hỗ trợ cửa sổ đầu ra
        # lớn hơn nhiều, nên có dư địa thật để chi ở đây thay vì cắt bớt prompt/cấu trúc.
        # Đã nâng lên 150000 (2026-10-04) khi Kira viết report - chưa xác nhận model của
        # Kira chấp nhận ngân sách đầu ra lớn vậy; nếu mọi lời gọi topics bắt đầu lỗi thì
        # giảm con số này trước tiên.
        #
        # temperature=0.3 chứ không phải 0.0: đã xác nhận thực tế là Beeknoee cache theo
        # (model, messages, temperature) nhưng KHÔNG theo max_tokens - lời gọi đầu tiên (bị
        # cắt, max_tokens=8000) với đúng prompt này ở temperature=0 đã bị cache, và mọi lần
        # thử lại sau đó ở temperature=0 cứ trả lại nguyên câu trả lời bị cắt đó dù sau này
        # max_tokens được nâng cao đến đâu. temperature khác 0 tránh làm "đầu độc" cache như
        # vậy nếu chuyện này lặp lại với prompt nào đó - và cũng tự nhiên hơn cho một tác vụ
        # viết so với tính tất định tuyệt đối. Giữ nguyên khi provider đang dùng là Kira -
        # chưa có bằng chứng backend của nó không cache tương tự, và temperature khác 0 vốn
        # là mặc định hợp lý cho tác vụ này.
        parsed, provider = await _generate(
            task="topics",
            system_prompt=TOPICS_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=150000,
            temperature=0.3,
            parse=_parse_topics,
        )
        return {**_restore_ids(parsed, aliases), "provider": provider}
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
        # được nâng so với ngân sách Kira cũ cũng vì lý do reasoning token ăn mất ngân sách,
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
