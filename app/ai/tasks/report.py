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
from app.ai.prompts.report import (
    NARRATIVE_DATA_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    TOPICS_DATA_PROMPT,
    TOPICS_SYSTEM_PROMPT,
)
from app.core.logging import get_logger

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


async def generate_topics_and_verbatims(movie_title: str, comments: list[dict[str, Any]]) -> dict[str, Any] | None:
    """`comments` là các dòng của get_comment_sample_for_movie() (id/message/
    reactions_count/sentiment). Trả về {"top_10_topics": [...], "top_10_verbatims":
    [...]} đúng cấu trúc ReportData của frontend, kèm "provider" ("bee" hoặc "kira",
    bên nào đã viết), hoặc None khi cả hai đều lỗi."""
    comments_payload = [
        {"id": c["id"], "message": c["message"], "likes": c.get("reactions_count") or 0, "sentiment": c["sentiment"]}
        for c in comments
    ]
    prompt = TOPICS_DATA_PROMPT.format(
        movie_title=movie_title,
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
        return {**parsed, "provider": provider}
    except Exception as exc:
        logger.warning("report_topics_failed", movie_title=movie_title, error=str(exc))
        return None


async def generate_narrative(
    movie_title: str,
    *,
    positive_percent: float,
    negative_percent: float,
    neutral_percent: float,
    topic_names: list[str],
) -> str | None:
    """Trả về đoạn "analysis", hoặc None khi lỗi - bên gọi nên dùng chuỗi rỗng thay vì
    chặn cả report."""
    prompt = NARRATIVE_DATA_PROMPT.format(
        movie_title=movie_title,
        positive_percent=positive_percent,
        negative_percent=negative_percent,
        neutral_percent=neutral_percent,
        topic_names="\n".join(f"- {name}" for name in topic_names) or "N/A",
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
