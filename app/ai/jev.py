"""Câu hỏi + chấm nhãn bằng model quyết định của Bee (app/ai/decisions.py - từ 21/10/2026 thay cho gọi thẳng TypeSafe):
cảm xúc / giai đoạn / khía cạnh của bình luận và giá trị bình luận dùng typesafe/jev-1.14 (decisions.comment_model), độ
liên quan và cảm xúc NỘI DUNG BÀI dùng openai/gpt-6-luna-decisions (decisions.post_model).

Chỉ là CA KHÓ: luật (sentiment_rules / relevance_rules) và ViSoBERT (app/ai/visobert.py) chạy trước. Model quyết định đọc
một đoạn văn bản (`state`) và trả lời các câu `choice` (kèm xác suất và độ tự tin); một request = một bình luận/bài, mọi câu
hỏi cùng lúc. Câu trả lời dưới ngưỡng tự tin rơi về nhãn an toàn (neutral / khac / không gắn khía cạnh).
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.ai.aspects import ASPECTS
from app.ai.decisions import comment_model, decide, post_model, usage_totals
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_SENTIMENT = {
    "positive": "Có thái độ tích cực rõ ràng với phim hoặc người trong phim: khen, thích, thấy hay/buồn cười/xúc động, "
    "háo hức muốn đi xem, muốn xem lại",
    "neutral": "Không bày tỏ khen hay chê phim: hỏi thông tin, tag bạn bè, kể chuyện bên lề, bàn chuyện ngoài lề, "
    "quảng cáo/spam, hoặc không đủ rõ",
    "negative": "Có thái độ tiêu cực rõ ràng với phim: chê, thất vọng, thấy dở/nhàm, chỉ trích, kêu gọi tẩy chay",
}
_STAGE = {
    "hong": "Rõ ràng CHƯA xem phim: đang mong chờ, hỏi lịch chiếu, định đi xem, rủ người khác đi xem, hoặc nói sẽ không xem",
    "da_xem": "Rõ ràng ĐÃ xem phim: kể lại cảm nhận sau khi xem, nhắc 'đã coi/xem rồi/vừa xem xong/ra rạp về', nhận xét "
    "chi tiết cảnh phim đã xem",
    "khac": "Không có dấu hiệu nào ở trên: bàn chuyện ngoài lề, hỏi thông tin chung, tag bạn, quảng cáo",
}


def _questions() -> dict[str, dict[str, Any]]:
    questions: dict[str, dict[str, Any]] = {
        "sentiment": {
            "type": "choice",
            "instructions": "Đây là bình luận mạng xã hội tiếng Việt (có teencode, viết tắt) dưới một bài đăng về bộ "
            "phim. Thái độ của người bình luận đối với bộ phim là gì? Nếu không rõ thì chọn neutral.",
            "criteria": _SENTIMENT,
        },
        "stage": {
            "type": "choice",
            "instructions": "Dựa vào bình luận, người viết đã xem bộ phim này chưa? Dấu hiệu đã xem: kể lại/bình luận về "
            "cảnh, khúc, đoạn, diễn xuất, kết phim như người đã xem; nói đã coi, coi rồi, vừa xem. Dấu hiệu chưa xem: "
            "chưa coi, chưa đi xem, hóng, mong chờ, bao giờ chiếu, định đi xem.",
            "criteria": _STAGE,
        },
    }
    for key, (label, hint) in ASPECTS.items():
        questions[f"a_{key}"] = {
            "type": "choice",
            "instructions": f"Bình luận nói gì về khía cạnh '{label}' ({hint}) của phim?",
            "criteria": {
                "khen": "Khen khía cạnh này",
                "che": "Chê khía cạnh này",
                "khong": "Không nhắc tới khía cạnh này",
            },
        }
    return questions


_QUESTIONS = _questions()

# Giá Jev (2026-10): $0,042 / 1 triệu token đầu vào, đầu ra miễn phí - để ước chi phí trong log (jev_usage).
USD_PER_M_INPUT = 0.042


def jev_is_configured() -> bool:
    # Bee được kiểm tra lúc gọi (app/ai/decisions.bee_endpoint) - chưa cấu hình thì lời gọi lỗi và bên gọi fail open.
    return True


def use_jev() -> bool:
    """Việc phân loại (comment, độ liên quan bài, giá trị comment) dùng Jev - Kira chỉ còn việc cần viết chữ (duyệt hashtag
    BFS, sinh câu tìm kiếm, đọc file import, viết report)."""
    return settings.classifier == "jev" and jev_is_configured()


def _label(answers: dict[str, Any]) -> dict[str, Any] | None:
    sentiment = answers.get("sentiment") or {}
    if sentiment.get("choice") not in _SENTIMENT:
        return None
    stage = answers.get("stage") or {}
    aspects = [
        f"{key[2:]}:{'+' if answer.get('choice') == 'khen' else '-'}"
        for key, answer in answers.items()
        if key.startswith("a_")
        and answer.get("choice") in ("khen", "che")
        and float(answer.get("confidence") or 0) >= settings.jev_aspect_min_confidence
    ]
    return {
        "sentiment": sentiment["choice"]
        if float(sentiment.get("confidence") or 0) >= settings.jev_min_confidence
        else "neutral",
        "aspects": aspects,
        "stage": stage.get("choice")
        if stage.get("choice") in _STAGE and float(stage.get("confidence") or 0) >= settings.jev_stage_min_confidence
        else "khac",
    }


async def classify_comments_jev(
    messages: list[str], films: list[dict[str, Any] | None], *, model: str | None = None
) -> list[dict[str, Any] | None]:
    """Cùng độ dài/thứ tự với `messages`: {"sentiment", "aspects", "stage"} hoặc None khi request của câu đó lỗi.
    `model` mặc định là model bình luận (jev-1.14); cảm xúc nội dung bài truyền decisions.post_model(). Chạy song song tối
    đa settings.jev_concurrency request."""
    semaphore = asyncio.Semaphore(max(1, settings.jev_concurrency))
    failures: list[str] = []
    model = model or comment_model()

    async def one(client: httpx.AsyncClient, message: str, film: dict[str, Any] | None) -> dict[str, Any] | None:
        title = (film or {}).get("title")
        state = f"Phim: {title}\nBình luận: {message}" if title else f"Bình luận: {message}"
        async with semaphore:
            try:
                return _label(await decide(model, state, _QUESTIONS, client=client))
            except Exception as exc:  # noqa: BLE001 - câu này để lượt sau
                failures.append(str(exc)[:200])
                return None

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        labels = await asyncio.gather(*(one(client, m, f) for m, f in zip(messages, films, strict=True)))
    logger.info(
        "jev_usage",
        model=model,
        comments=len(messages),
        total_requests=usage_totals["requests"],
        total_input_tokens=usage_totals["input_tokens"],
        total_usd=round(usage_totals["input_tokens"] / 1e6 * USD_PER_M_INPUT, 6),
    )
    if failures:
        logger.warning("jev_comment_failed", failed=len(failures), total=len(messages), error=failures[0])
    return labels


async def ask(
    state: str,
    questions: dict[str, dict[str, Any]],
    *,
    client: httpx.AsyncClient | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Một request: `answers` theo tên câu hỏi (mặc định model bình luận). Lỗi sau 3 lần thử thì raise."""
    return await decide(model or comment_model(), state, questions, client=client)


# --- Độ liên quan của bài (thay Kira ở app/ai/tasks/post_relevance.py khi settings.classifier = "jev") -----------------
_RELEVANCE = {
    "related": "Bài nói về ĐÚNG phim đích với tư cách một bộ phim: trailer, poster, nội dung, ngày chiếu, suất chiếu, rạp, "
    "vé, doanh thu, review/cảm nhận sau khi xem, hậu trường, diễn viên/đoàn phim quảng bá hoặc bàn về phim, clip/edit "
    "từ phim. Tên phim có thể viết tắt, không dấu, sai chính tả, hoặc chỉ ngầm hiểu qua diễn viên kèm ngữ cảnh phim.",
    "not_related": "Bài KHÔNG nói về phim đích: nói về phim khác (kể cả phim trùng/gần tên, phim trong danh sách phim "
    "khác); bài hát/MV/karaoke trùng tên; nội dung tâm linh, tử vi, triết lý sống dùng chữ trong tên phim; dùng chữ "
    "trong tên phim theo nghĩa thường; nội dung nước ngoài chỉ trùng hashtag không dấu; chỉ nhét hashtag phim vào nội "
    "dung khác (bán hàng, vlog, trend, minigame).",
    "uncertain": "Không đủ thông tin để biết bài có nói về phim đích hay không.",
}


async def classify_post_relevance_jev(
    *, content: str, movie: dict[str, Any], keyword: str | None, platform: str | None, other_titles: list[str]
) -> dict[str, Any] | None:
    """{"label": related|not_related|uncertain, "confidence", "reason"} như classify_post_relevance_kira, hoặc None
    khi lỗi (bên gọi giữ bài lại)."""
    from app.ai.movie_context import movie_context_block

    others = ", ".join(title for title in other_titles if title and title != movie.get("title"))
    state = (
        f"PHIM ĐÍCH:\n{movie_context_block(movie, logline_chars=250)}\n"
        f"PHIM KHÁC ĐANG THEO DÕI (bài về các phim này không phải về phim đích): {others or '(không)'}\n"
        f"TÌM THẤY QUA: {keyword or movie.get('title') or ''} (nền tảng {platform or 'không rõ'})\n\n"
        f"BÀI VIẾT:\n{content[:1500]}"
    )
    question = {
        "relevance": {
            "type": "choice",
            "instructions": "Bài viết mạng xã hội này có nói về PHIM ĐÍCH (bộ phim chiếu rạp ở trên) không?",
            "criteria": _RELEVANCE,
        }
    }
    try:
        answer = (await ask(state, question, model=post_model())).get("relevance") or {}
    except Exception as exc:  # noqa: BLE001 - fail open như Kira
        logger.warning("jev_post_relevance_failed", error=str(exc)[:200])
        return None
    label = answer.get("choice")
    if label not in _RELEVANCE:
        return None
    confidence = float(answer.get("confidence") or 0)
    # Không đủ tự tin thì "uncertain": resolve_relevance xử lý như Kira không kết luận (giữ bài theo luật từ khoá).
    if label != "uncertain" and confidence < settings.jev_min_confidence:
        label = "uncertain"
    return {"label": label, "confidence": confidence, "reason": f"jev {answer.get('choice')} {confidence:.2f}"}


# --- Giá trị comment của bài (thay Kira ở app/services/comment_value.py) -------------------------------------------
# Loại bài -> điểm 0-100 "có đáng cào comment không"; điểm = trung bình theo xác suất từng loại.
COMMENT_VALUE_KINDS: dict[str, tuple[int, str]] = {
    "review": (90, "Review, cảm nhận về phim - người xem sẽ vào bàn luận khen chê"),
    "discussion": (85, "Đặt câu hỏi, tranh luận, so sánh, tin gây tranh cãi về phim hoặc diễn viên"),
    "announcement": (55, "Tin tức, thông báo về phim: lịch chiếu, doanh thu, ra mắt trailer/poster"),
    "promo_content": (45, "Clip/ảnh/hậu trường quảng bá phim của chính phim hoặc trang giải trí"),
    "ad": (10, "Quảng cáo đặt vé, khuyến mãi, bán hàng"),
    "giveaway": (5, "Minigame, giveaway, kêu gọi tag bạn bè nhận quà"),
    "off_topic": (0, "Không thật sự nói về bộ phim"),
}


async def judge_comment_value_jev(title: str, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Cùng dạng kết quả với comment_value._judge_movie: {post_id: {"score", "kind", "reason"}}."""
    question = {
        "kind": {
            "type": "choice",
            "instructions": f"Đây là một bài mạng xã hội về phim '{title}'. Bài thuộc loại nào? (để biết bình luận "
            "bên dưới có giúp hiểu khán giả nghĩ gì về phim không)",
            "criteria": {key: hint for key, (_, hint) in COMMENT_VALUE_KINDS.items()},
        }
    }
    out: dict[str, dict[str, Any]] = {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:

        async def one(row: dict[str, Any]) -> None:
            caption = " ".join(str(row.get("content") or "").split())[:400]
            state = f'[{row.get("platform")}] tác giả "{row.get("author") or "?"}", {row.get("reply_count") or 0} bình luận:\n{caption}'
            try:
                answer = (await ask(state, question, client=client)).get("kind") or {}
            except Exception as exc:  # noqa: BLE001 - bài này chấm ở lượt sau
                logger.warning("jev_comment_value_failed", post_id=row.get("id"), error=str(exc)[:200])
                return
            probs = answer.get("probabilities") or {}
            score = round(
                sum(COMMENT_VALUE_KINDS[k][0] * float(p) for k, p in probs.items() if k in COMMENT_VALUE_KINDS)
            )
            out[row["id"]] = {
                "score": max(0, min(100, score)),
                "kind": str(answer.get("choice") or "")[:30],
                "reason": "jev",
            }

        await asyncio.gather(*(one(row) for row in rows))
    return out
