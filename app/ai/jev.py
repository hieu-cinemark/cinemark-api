"""Gán nhãn comment bằng Jev - mô hình "System One" của TypeSafe AI (api.typesafe.ai, từ 2026-10-13).

Jev không viết chữ: nó đọc một đoạn văn bản (`state`) và trả lời các câu hỏi có kiểu - ở đây đều là `choice` (chọn một
trong các tiêu chí, kèm xác suất từng tiêu chí và độ tự tin). Một request = một comment, mọi câu hỏi cùng lúc: cảm xúc,
giai đoạn khán giả, và mỗi khía cạnh trong app/ai/aspects.ASPECTS một câu khen / chê / không nhắc (Jev không có câu hỏi
chọn nhiều). Nhanh (~0,4 giây/comment, chạy song song) và rẻ hơn Kira nhiều - thay Kira cho comment khi
settings.comment_labeler = "jev" (xem app/ai/tasks/sentiment.py).

Đo 2026-10-13 trên 30 comment thật: cảm xúc trùng Kira 22/30, giai đoạn 20/30 (Kira không phải đáp án chuẩn - nhiều chỗ
lệch Jev hợp lý hơn). Câu trả lời dưới ngưỡng tự tin rơi về nhãn an toàn (neutral / khac / không gắn khía cạnh).
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.ai.aspects import ASPECTS
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


def jev_is_configured() -> bool:
    return bool(settings.jev_api_key)


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


async def classify_comments_jev(messages: list[str], films: list[dict[str, Any] | None]) -> list[dict[str, Any] | None]:
    """Cùng độ dài/thứ tự với `messages`: {"sentiment", "aspects", "stage"} hoặc None khi request của comment đó lỗi
    (bên gọi có thể chuyển sang Kira). Chạy song song tối đa settings.jev_concurrency request."""
    semaphore = asyncio.Semaphore(max(1, settings.jev_concurrency))
    failures: list[str] = []

    async def one(client: httpx.AsyncClient, message: str, film: dict[str, Any] | None) -> dict[str, Any] | None:
        title = (film or {}).get("title")
        state = f"Phim: {title}\nBình luận: {message}" if title else f"Bình luận: {message}"
        body = {"model": settings.jev_model, "state": state, "questions": _QUESTIONS}
        async with semaphore:
            for attempt in range(3):
                try:
                    resp = await client.post(f"{settings.jev_api_base_url.rstrip('/')}/systemone", json=body)
                    if resp.status_code == 429 or resp.status_code >= 500:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    resp.raise_for_status()
                    return _label((resp.json() or {}).get("answers") or {})
                except (httpx.HTTPError, ValueError) as exc:
                    if attempt == 2:
                        failures.append(str(exc)[:200])
                    await asyncio.sleep(1)
            return None

    headers = {"Authorization": f"Bearer {settings.jev_api_key}"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), headers=headers) as client:
        labels = await asyncio.gather(*(one(client, m, f) for m, f in zip(messages, films, strict=True)))
    if failures:
        logger.warning("jev_comment_failed", failed=len(failures), total=len(messages), error=failures[0])
    return labels
