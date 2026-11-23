# app/api/routes/rag.py
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.core.errors import UpstreamError
from app.core.logging import get_logger
from app.services.rag.ask_comments import ask_comments

logger = get_logger(__name__)
router = APIRouter(prefix="/rag", tags=["rag"])


class AskCommentsRequest(BaseModel):
    movie_id: str = Field(min_length=1)
    question: str = Field(min_length=3, max_length=500)
    platform: Literal["facebook", "threads", "tiktok", "youtube"] | None = None
    sentiment: Literal["positive", "neutral", "negative"] | None = None
    limit: int = Field(default=8, ge=1, le=20)


@router.post("/ask-comments")
async def ask_comments_route(body: AskCommentsRequest) -> dict[str, Any]:
    try:
        return await ask_comments(body.question, movie_id=body.movie_id, platform=body.platform,
                                  sentiment=body.sentiment, limit=body.limit)
    except RuntimeError as exc:  # call_llm: AI tắt/cấu hình sai/provider lỗi
        raise UpstreamError(f"AI could not answer: {exc}") from exc
    except Exception as exc:
        logger.warning("rag_vector_store_failed", error=exc)
        raise UpstreamError("Vector store unavailable") from exc