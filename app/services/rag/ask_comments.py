# app/services/rag/__init__.py  (file rỗng)
# app/services/rag/ask_comments.py
from __future__ import annotations

from typing import Any

from app.ai.prompts.ask_comments import ASK_COMMENTS_SYSTEM_PROMPT
from app.ai.retrieval.base import build_context
from app.ai.retrieval.factory import get_retriever
from app.ai.runtime import call_llm
from app.ai.tasks.ask_comments import NO_DATA, build_user_prompt, cited_sources, fit_sources, strip_invalid_citations
from app.core.logging import get_logger

logger = get_logger(__name__)
MIN_SCORE = 0.0   # chỉnh sau khi đo trên dữ liệu thật
CONTEXT_CHARS = 6000


async def ask_comments(question: str, *, movie_id: str, platform: str | None = None,
                       sentiment: str | None = None, limit: int = 8) -> dict[str, Any]:
    filters = {k: v for k, v in {"movie_id": movie_id, "platform": platform, "sentiment": sentiment}.items() if v}
    retriever = await get_retriever()
    found = [r for r in await retriever.retrieve(question, limit=limit, filters=filters) if r.score >= MIN_SCORE]
    sources = fit_sources(found, CONTEXT_CHARS)
    if not sources:
        return {"answer": NO_DATA, "sources": []}
    raw = await call_llm(
        task="ask_comments", system_prompt=ASK_COMMENTS_SYSTEM_PROMPT,
        user_prompt=build_user_prompt(question, build_context(sources, max_chars=CONTEXT_CHARS)),
        max_tokens=700, temperature=0.2,
        force=True,  # người dùng bấm hỏi: chạy cả khi công tắc AI chung đang tắt
    )
    answer = strip_invalid_citations(raw.strip(), len(sources))
    logger.info("rag_answered", movie_id=movie_id, sources=len(sources), cited=len(cited_sources(answer, len(sources))))
    return {"answer": answer, "sources": [
        {"index": i, "id": s.document.id, "text": s.document.text, "score": round(s.score, 4),
         "platform": s.document.metadata.get("platform"), "sentiment": s.document.metadata.get("sentiment")}
        for i, s in enumerate(sources, start=1)]}