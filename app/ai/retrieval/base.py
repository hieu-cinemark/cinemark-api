"""Interface cho RAG: tài liệu, embedding, kho vector và bộ truy hồi."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class Document:
    """Một đoạn văn bản có thể đưa vào ngữ cảnh (bài viết, bình luận, báo, ghi chú...)."""

    id: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScoredDocument:
    document: Document
    score: float


class Embedder(Protocol):
    """Biến văn bản thành vector đã chuẩn hoá (tích vô hướng = cosine)."""

    dim: int

    def embed(self, texts: Sequence[str]) -> list[list[float]]: ...


class VectorStore(Protocol):
    """Lưu và tìm vector theo độ tương đồng, lọc được theo metadata (vd. movie_id, platform)."""

    async def upsert(self, documents: Sequence[Document], vectors: Sequence[Sequence[float]]) -> None: ...

    async def search(
        self, vector: Sequence[float], *, limit: int, filters: dict[str, Any] | None = None
    ) -> list[ScoredDocument]: ...


class Retriever(Protocol):
    """Trả các tài liệu liên quan nhất tới câu hỏi."""

    async def retrieve(
        self, query: str, *, limit: int = 8, filters: dict[str, Any] | None = None
    ) -> list[ScoredDocument]: ...


def build_context(results: Sequence[ScoredDocument], *, max_chars: int = 6000) -> str:
    """Ghép tài liệu thành khối ngữ cảnh đánh số [1], [2]... để chèn vào prompt, dừng khi vượt `max_chars`."""
    parts: list[str] = []
    used = 0
    for index, item in enumerate(results, start=1):
        block = f"[{index}] {item.document.text.strip()}"
        if used + len(block) > max_chars:
            break
        parts.append(block)
        used += len(block)
    return "\n\n".join(parts)
