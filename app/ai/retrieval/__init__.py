"""Truy hồi ngữ cảnh cho LLM (RAG).

Hiện chỉ có interface trong `base.py`. Khi thêm RAG: cài đặt `Embedder` (vd. bọc `app.ai.local.embedding`), một
`VectorStore` (pgvector, Vectorize, R2 + chỉ mục...) và `Retriever`, rồi chèn kết quả vào prompt qua `build_context`.
"""

from app.ai.retrieval.base import Document, Embedder, Retriever, ScoredDocument, VectorStore, build_context

__all__ = ["Document", "Embedder", "Retriever", "ScoredDocument", "VectorStore", "build_context"]
