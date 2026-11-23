"""Truy hồi ngữ cảnh cho LLM (RAG).

Hiện chỉ có interface trong `base.py`. Kho vector đã chọn là Qdrant (chưa cài đặt). Khi thêm RAG: cài đặt `Embedder` (bọc
`app.ai.local.embedding`), `QdrantVectorStore` và `Retriever`, rồi chèn kết quả vào prompt qua `build_context`.
Xem `README.md` cùng thư mục.
"""

from app.ai.retrieval.base import Document, Embedder, Retriever, ScoredDocument, VectorStore, build_context

__all__ = ["Document", "Embedder", "Retriever", "ScoredDocument", "VectorStore", "build_context"]
