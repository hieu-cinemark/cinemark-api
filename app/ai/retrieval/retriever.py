import asyncio

from app.ai.retrieval.base import Embedder, ScoredDocument, VectorStore


class VectorRetriever:
    """
    Lấy vector từ vector store và trả về các document có điểm số cao nhất.
    """

    def __init__(self, embedder: Embedder, store: VectorStore) -> None:
        self._embedder = embedder
        self._store = store

    async def retrieve(self, query: str, *, limit: int = 8, filters: dict | None = None) -> list[ScoredDocument]:
        vector = (await asyncio.to_thread(self._embedder.embed, [query]))[0]
        return await self._store.search(vector, limit=limit, filters=filters)
