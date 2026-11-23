from __future__ import annotations

import asyncio

from qdrant_client import AsyncQdrantClient

from app.ai.retrieval.e5_embedder import E5Embedder
from app.ai.retrieval.qdrant_store import QdrantVectorStore, ensure_collection
from app.ai.retrieval.retriever import VectorRetriever
from app.core.config import settings

_client: AsyncQdrantClient | None = None
_retriever: VectorRetriever | None = None
_lock = asyncio.Lock()


def new_client() -> AsyncQdrantClient:
    return AsyncQdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key)


async def get_retriever() -> VectorRetriever:
    global _client, _retriever
    if _retriever is not None:
        return _retriever
    async with _lock:
        if _retriever is None:
            client = new_client()
            await ensure_collection(client)
            _client, _retriever = client, VectorRetriever(E5Embedder(), QdrantVectorStore(client))
    return _retriever


async def close_retriever() -> None:
    global _client, _retriever
    client, _client, _retriever = _client, None, None
    if client is not None:
        await client.close()