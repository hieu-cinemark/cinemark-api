from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

from qdrant_client import AsyncQdrantClient, models

from app.ai.local.embedding import DIM
from app.ai.retrieval.base import Document, ScoredDocument

COLLECTION = "comments"
_NAMESPACE = uuid.UUID("6f1c2a52-0000-4000-8000-000000000001")  # cố định để upsert lại cùng một điểm
BATCH_SIZE = 256

def point_id(comment_id: str) -> str:
    """Tạo point_id từ comment_id để upsert lại cùng một điểm."""
    return str(uuid.uuid5(_NAMESPACE, comment_id))

async def ensure_collection(client: AsyncQdrantClient) -> None:
    """Tạo collection nếu chưa có."""
    if not await client.collection_exists(COLLECTION):
        await client.create_collection(
            COLLECTION,
            vectors_config=models.VectorParams(
                size=DIM, 
                distance=models.Distance.COSINE,
                datatype=models.Datatype.FLOAT16,
                on_disk=True
            ),
            quantization_config=models.ScalarQuantization(
                scalar=models.ScalarQuantizationConfig(
                    type=models.ScalarType.INT8,
                    always_ram=True
                )
            )
    )
    info = await client.get_collection(COLLECTION)
    indexes = {
        "movie_id": models.PayloadSchemaType.KEYWORD,
        "platform": models.PayloadSchemaType.KEYWORD,
        "sentiment": models.PayloadSchemaType.KEYWORD,
        "posted_at": models.PayloadSchemaType.DATETIME,
    }

    for field, schema in indexes.items():
        if field not in info.payload_schema:
            await client.create_payload_index(
                COLLECTION,
                field_name=field,
                field_schema=schema,
                wait=True,
            )


class QdrantVectorStore:
    """
        Vector store sử dụng Qdrant
    """
    def __init__(self, client: AsyncQdrantClient) -> None:
        self._client = client
        
    async def upsert(self, documents: Sequence[Document], vectors: Sequence[Sequence[float]]) -> None:
        if len(documents) != len(vectors):
            raise ValueError("Documents and vectors must match")
        
        for start in range(0, len(documents), BATCH_SIZE):
            batch_docs = documents[start:start + BATCH_SIZE]
            batch_vectors = vectors[start:start + BATCH_SIZE]

            points = [
                models.PointStruct(
                    id=point_id(doc.id),
                    vector=list(vec),
                    payload={
                        **doc.metadata,
                        "comment_id": doc.id,
                        "text": doc.text,
                    },
                )
                for doc, vec in zip(
                    batch_docs,
                    batch_vectors,
                    strict=True,
                )
            ]

            await self._client.upsert(
                COLLECTION,
                points=points,
                wait=True,
            )
        
    async def search(self, vector: Sequence[float], *, limit: int, filters: dict[str, Any] | None = None) -> list[ScoredDocument]:
        query_filter = (
            models.Filter(must=[models.FieldCondition(key=k, match=models.MatchValue(value=v)) for k, v in filters.items()]) if filters else None
        )
        result = await self._client.query_points(COLLECTION, query=list(vector), limit=limit, query_filter=query_filter, with_payload=True)
        out: list[ScoredDocument] = []
        for hit in result.points:
            payload = dict(hit.payload or {})
            text = str(payload.pop("text", ""))
            out.append(ScoredDocument(Document(id=str(payload.get("comment_id", hit.id)), text=text, metadata=payload), float(hit.score)))
        return out