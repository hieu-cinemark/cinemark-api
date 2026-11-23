"""Kho vector Qdrant + Retriever: chạy trên Qdrant in-memory (không cần container) với embedder giả (không cần torch).

Chạy: .venv/bin/python -m pytest tests/test_qdrant_store.py -q -p no:cacheprovider
Chạy trên Qdrant thật (container, collection tạm tự xoá; kiểm tra thêm chỉ mục payload mà in-memory không giữ):
    QDRANT_URL=http://localhost:6333 .venv/bin/python -m pytest tests/test_qdrant_store.py -q -p no:cacheprovider
Thử với model e5 thật: RUN_E5=1 .venv/bin/python -m pytest tests/test_qdrant_store.py -q -k real_e5
"""

import asyncio
import hashlib
import os
import uuid

import numpy as np
import pytest
from qdrant_client import AsyncQdrantClient

from app.ai.local.embedding import DIM
from app.ai.retrieval import qdrant_store
from app.ai.retrieval.base import Document, build_context
from app.ai.retrieval.retriever import VectorRetriever

# Chấp nhận cả tên cũ (QdRantVectorStore) lẫn tên đã sửa (QdrantVectorStore).
Store = getattr(qdrant_store, "QdrantVectorStore", None) or qdrant_store.QdRantVectorStore


class FakeEmbedder:
    """Vector giả xác định theo từ: câu chứa cùng từ thì gần nhau; đã chuẩn hoá nên tích vô hướng = cosine."""

    dim = DIM

    def embed(self, texts):
        out = []
        for text in texts:
            vec = np.zeros(DIM, dtype=np.float32)
            for word in text.lower().split():
                vec[int(hashlib.md5(word.encode()).hexdigest(), 16) % DIM] += 1.0
            norm = np.linalg.norm(vec) or 1.0
            out.append((vec / norm).tolist())
        return out


# Một event loop dùng chung: client async nối tới server thật không sống qua nhiều lần asyncio.run().
_LOOP = asyncio.new_event_loop()
QDRANT_URL = os.getenv("QDRANT_URL")


def run(coro):
    return _LOOP.run_until_complete(coro)


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setattr(
        qdrant_store, "COLLECTION", f"test_{uuid.uuid4().hex[:8]}"
    )  # không đụng collection "comments" thật
    client = AsyncQdrantClient(url=QDRANT_URL) if QDRANT_URL else AsyncQdrantClient(":memory:")
    run(qdrant_store.ensure_collection(client))
    yield Store(client), client
    if QDRANT_URL:
        run(client.delete_collection(qdrant_store.COLLECTION))
    run(client.close())


def docs():
    return [
        Document("c1", "phim hay quá diễn viên đẹp", {"movie_id": "m1", "platform": "facebook", "sentiment": "pos"}),
        Document("c2", "kịch bản dở diễn viên tệ", {"movie_id": "m1", "platform": "tiktok", "sentiment": "neg"}),
        Document("c3", "nhạc phim rất cuốn", {"movie_id": "m2", "platform": "facebook", "sentiment": "pos"}),
    ]


def fill(store_pair):
    store, _ = store_pair
    embedder = FakeEmbedder()
    items = docs()
    run(store.upsert(items, embedder.embed([d.text for d in items])))
    return embedder


def test_point_id_is_deterministic_uuid():
    first = qdrant_store.point_id("comment-1")
    assert first == qdrant_store.point_id("comment-1") != qdrant_store.point_id("comment-2")
    assert len(first) == 36 and first.count("-") == 4


def test_ensure_collection_is_idempotent(store):
    _, client = store
    run(qdrant_store.ensure_collection(client))  # gọi lần hai không được lỗi
    info = run(client.get_collection(qdrant_store.COLLECTION))
    assert info.config.params.vectors.size == DIM


@pytest.mark.skipif(not QDRANT_URL, reason="Qdrant in-memory không giữ chỉ mục payload (đặt QDRANT_URL)")
def test_ensure_collection_creates_payload_indexes(store):
    info = run(store[1].get_collection(qdrant_store.COLLECTION))
    assert {"movie_id", "platform", "sentiment", "posted_at"} <= set(
        info.payload_schema
    )  # "sentiment" viết đúng chính tả


def test_search_ranks_by_similarity_and_returns_text(store):
    embedder = fill(store)
    found = run(store[0].search(embedder.embed(["diễn viên đẹp"])[0], limit=3))
    assert found[0].document.id == "c1" and found[0].document.text == "phim hay quá diễn viên đẹp"
    assert found[0].score >= found[1].score >= found[2].score
    assert found[0].document.metadata["movie_id"] == "m1"
    assert "text" not in found[0].document.metadata  # text tách khỏi metadata


def test_search_filters_by_metadata(store):
    embedder = fill(store)
    vector = embedder.embed(["diễn viên"])[0]
    only_m1 = run(store[0].search(vector, limit=10, filters={"movie_id": "m1"}))
    assert {r.document.id for r in only_m1} == {"c1", "c2"}
    both = run(store[0].search(vector, limit=10, filters={"movie_id": "m1", "platform": "tiktok"}))
    assert [r.document.id for r in both] == ["c2"]
    assert run(store[0].search(vector, limit=10, filters={"movie_id": "khong-co"})) == []


def test_upsert_same_comment_twice_keeps_one_point(store):
    embedder = fill(store)
    again = [Document("c1", "đã sửa nội dung", {"movie_id": "m1", "platform": "facebook", "sentiment": "pos"})]
    run(store[0].upsert(again, embedder.embed(["đã sửa nội dung"])))
    assert run(store[1].count(qdrant_store.COLLECTION)).count == 3
    top = run(store[0].search(embedder.embed(["đã sửa nội dung"])[0], limit=1))[0]
    assert top.document.id == "c1" and top.document.text == "đã sửa nội dung"


def test_upsert_rejects_mismatched_lengths(store):
    with pytest.raises(ValueError):
        run(store[0].upsert(docs(), [[0.0] * DIM]))


def test_upsert_handles_more_than_one_batch(store):
    embedder = FakeEmbedder()
    count = qdrant_store.BATCH_SIZE * 2 + 5
    many = [Document(f"x{i}", f"bình luận số {i}", {"movie_id": "m9"}) for i in range(count)]
    run(store[0].upsert(many, embedder.embed([d.text for d in many])))
    assert run(store[1].count(qdrant_store.COLLECTION)).count == count


def test_retriever_embeds_query_filters_and_builds_context(store):
    fill(store)
    retriever = VectorRetriever(FakeEmbedder(), store[0])
    results = run(retriever.retrieve("diễn viên đẹp", limit=2, filters={"movie_id": "m1"}))
    assert results[0].document.id == "c1" and len(results) == 2
    context = build_context(results, max_chars=500)
    assert context.startswith("[1] phim hay quá") and "[2]" in context


@pytest.mark.skipif(not os.getenv("RUN_E5"), reason="cần tải model e5 (RUN_E5=1)")
def test_real_e5_embedder_matches_dim_and_is_normalised():
    from app.ai.retrieval.e5_embedder import E5Embedder

    vectors = E5Embedder().embed(["phim hay quá", "diễn viên diễn tệ"])
    assert len(vectors) == 2 and len(vectors[0]) == DIM
    assert abs(float(np.dot(vectors[0], vectors[0])) - 1.0) < 1e-3
    assert E5Embedder().embed([]) == []
