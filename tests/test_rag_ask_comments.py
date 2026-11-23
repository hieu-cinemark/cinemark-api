"""RAG hỏi đáp bình luận: hàm thuần của task, job nạp Qdrant, service, route và factory (Qdrant in-memory, embedder và LLM giả)."""

import asyncio
import hashlib
import sqlite3
from unittest.mock import AsyncMock

import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from qdrant_client import AsyncQdrantClient

from app.ai.local.embedding import DIM
from app.ai.retrieval import factory, qdrant_store
from app.ai.retrieval.base import Document, ScoredDocument
from app.ai.retrieval.qdrant_store import QdrantVectorStore
from app.ai.retrieval.retriever import VectorRetriever
from app.ai.tasks import ask_comments as task
from app.api.routes import rag as rag_route
from app.core.errors import UpstreamError, register_exception_handlers
from app.services.rag import ask_comments as service
from scripts.jobs import load_qdrant

_LOOP = asyncio.new_event_loop()


def run(coro):
    return _LOOP.run_until_complete(coro)


class FakeEmbedder:
    dim = DIM

    def embed(self, texts):
        out = []
        for text in texts:
            vec = np.zeros(DIM, dtype=np.float32)
            for word in text.lower().split():
                vec[int(hashlib.md5(word.encode()).hexdigest(), 16) % DIM] += 1.0
            out.append((vec / (np.linalg.norm(vec) or 1.0)).tolist())
        return out


def scored(*texts):
    return [ScoredDocument(Document(f"c{i}", t), 0.9 - i / 100) for i, t in enumerate(texts)]


# --- hàm thuần của task -------------------------------------------------------------------------------------------


def test_fit_sources_matches_build_context_budget():
    results = scored("a" * 50, "b" * 50, "c" * 50)
    kept = task.fit_sources(results, max_chars=120)
    assert len(kept) == 2  # "[1] " + 50 = 54; thêm "[2] " + 50 = 108 <= 120; khối 3 vượt
    assert task.fit_sources([], 100) == []


def test_citations_are_validated():
    answer = "Nhiều người khen nhạc [1][3], ý khác [7] và lặp lại [1]."
    assert task.cited_sources(answer, 3) == [1, 3]
    assert task.strip_invalid_citations(answer, 3) == "Nhiều người khen nhạc [1][3], ý khác  và lặp lại [1]."


def test_user_prompt_contains_question_and_context():
    prompt = task.build_user_prompt("  Khán giả nghĩ gì?  ", "[1] hay quá")
    assert "CÂU HỎI: Khán giả nghĩ gì?" in prompt and "[1] hay quá" in prompt


def test_prompt_is_registered_as_default():
    from app.ai.prompts.defaults import default_system_prompts

    assert "chỉ dùng thông tin" in default_system_prompts()["ask_comments"].lower()


# --- job nạp ------------------------------------------------------------------------------------------------------


@pytest.fixture
def embeddings(tmp_path):
    conn = sqlite3.connect(tmp_path / "emb.sqlite")
    conn.execute(
        "CREATE TABLE comment_embeddings (comment_id TEXT PRIMARY KEY, movie_id TEXT, embedding BLOB NOT NULL)"
    )
    embedder = FakeEmbedder()
    texts = {f"cm{i:03d}": f"bình luận số {i} rất dài để qua bộ lọc độ dài tối thiểu" for i in range(7)}
    for cid, text in texts.items():
        conn.execute(
            "INSERT INTO comment_embeddings VALUES (?, 'm1', ?)",
            (cid, np.array(embedder.embed([text])[0], np.float16).tobytes()),
        )
    conn.execute("INSERT INTO comment_embeddings VALUES ('other', 'm2', ?)", (np.zeros(DIM, np.float16).tobytes(),))
    conn.execute("INSERT INTO comment_embeddings VALUES ('bad-dim', 'm1', ?)", (np.zeros(10, np.float16).tobytes(),))
    conn.commit()
    return conn, texts


def d1_rows(texts):
    rows = {
        cid: {
            "id": cid,
            "message": text,
            "platform": "tiktok",
            "sentiment": "positive",
            "reactions_count": 3,
            "posted_at": "2026-11-18T08:44:58+00:00",
        }
        for cid, text in texts.items()
    }
    rows["cm001"]["message"] = "ngắn"  # dưới min_chars -> bị bỏ
    rows["cm002"]["posted_at"] = "không phải ngày"  # hỏng -> bỏ khỏi payload nhưng vẫn nạp
    del rows["cm003"]  # không còn trong D1 -> bỏ
    return rows


@pytest.fixture
def qstore():
    client = AsyncQdrantClient(":memory:")
    run(qdrant_store.ensure_collection(client))
    yield QdrantVectorStore(client), client
    run(client.close())


def test_iso_utc_normalises_or_drops():
    assert load_qdrant.iso_utc("2026-11-18T08:44:58+00:00") == "2026-11-18T08:44:58+00:00"
    assert load_qdrant.iso_utc("2026-11-18T15:44:58+07:00") == "2026-11-18T08:44:58+00:00"
    assert load_qdrant.iso_utc("2026-11-18T08:44:58") == "2026-11-18T08:44:58+00:00"
    assert load_qdrant.iso_utc(None) is None and load_qdrant.iso_utc("hỏng") is None


def test_load_movie_filters_pages_and_is_idempotent(embeddings, qstore):
    conn, texts = embeddings
    store, client = qstore
    rows = d1_rows(texts)
    fetch = AsyncMock(side_effect=lambda ids: {i: rows[i] for i in ids if i in rows})
    cursor: list[str] = []

    stats = run(
        load_qdrant.load_movie(conn, store, "m1", min_chars=20, page_size=3, fetch=fetch, on_page=cursor.append)
    )
    # m1 có 7 + 1 dòng sai số chiều = 8 vector; trừ ngắn (cm001), vắng D1 (cm003), sai chiều (bad-dim) -> 5 điểm
    assert stats == {"seen": 8, "loaded": 5} and len(cursor) == 3 and cursor[-1] == "cm006"
    assert run(client.count(qdrant_store.COLLECTION)).count == 5

    run(load_qdrant.load_movie(conn, store, "m1", min_chars=20, page_size=3, fetch=fetch))  # chạy lại: không nhân đôi
    assert run(client.count(qdrant_store.COLLECTION)).count == 5

    hit = run(store.search(FakeEmbedder().embed([texts["cm002"]])[0], limit=1, filters={"movie_id": "m1"}))[0]
    assert hit.document.id == "cm002" and "posted_at" not in hit.document.metadata
    assert hit.document.metadata["platform"] == "tiktok" and hit.document.metadata["reactions"] == 3


def test_load_movie_resumes_limits_and_dry_run(embeddings, qstore):
    conn, texts = embeddings
    store, client = qstore
    rows = d1_rows(texts)
    fetch = AsyncMock(side_effect=lambda ids: {i: rows[i] for i in ids if i in rows})

    dry = run(load_qdrant.load_movie(conn, store, "m1", dry_run=True, fetch=fetch))
    assert dry["loaded"] == 5 and run(client.count(qdrant_store.COLLECTION)).count == 0

    # thứ tự theo comment_id: bad-dim, cm000, cm001, cm002, cm003, ...
    first = run(load_qdrant.load_movie(conn, store, "m1", limit=4, page_size=3, fetch=fetch))
    assert first["seen"] == 4  # limit chặn cả trang cuối (3 + 1)
    resumed = run(load_qdrant.load_movie(conn, store, "m1", after="cm002", fetch=fetch))  # tiếp từ con trỏ
    assert resumed["seen"] == 4  # cm003..cm006
    assert run(client.count(qdrant_store.COLLECTION)).count == 5


def test_cursor_roundtrip_and_corrupt_file(tmp_path):
    path = tmp_path / "cursor.json"
    assert load_qdrant.load_cursor(path) == {}
    load_qdrant.save_cursor({"m1": "cm005"}, path)
    assert load_qdrant.load_cursor(path) == {"m1": "cm005"}
    path.write_text("{hỏng")
    assert load_qdrant.load_cursor(path) == {}


# --- service + route + factory ---------------------------------------------------------------------------------------


@pytest.fixture
def filled(qstore, monkeypatch):
    store, _ = qstore
    embedder = FakeEmbedder()
    items = [
        Document("c1", "nhạc phim rất cuốn", {"movie_id": "m1", "platform": "facebook", "sentiment": "positive"}),
        Document("c2", "kịch bản dở quá", {"movie_id": "m1", "platform": "tiktok", "sentiment": "negative"}),
        Document("c3", "nhạc phim bình thường", {"movie_id": "m2", "platform": "facebook", "sentiment": "neutral"}),
    ]
    run(store.upsert(items, embedder.embed([d.text for d in items])))
    retriever = VectorRetriever(embedder, store)
    monkeypatch.setattr(service, "get_retriever", AsyncMock(return_value=retriever))
    return retriever


def test_service_answers_with_valid_citations_and_filters(filled, monkeypatch):
    llm = AsyncMock(return_value="Khen nhạc phim [1], và một ý bịa [9].")
    monkeypatch.setattr(service, "call_llm", llm)
    result = run(service.ask_comments("nhạc phim thế nào", movie_id="m1", limit=5))
    assert result["answer"] == "Khen nhạc phim [1], và một ý bịa ."
    assert {s["id"] for s in result["sources"]} == {"c1", "c2"}  # chỉ phim m1, không lẫn c3 của m2
    assert [s["index"] for s in result["sources"]] == [1, 2]
    kwargs = llm.call_args.kwargs
    assert kwargs["task"] == "ask_comments" and kwargs["force"] is True
    assert "[1] nhạc phim rất cuốn" in kwargs["user_prompt"]

    only_neg = run(service.ask_comments("phim", movie_id="m1", sentiment="negative"))
    assert [s["id"] for s in only_neg["sources"]] == ["c2"]


def test_service_without_results_skips_the_llm(filled, monkeypatch):
    llm = AsyncMock()
    monkeypatch.setattr(service, "call_llm", llm)
    result = run(service.ask_comments("nhạc phim", movie_id="khong-co"))
    assert result == {"answer": task.NO_DATA, "sources": []}
    llm.assert_not_called()


def _client(monkeypatch, ask):
    monkeypatch.setattr(rag_route, "ask_comments", ask)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(rag_route.router)
    return TestClient(app)


def test_route_validates_and_maps_errors(monkeypatch):
    ok = AsyncMock(return_value={"answer": "ok", "sources": []})
    client = _client(monkeypatch, ok)
    body = {"movie_id": "m1", "question": "khán giả nghĩ gì về nhạc phim"}
    assert client.post("/rag/ask-comments", json=body).json() == {"answer": "ok", "sources": []}
    assert ok.call_args.kwargs == {"movie_id": "m1", "platform": None, "sentiment": None, "limit": 8}

    for bad in (
        {"movie_id": "m1", "question": "ab"},
        {**body, "sentiment": "vui"},
        {**body, "limit": 99},
        {"question": "câu hỏi hợp lệ"},
    ):
        assert client.post("/rag/ask-comments", json=bad).status_code == 422

    assert (
        _client(monkeypatch, AsyncMock(side_effect=RuntimeError("ai_temporarily_disabled")))
        .post("/rag/ask-comments", json=body)
        .status_code
        == 502
    )
    assert (
        _client(monkeypatch, AsyncMock(side_effect=ConnectionError("qdrant down")))
        .post("/rag/ask-comments", json=body)
        .status_code
        == 502
    )


def test_factory_builds_once_and_closes(monkeypatch):
    built = []

    def fake_client():
        built.append(AsyncQdrantClient(":memory:"))
        return built[-1]

    monkeypatch.setattr(factory, "new_client", fake_client)
    monkeypatch.setattr(factory, "E5Embedder", FakeEmbedder)
    monkeypatch.setattr(factory, "_lock", asyncio.Lock())

    async def scenario():
        first, second = await asyncio.gather(factory.get_retriever(), factory.get_retriever())
        assert first is second and len(built) == 1
        await factory.close_retriever()
        assert factory._retriever is None

    run(scenario())


def test_upstream_error_type_is_used_by_route():
    assert issubclass(UpstreamError, Exception)
