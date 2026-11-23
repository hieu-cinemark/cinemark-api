# Hướng dẫn thêm RAG và LangGraph vào cinemark-api

Tài liệu này chỉ chỗ đặt code, khung mẫu và các bẫy cần tránh. Mọi thứ nằm trong `app/ai/` (logic AI) và
`app/services/` (đọc/ghi DB, job nền). Đọc nhanh `app/ai/README.md` trước.

## 0. Cái gì đã có sẵn

| Thành phần | File | Ghi chú |
|---|---|---|
| Gọi LLM có cấu hình + retry | `app/ai/runtime.py` | `call_llm` (raise khi lỗi), `call_llm_json` (không raise, trả `(data|None, raw)`) |
| Provider (Bee, tương thích OpenAI) | `app/ai/providers/client.py` | Đã tính chi phí vào ví Bee, trang "Ngân sách AI" đọc từ đây |
| Cấu hình bật/tắt + prompt ghi đè | bảng `ai_settings` | `runtime.load_ai_runtime()`, cache 60 giây |
| System prompt mặc định theo task | `app/ai/prompts/defaults.py` | Task không có prompt ghi đè thì rơi về đây |
| Embedding | `app/ai/local/embedding.py` | `embed_texts(list[str]) -> np.ndarray (n, 384)`, đã chuẩn hoá, model `multilingual-e5-small` |
| Interface RAG | `app/ai/retrieval/base.py` | `Document`, `ScoredDocument`, `Embedder`, `VectorStore`, `Retriever`, `build_context` |
| Job nền | `app/services/ai_jobs/` | Ví dụ nhiều bước: `review_angles.py` |

Dữ liệu vector hiện có: file SQLite `data/comment_embeddings.sqlite` trên máy chạy job (~1,8 triệu bình luận, float16). Kho vector đã chọn: **Qdrant** (xem 1.2).

Chưa có: bản cài đặt `VectorStore`/`Retriever`, thư mục `graphs/`, thư viện `langgraph`, thư viện `qdrant-client`.

## 1. RAG

### 1.1 Các bước

1. **Chọn kho vector** (mục 1.2).
2. **Embedder**: bọc `embed_texts`. Interface `Embedder.embed` là hàm đồng bộ và trả `list[list[float]]`.
3. **VectorStore**: cài đặt `upsert` và `search` (cả hai `async`, xem chữ ký trong `base.py`).
4. **Retriever**: nhận câu hỏi → embed → `search` → trả `list[ScoredDocument]`.
5. **Job nạp dữ liệu**: đọc bài/bình luận từ D1, embed, `upsert`. Đặt ở `app/services/ai_jobs/`.
6. **Dùng trong task**: gọi retriever, ghép bằng `build_context`, chèn vào user prompt, gọi `runtime.call_llm`.

### 1.2 Kho vector: Qdrant

Đã chọn Qdrant. Lý do: lọc theo thuộc tính ngay trong truy vấn (`movie_id`, `platform`, ngày, cảm xúc), chạy một container Docker,
hỗ trợ vector `float16` và nén `int8`, và không dùng Supabase hay D1 (đang vượt egress / hay quá tải).

- **Hiện trạng:** embedding bình luận nằm ở `data/comment_embeddings.sqlite` (xem `app/ai/retrieval/README.md`). Dùng lại, không tính lại.
- **Chạy:** `docker run -p 6333:6333 -v $(pwd)/data/qdrant:/qdrant/storage qdrant/qdrant` (Mac dev trước; lên VPS cần biết RAM
  trống, nên có từ 2 GB cho dữ liệu cỡ này). Qdrant Cloud gói miễn phí (~1 GB) chỉ đủ khi chọn lọc bớt bình luận.
- **Quy mô:** 1,8 triệu × 384 × 2 byte ≈ 1,4 GB ở float16; nén int8 còn ~0,7 GB RAM, bản gốc để trên đĩa. Không nên nạp hết: lọc
  bình luận đủ dài, thuộc bài liên quan phim, ưu tiên phim đang chiếu.
- **Điểm và payload:** id là UUID `uuid5` từ `comment_id`; payload gồm `comment_id`, `movie_id`, `platform`, `posted_at`,
  `sentiment`, `reactions` và nội dung bình luận (truy hồi không phải đọc D1). Chỉ mục payload cho `movie_id` (tenant), `platform`,
  `sentiment`, `posted_at`.
- **Tiền tố e5:** vector hiện có được embed với `"query: "`. Giữ đúng tiền tố đó khi truy vấn để cùng một không gian.
- **Môi trường test:** `QdrantClient(":memory:")` không cần server; chế độ lưu cục bộ theo đường dẫn chỉ hợp dữ liệu nhỏ, đừng dùng cho dữ liệu thật.

### 1.3 Khung mẫu

`app/ai/retrieval/e5_embedder.py`

```python
from __future__ import annotations

from collections.abc import Sequence

from app.ai.local.embedding import DIM, embed_texts


class E5Embedder:
    dim = DIM

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return embed_texts(list(texts)).tolist()
```

`app/ai/retrieval/qdrant_store.py` (khớp `VectorStore` trong `base.py`; tên API `qdrant-client` viết theo trí nhớ, đối chiếu tài liệu theo phiên bản cài)

```python
from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

from qdrant_client import AsyncQdrantClient, models

from app.ai.local.embedding import DIM
from app.ai.retrieval.base import Document, ScoredDocument

COLLECTION = "comments"
_NAMESPACE = uuid.UUID("6f1c2a52-0000-4000-8000-000000000001")  # cố định để upsert lại cùng một điểm


def point_id(comment_id: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, comment_id))


async def ensure_collection(client: AsyncQdrantClient) -> None:
    if await client.collection_exists(COLLECTION):
        return
    await client.create_collection(
        COLLECTION,
        vectors_config=models.VectorParams(
            size=DIM, distance=models.Distance.COSINE, datatype=models.Datatype.FLOAT16, on_disk=True
        ),
        quantization_config=models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(type=models.ScalarType.INT8, always_ram=True)
        ),
    )
    for field, schema in (
        ("movie_id", models.PayloadSchemaType.KEYWORD),
        ("platform", models.PayloadSchemaType.KEYWORD),
        ("sentiment", models.PayloadSchemaType.KEYWORD),
        ("posted_at", models.PayloadSchemaType.DATETIME),
    ):
        await client.create_payload_index(COLLECTION, field_name=field, field_schema=schema)


class QdrantVectorStore:
    def __init__(self, client: AsyncQdrantClient) -> None:
        self._client = client

    async def upsert(self, documents: Sequence[Document], vectors: Sequence[Sequence[float]]) -> None:
        points = [
            models.PointStruct(
                id=point_id(doc.id), vector=list(vec), payload={**doc.metadata, "comment_id": doc.id, "text": doc.text}
            )
            for doc, vec in zip(documents, vectors, strict=True)
        ]
        await self._client.upsert(COLLECTION, points=points)

    async def search(
        self, vector: Sequence[float], *, limit: int, filters: dict[str, Any] | None = None
    ) -> list[ScoredDocument]:
        query_filter = (
            models.Filter(
                must=[models.FieldCondition(key=k, match=models.MatchValue(value=v)) for k, v in filters.items()]
            )
            if filters
            else None
        )
        result = await self._client.query_points(
            COLLECTION, query=list(vector), limit=limit, query_filter=query_filter, with_payload=True
        )
        out: list[ScoredDocument] = []
        for hit in result.points:
            payload = dict(hit.payload or {})
            text = str(payload.pop("text", ""))
            out.append(
                ScoredDocument(
                    Document(id=str(payload.get("comment_id", hit.id)), text=text, metadata=payload), float(hit.score)
                )
            )
        return out
```

`app/ai/retrieval/retriever.py`

```python
import asyncio

from app.ai.retrieval.base import Embedder, ScoredDocument, VectorStore


class VectorRetriever:
    def __init__(self, embedder: Embedder, store: VectorStore) -> None:
        self._embedder, self._store = embedder, store

    async def retrieve(self, query: str, *, limit: int = 8, filters: dict | None = None) -> list[ScoredDocument]:
        # embed_texts chạy torch đồng bộ: đẩy ra luồng riêng để không chặn event loop
        vector = (await asyncio.to_thread(self._embedder.embed, [query]))[0]
        return await self._store.search(vector, limit=limit, filters=filters)
```

Dùng trong một task (`app/ai/tasks/<task>.py` + prompt trong `app/ai/prompts/<task>.py`):

```python
from app.ai.retrieval.base import build_context
from app.ai.runtime import call_llm

results = await retriever.retrieve(question, limit=8, filters={"movie_id": movie_id})
context = build_context(results, max_chars=6000)
answer = await call_llm(
    system_prompt=MY_TASK_SYSTEM_PROMPT,
    user_prompt=f"Ngữ cảnh:\n{context}\n\nCâu hỏi: {question}",
    max_tokens=800,
    task="my_task",
)
```

### 1.4 Bẫy cần nhớ

- **Torch chỉ chắc chắn có trên Mac dev** (embedding job đang chạy ở đó). Kiểm tra VPS có cài `torch`/`transformers` chưa trước khi
  đặt job nạp dữ liệu hoặc retriever ở scheduler.
- **Dữ liệu thật nằm ở D1**, không phải Supabase. Đọc qua `app/repositories/d1/` và `d1_exec` như các job khác.
- **Đừng đưa thông tin riêng tư** (email, cookie, token tài khoản) vào tài liệu nạp vào RAG.
- Đo chất lượng truy hồi bằng một bộ câu hỏi mẫu trước khi nối vào giao diện. Tương tự cách đo luật relevance bằng bộ bài rà soát.

## 2. LangGraph

### 2.1 Nguyên tắc

- Đặt mỗi đồ thị ở `app/ai/graphs/<tên>.py` (thư mục mới, thêm `__init__.py`). Thêm `langgraph` vào `pyproject.toml`.
- **Mọi node gọi LLM phải đi qua `runtime.call_llm` / `call_llm_json` với `task="..."`.** Không để LangGraph/LangChain tự gọi
  provider: làm vậy sẽ bỏ qua bật/tắt AI, prompt ghi đè trong `ai_settings`, retry, và số liệu chi phí ở trang Ngân sách AI.
- Giữ node là hàm thuần nhận/trả state; phần đọc/ghi DB bọc ngoài đồ thị, ở `app/services/ai_jobs/`.
- Mỗi task mới cần: prompt trong `app/ai/prompts/`, và (nếu muốn người vận hành sửa được) khai báo trong
  `app/ai/prompts/defaults.py`.

### 2.2 Khung mẫu

`app/ai/graphs/answer_with_rag.py`

```python
from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from app.ai.retrieval.base import ScoredDocument, build_context
from app.ai.runtime import call_llm, call_llm_json


class State(TypedDict, total=False):
    question: str
    movie_id: str
    docs: list[ScoredDocument]
    enough: bool
    answer: str


def build_graph(retriever):
    async def retrieve(state: State) -> State:
        docs = await retriever.retrieve(state["question"], limit=8, filters={"movie_id": state["movie_id"]})
        return {"docs": docs}

    async def grade(state: State) -> State:
        data, _ = await call_llm_json(
            system_prompt='Trả JSON {"enough": true|false}: ngữ cảnh có đủ để trả lời câu hỏi không.',
            user_prompt=f"Câu hỏi: {state['question']}\n\n{build_context(state['docs'], max_chars=3000)}",
            max_tokens=50,
            task="rag_grade",
        )
        return {"enough": bool(data and data.get("enough"))}

    async def answer(state: State) -> State:
        text = await call_llm(
            system_prompt="Trả lời chỉ dựa trên ngữ cảnh, trích số [n].",
            user_prompt=f"{build_context(state['docs'])}\n\nCâu hỏi: {state['question']}",
            max_tokens=800,
            force=True,
            task="rag_answer",
        )
        return {"answer": text}

    graph = StateGraph(State)
    graph.add_node("retrieve", retrieve)
    graph.add_node("grade", grade)
    graph.add_node("answer", answer)
    graph.add_edge(START, "retrieve")
    graph.add_edge("retrieve", "grade")
    graph.add_conditional_edges("grade", lambda s: "answer" if s["enough"] else END)
    graph.add_edge("answer", END)
    return graph.compile()
```

Chạy: `result = await build_graph(retriever).ainvoke({"question": q, "movie_id": mid})`.

Cú pháp `StateGraph`, `add_conditional_edges`, `compile`, `ainvoke` có thể đổi giữa các phiên bản `langgraph`. Ghim phiên bản
trong `pyproject.toml` và đối chiếu tài liệu chính thức khi cài.

### 2.3 Chỗ nên thử đầu tiên

`app/services/ai_jobs/review_angles.py` (hàm `analyze_next_batch`, khoảng 156 dòng, nhiều nhánh) là luồng nhiều bước nhất đang
có. Tách thành các node (chọn lô → gọi AI → kiểm tra → ghi) sẽ dễ thấy lợi ích. Giữ nguyên test cũ để so sánh kết quả.

## 3. Nối vào hệ thống

1. **Job nền**: thêm hàm trong `app/services/ai_jobs/` gọi đồ thị, đăng ký vào scheduler như các job cùng thư mục.
2. **API**: thêm route ở `app/api/routes/` nếu cần gọi từ Worker/giao diện. Worker gọi cinemark-api qua `API_AUTH_KEY`
   (`X-API-Key`).
3. **Giao diện**: nếu hiển thị kết quả, đặt hook và component ở `cinemark/apps/web/src/features/...`; Worker
   (`cinemark/apps/api/src/integrations/ai/`) chỉ cần thêm nếu muốn gọi AI trực tiếp, không qua cinemark-api.

## 4. Kiểm thử

- Test node và retriever bằng hàm thuần, không gọi mạng: `monkeypatch` `app.ai.runtime.call_llm` / `call_llm_json`.
- Test kho vector với `AsyncQdrantClient(":memory:")` và vài vector tự dựng (ba vector gần nhau, một vector xa): kiểm tra thứ tự điểm và lọc `filters`.
- Chạy: `.venv/bin/python -m pytest -q`, `.venv/bin/ruff check app tests`, `.venv/bin/ruff format app tests`.
- Ghi tên log mới vào `app/core/log_messages.py` nếu dùng `logger.info("<khoá>")`; repo có kiểm tra khoá log.

## 5. Danh sách việc

- [x] Chọn kho vector: Qdrant.
- [ ] Thêm `qdrant-client`, cài `QdrantVectorStore` + tạo collection/chỉ mục, viết test lọc `filters`.
- [ ] Chạy Qdrant (Docker) và quyết định nơi đặt (Mac / VPS / Cloud), phạm vi bình luận nạp.
- [ ] `scripts/jobs/load_qdrant.py`: nạp từ `comment_embeddings.sqlite` + thuộc tính/nội dung từ D1.
- [ ] `E5Embedder` + `VectorRetriever`.
- [ ] Bộ câu hỏi mẫu để đo chất lượng truy hồi.
- [ ] Thêm `langgraph` (ghim phiên bản), thư mục `app/ai/graphs/`, đồ thị đầu tiên.
- [ ] Prompt mới vào `app/ai/prompts/` + `defaults.py`.
- [ ] Đăng ký job / route, cập nhật `app/ai/README.md` và `app/ai/retrieval/README.md`.
