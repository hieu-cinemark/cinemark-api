# app/ai/retrieval

Khung cho RAG (truy hồi ngữ cảnh rồi đưa vào prompt). Hiện chỉ có interface trong `base.py`, **chưa có bản cài đặt**:

- `Document`, `ScoredDocument`: đoạn văn bản và điểm tương đồng.
- `Embedder`: biến văn bản thành vector đã chuẩn hoá (bọc `app.ai.local.embedding`).
- `VectorStore`: lưu và tìm vector, lọc theo metadata (`movie_id`, `platform`...).
- `Retriever`: trả tài liệu liên quan nhất cho một câu hỏi.
- `build_context`: ghép tài liệu thành khối `[1] ... [2] ...` để chèn vào user prompt.

## Hiện trạng vector

Chưa có kho vector chuyên dụng. Embedding bình luận (`intfloat/multilingual-e5-small`, 384 chiều, đã chuẩn hoá) nằm trong file
SQLite trên máy chạy job: `data/comment_embeddings.sqlite` (`comment_id`, `movie_id`, vector float16, cờ `with_context`; khoảng
1,8 triệu dòng, ~2,1 GB). Job nạp: `scripts/jobs/embed_comments.py`. Đọc theo `comment_id` ở `app/services/attack/data.py`
(`load_vectors`) rồi tính cosine bằng numpy. File này không có trên VPS.

## Kho vector đã chọn: Qdrant

Chưa cài đặt - đây là thiết kế dự kiến. Tên API `qdrant-client` dưới đây cần đối chiếu tài liệu theo phiên bản cài.

- **Chạy:** container `qdrant/qdrant` (cổng 6333, mount thư mục lưu trữ), ví dụ
  `docker run -p 6333:6333 -v $(pwd)/data/qdrant:/qdrant/storage qdrant/qdrant`. Không dùng Supabase hay D1 (Supabase đang vượt
  hạn mức egress, D1 hay quá tải).
- **Collection** (vd. `comments`): cosine, 384 chiều, `datatype=float16`, nén scalar `int8`, vector và payload để `on_disk`.
- **Điểm:** id là UUID `uuid5` sinh từ `comment_id` (Qdrant chỉ nhận số nguyên hoặc UUID); giữ `comment_id` gốc trong payload.
- **Payload:** `comment_id`, `movie_id`, `platform`, `posted_at`, `sentiment`, `reactions` và nội dung bình luận (để truy hồi
  không phải đọc D1). Không đưa thông tin riêng tư (email, cookie, token).
- **Chỉ mục payload:** `movie_id` (keyword, đánh dấu tenant), `platform`, `sentiment` (keyword), `posted_at` (datetime).
- **Embedding:** dùng lại vector e5-small đã có, không tính lại. Truy vấn dùng đúng tiền tố `"query: "` như lúc embed để cùng
  một không gian.
- **Phạm vi nạp:** không nạp hết 1,8 triệu. Lọc bình luận đủ dài, thuộc bài liên quan phim, ưu tiên phim đang chiếu hoặc nhiều
  lượt thích.

### Việc cần làm

1. Thêm `qdrant-client` vào `pyproject.toml`.
2. `qdrant_store.py`: cài `VectorStore` (`filters` -> `Filter(must=[FieldCondition(...)])`), hàm tạo collection và chỉ mục.
3. `scripts/jobs/load_qdrant.py`: đọc SQLite theo lô, ghép nội dung và thuộc tính từ D1 (đọc theo lô, nghỉ khi D1 quá tải như
   `embed_comments.py`), `upsert`; có con trỏ để dừng rồi chạy tiếp.
4. `Embedder` bọc `embed_texts`, `Retriever` ghép embedder + store.
5. Test bằng `QdrantClient(":memory:")`: lọc theo `movie_id`, thứ tự điểm.
6. Nối vào một task AI qua `build_context` (prompt đặt ở `app/ai/prompts/`).

Đặt phần cài đặt cụ thể trong thư mục này. Hướng dẫn chi tiết và khung code mẫu: `docs/ai-rag-langgraph.md`.
