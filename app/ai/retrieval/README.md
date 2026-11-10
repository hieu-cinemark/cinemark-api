# app/ai/retrieval

Khung cho RAG (truy hồi ngữ cảnh rồi đưa vào prompt). Hiện chỉ có interface trong `base.py`:

- `Document`, `ScoredDocument`: đoạn văn bản và điểm tương đồng.
- `Embedder`: biến văn bản thành vector đã chuẩn hoá (có thể bọc `app.ai.local.embedding`).
- `VectorStore`: lưu và tìm vector, lọc theo metadata (`movie_id`, `platform`...).
- `Retriever`: trả tài liệu liên quan nhất cho một câu hỏi.
- `build_context`: ghép tài liệu thành khối `[1] ... [2] ...` để chèn vào user prompt.

Gợi ý cài đặt: kho vector bằng pgvector (Supabase) hoặc Cloudflare Vectorize; nguồn tài liệu là bài viết, bình luận
và báo đã có trong D1. Khi thêm, đặt phần cài đặt cụ thể trong thư mục này và giữ prompt ở `app/ai/prompts/`.
