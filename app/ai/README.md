# app/ai

Mọi thứ liên quan tới LLM và model AI.

| Thư mục / file | Vai trò |
|---|---|
| `runtime.py` | Cấu hình theo task (bật/tắt, prompt ghi đè trong `ai_settings`), `call_llm`, `call_llm_json` |
| `providers/` | Kết nối nhà cung cấp: client HTTP tương thích OpenAI, Bee, model quyết định, Jev |
| `prompts/` | System prompt và khung user prompt của từng task |
| `tasks/` | Logic từng tác vụ: phân loại, sinh report, kiểm tra câu trả lời AI |
| `domain/` | Tri thức nghiệp vụ dùng trong prompt và hậu xử lý |
| `local/` | Model chạy tại chỗ: ViSoBERT, embedding e5 |
| `retrieval/` | Interface RAG (chưa có cài đặt) |

## Thêm một task AI mới

1. Viết prompt trong `prompts/<task>.py`.
2. Viết logic (dựng user prompt, kiểm tra câu trả lời) trong `tasks/<task>.py`, giữ hàm thuần để test được.
3. Gọi LLM qua `runtime.call_llm` hoặc `runtime.call_llm_json` với `task="<task>"` để dùng chung cấu hình và retry.
4. Phần đọc/ghi DB đặt ở `app/services/`, không đặt trong `app/ai/`.

## Thêm RAG

Cài đặt các interface trong `retrieval/base.py` (`Embedder`, `VectorStore`, `Retriever`), rồi chèn kết quả vào user
prompt bằng `build_context`. Xem `retrieval/README.md`.
