"""Mọi thứ liên quan tới LLM và model AI.

- `runtime.py`: cấu hình theo task (bật/tắt, prompt ghi đè) và `call_llm` / `call_llm_json`.
- `providers/`: kết nối nhà cung cấp (client HTTP tương thích OpenAI, Bee, model quyết định, Jev).
- `prompts/`: system prompt và khung user prompt của từng task.
- `tasks/`: logic từng tác vụ AI (phân loại, sinh report, kiểm tra câu trả lời).
- `domain/`: tri thức nghiệp vụ dùng trong prompt và hậu xử lý (khía cạnh, thông tin phim, đi rạp, độ chính xác).
- `local/`: model chạy tại chỗ (ViSoBERT, embedding).
- `retrieval/`: interface RAG.
"""
