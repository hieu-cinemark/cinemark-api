# app/ai/providers

Kết nối tới nhà cung cấp AI.

| File | Vai trò |
|---|---|
| `client.py` | Client tương thích OpenAI dùng chung: đọc provider từ bảng `ai_providers`, retry khi 429, giới hạn đồng thời, stream, log token, parse JSON |
| `bee.py` | `call_bee`: gọi Bee với model chọn riêng (report) |
| `decisions.py` | Model quyết định của Bee (`/decisions`): trả lời câu hỏi lựa chọn kèm xác suất |
| `jev.py` | Bộ câu hỏi chấm nhãn bình luận / bài viết trên model quyết định |

Khoá API và model nằm trong DB, không ghi trong code. Thêm provider mới: thêm dòng `ai_providers`, không cần sửa code
nếu provider tương thích OpenAI.
