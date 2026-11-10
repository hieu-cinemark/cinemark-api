# app/ai/local

Model chạy tại chỗ, không tốn lời gọi API. Cần `requirements-ml.txt` (torch, transformers) và thư mục model.

- `visobert.py`: cảm xúc bình luận (khen / chê / trung lập).
- `visobert_relevance.py`: độ liên quan của bài (related / not_related).
- `embedding.py`: embedding câu e5-small 384 chiều.

Thiếu thư viện hoặc model thì các hàm báo không khả dụng và bên gọi chuyển sang AI.
