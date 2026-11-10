# app/ai/tasks

Logic từng tác vụ AI: dựng đầu vào, gọi LLM / model, kiểm tra và chuẩn hoá câu trả lời.

| File | Tác vụ |
|---|---|
| `post_relevance.py` | Độ liên quan của bài với phim (lúc ingest) |
| `sentiment.py`, `sentiment_rules.py` | Cảm xúc, khía cạnh, giai đoạn khán giả của bình luận |
| `report.py` | Sinh report Social Topic |
| `import_parser.py` | Đọc dữ liệu tài khoản / proxy dán thô |
| `review_angles.py` | Kiểm tra câu trả lời gom góc Review Angles |

Mọi tác vụ fail open: lỗi AI thì trả None hoặc giữ nhãn theo luật, không làm hỏng luồng chính.
