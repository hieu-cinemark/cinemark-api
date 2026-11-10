# app/workers/ingest_consumer

Đọc `raw_posts` / `raw_comments` từ Kafka, áp luật và model độ liên quan, ghi D1. Offset chỉ commit khi message đã xử lý
xong hoặc đã vào dead letter. `sentiment_sweep.py` chạy nền: gán cảm xúc cho bình luận (mỗi phút) và bài (mỗi 10 phút).
