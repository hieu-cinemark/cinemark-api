# app/clients

Client mỏng tới hệ thống bên ngoài, không chứa nghiệp vụ.

| File | Kết nối |
|---|---|
| `d1.py` | Cloudflare D1 qua HTTP API (`database="scraper" / "app" / "config"`), retry khi D1 quá tải |
| `kafka.py` | Đẩy yêu cầu cào lên Kafka |
| `redis.py` | Redis dùng chung (tiền tố `social_crawler:`) |
| `lake.py` | R2 qua S3 API |
| `telegram.py` | Gửi cảnh báo Telegram |
