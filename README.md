# cinemark-api

Service Python (FastAPI) chạy trên VPS, nối spider-hub (cào dữ liệu) với cinemark (dashboard trên Cloudflare Worker):

- **Kích hoạt cào:** đọc phim / từ khoá đang bật từ D1 `cinemark-scraper`, đẩy yêu cầu cào lên Kafka `crawl_requests`.
- **Nhận kết quả:** đọc bài và bình luận spider-hub đẩy lên Kafka, phân loại (liên quan, cảm xúc, khía cạnh) rồi ghi D1.
- **Chạy nền:** lịch cào, tổng hợp số liệu, job AI (nhãn tác giả, niche kênh, Review Angles), báo cáo, cảnh báo.
- **API cho dashboard spider-hub:** tài khoản, proxy, từ khoá, phim, thống kê, log, hàng đợi.

Service không có database riêng: dữ liệu nằm ở Cloudflare D1 (gọi qua HTTP API), cấu hình tài khoản/AI ở Postgres
(Supabase), trạng thái ngắn hạn ở Redis, dữ liệu thô ở R2.

## Luồng chính

```
dashboard / scheduler --POST /<nền tảng>/run--> cinemark-api --Kafka crawl_requests--> spider-hub (cào)
                                                                                            |
D1 cinemark-scraper <--ghi bài, bình luận-- ingest_consumer <--Kafka raw_posts / raw_comments-+
        |
        +--> Worker cinemark đọc trực tiếp để hiển thị
```

## Cài đặt và chạy

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # thêm requirements-ml.txt nếu chạy model tại chỗ
cp .env.example .env                       # điền Kafka, CLOUDFLARE_*, Redis, Supabase...

uvicorn app.main:app --reload              # API
python -m app.workers.ingest_consumer.main # nhận bài/bình luận từ Kafka
python -m app.workers.scheduler.main       # lịch cào và job nền
pytest                                      # test
```

Biến môi trường đầy đủ ở `.env.example` và `app/core/config.py`.

## Triển khai

Push lên `main` thì GitHub Actions deploy lên VPS (`deploy/vps/deploy-repo.sh`). Các service systemd ở `deploy/systemd/`.

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `app/api/` | Route HTTP |
| `app/core/` | Cấu hình, xác thực, log, middleware |
| `app/clients/` | Client mỏng: D1, Kafka, Redis, R2, Telegram |
| `app/repositories/` | Truy vấn SQL theo bảng (D1) và SQLite tại máy |
| `app/services/` | Nghiệp vụ: cào, job AI, báo cáo, thống kê, vận hành |
| `app/ai/` | Mọi thứ về LLM/model: provider, prompt, task, model tại chỗ, RAG |
| `app/workers/` | Tiến trình chạy lâu: ingest, scheduler, lake writer |
| `app/vision/` | Model đọc ảnh tại chỗ |
| `scripts/` | Job chạy định kỳ, script train model, công cụ chạy tay |
| `deploy/` | systemd và script deploy |
| `tests/` | Test (pytest) |

Mỗi thư mục có README riêng.
