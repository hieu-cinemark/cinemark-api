# app/workers

Điểm vào của các tiến trình chạy lâu (`python -m app.workers.<tên>.main`):

- `ingest_consumer/`: đọc bài / bình luận từ Kafka, phân loại, ghi D1.
- `scheduler/`: lịch cào và mọi job nền.
- `lake_writer/`: lưu message Kafka thô lên R2.
