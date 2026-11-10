# app

Mã nguồn của service. `main.py` dựng ứng dụng FastAPI (log, middleware, handler lỗi, router).

Quy ước phụ thuộc: `api` -> `services` -> `repositories` / `clients` / `ai`; `core` dùng được ở mọi tầng.
`workers` là điểm vào của các tiến trình chạy nền, gọi xuống `services`.
