# app/api/routes

Mỗi file một nhóm route. Route chỉ kiểm tra đầu vào, gọi `app/services` và trả JSON.

| File | Nội dung |
|---|---|
| `platform_scraper.py` | `POST /<nền tảng>/run` dùng chung để kích hoạt cào |
| `facebook.py`, `threads.py`, `tiktok.py` | Route riêng của từng nền tảng |
| `keywords.py`, `movies.py` | Từ khoá và phim |
| `settings.py` | Tài khoản, proxy, lịch cào, cấu hình AI |
| `jobs.py`, `logs.py`, `stats.py` | Hàng đợi, log, thống kê cho dashboard |
| `token_refresh.py` | Làm mới phiên đăng nhập |
| `omar.py`, `research.py`, `cron.py`, `auth.py`, `health.py` | Omar API, tin báo chí, danh sách cron, kiểm tra khoá, health |
