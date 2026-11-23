# Chuyển bảng cấu hình từ Supabase sang D1

Từ 23/11/2026 toàn bộ bảng cấu hình có thể nằm ở D1 `cinemark-config-db` thay vì Supabase. Mặc định vẫn là Supabase; chọn nơi lưu bằng
`CONFIG_DB_BACKEND=postgres|d1` (một cờ chung cho cinemark-api và spider-hub).

## Bảng đã có bản D1

| Nhóm | Bảng | cinemark-api | spider-hub |
|---|---|---|---|
| AI + cào | `ai_settings`, `ai_providers`, `ai_accuracy_runs`, `proxy_settings` | `config_db/d1_store.py` | `db/config.py`, `db/proxy_settings.py` (đọc) |
| Vận hành | `filter_keywords`, `crawl_schedules`, `comment_crawl_schedules`, `cleanup_settings`, `cleanup_run_history`, `discovery_settings`, `omar_api_settings` | `config_db/d1_ops.py` | không đọc |
| Tài khoản/proxy | `platform_accounts`, `platform_proxies`, `proxy_providers`, `account_events` | `config_db/d1_accounts.py` | `db/d1_pool.py` (đọc + ghi, qua `accounts.py`, `proxies.py`, `account_events.py`, `proxy_settings.py`, `runtime/proxy_lease.py`) |

- cinemark-api dựng query bằng SQLAlchemy Core (dialect SQLite) rồi gửi qua `d1_exec(..., database="config")`; mỗi hàm trong `config_db/*.py`
  có một nhánh `if d1_store.enabled()` ở đầu, chữ ký và dữ liệu trả về giữ nguyên (bool, dict, datetime).
- cinemark-api là nơi TẠO bảng (`ensure_tables`, `IF NOT EXISTS`, chạy ở lần dùng đầu và khi khởi động). spider-hub chỉ đọc/ghi dữ liệu, nên
  deploy cinemark-api (hoặc chạy script chép) trước khi bật cờ ở spider-hub.
- spider-hub không dùng SQLAlchemy: `db/d1.py` là client httpx đồng bộ, `db/d1_pool.py` viết SQL SQLite trực tiếp (thời gian tính ở Python,
  tên sự kiện log giữ nguyên bản Postgres). Script trong `scripts/` dùng `db/script_connect.py` (tự chọn Postgres hoặc D1).
- Định dạng thời gian trong D1: chuỗi ISO-8601 UTC độ rộng cố định (`2026-11-23T02:17:34.025795+00:00`) để so sánh chuỗi đúng thứ tự.
- Khác biệt so với Postgres:
  - Chọn-và-giữ chỗ tài khoản/proxy (`claim_account`, `claim_proxy`) là MỘT câu `UPDATE ... WHERE id = (SELECT ... LIMIT 1) RETURNING`, D1 chạy
    từng câu nguyên tử nên thay được `FOR UPDATE SKIP LOCKED`.
  - Cooldown luỹ thừa tính bằng `strftime('now', '+N minutes')` với số mũ chặn ở 20 và trần 2 giờ / `cooldown_max_minutes`.
  - Bảng Postgres cũ còn các cột không code nào dùng (`assigned_proxy_id`, `needs_manual_login`, `last_relogin_*`, `session_client*`,
    `manual_nurture_days`, `proxy_providers.kind/ip_allowlist`): không chép sang D1.

## Cutover (làm đúng thứ tự)

1. Thêm biến vào `.env`: cinemark-api cần `CLOUDFLARE_D1_CONFIG_DATABASE_ID` (id của `cinemark-config-db`, xem `wrangler d1 list`); spider-hub
   cần `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_D1_CONFIG_DATABASE_ID` (VPS và Mac). Token cần quyền D1 Edit.
2. Deploy bản mới của cả hai repo (cờ còn `postgres`, chưa đổi hành vi).
3. Tạm dừng crawler (spider-hub-crawl) và ngừng sửa cài đặt trên dashboard, rồi chép dữ liệu mới nhất, vì cookie/phiên tài khoản và cooldown
   đổi liên tục: `python -m scripts.tools.copy_config_to_d1` (xem trước số dòng) rồi `--apply`.
4. Đặt `CONFIG_DB_BACKEND=d1` ở cinemark-api (web + scheduler) và spider-hub (mọi tiến trình: crawl, consumer, dashboard nếu có), restart. Hai
   bên phải cùng giá trị: bên còn `postgres` sẽ đọc/ghi dữ liệu cũ.
5. Bật lại crawler. Kiểm tra: dashboard hiện đủ tài khoản/proxy; sửa thử một cài đặt rồi thấy spider-hub nhận trong vòng một phút; một lượt cào
   ghi `last_used_at` và `account_events` vào D1.
6. Quay lại nếu cần: đặt `CONFIG_DB_BACKEND=postgres` ở cả hai bên và restart (dữ liệu ghi lúc đang ở D1 không tự về Postgres).

## Bỏ hẳn Supabase (làm sau khi D1 chạy ổn một thời gian)

Chưa làm trong đợt này để còn đường quay lại. Khi đã yên tâm: xoá nhánh Postgres trong `config_db/*.py` và `db/*.py` (cùng `conn.py`,
`db/connection.py`, `scripts/dev_db_sync.py`, `scripts/migrate_proxy_providers.py`, `scripts/dev_db_schema.sql`), bỏ `psycopg`/`psycopg_pool`
khỏi requirements, bỏ `DATABASE_URL`/`LOCAL_DATABASE_URL`, rồi đóng project Supabase (giữ một bản `pg_dump` làm sao lưu).

## Lưu ý

- Worker cinemark vẫn đọc cấu hình AI qua bảng `ai_runtime` (đồng bộ từ `app/services/ops/ai_runtime_sync.py`), không đổi.
- D1 không có transaction nhiều câu: các hàm ở trên chỉ ghi một dòng mỗi câu nên không cần; `delete_proxy_provider` kiểm tra rồi mới xoá (có
  khoảng hở rất nhỏ giữa hai bước, chấp nhận được với thao tác thủ công trên dashboard).
- Mỗi lượt cào gọi D1 qua mạng (vài chục ms đến vài trăm ms mỗi câu) - ngang Supabase trước đây. Cài đặt vẫn cache 60 giây ở spider-hub.
- Schema D1 khai ở `d1_store.py`/`d1_ops.py`/`d1_accounts.py` (cinemark-api); `tests/test_d1_pool.py` của spider-hub có bản DDL chép tay để
  chạy SQL thật trên SQLite - đổi cột ở cinemark-api thì sửa cả hai.
