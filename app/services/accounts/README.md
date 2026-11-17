# app/services/accounts

Tài khoản và proxy cào (bảng `platform_accounts`, `platform_proxies` trên Supabase): đọc/ghi cấu hình, kiểm tra cookie
lúc nhập, User-Agent gốc của phiên, token và sức khoẻ tài khoản.

`platform_config_db.py` giờ chỉ là cửa ngõ re-export (17/11/2026): code thật nằm ở [config_db/](config_db/README.md), chia theo mảng
(tài khoản/proxy, lịch, AI, proxy_settings, dọn dẹp, ShortVid). Test monkeypatch phải patch module con.
