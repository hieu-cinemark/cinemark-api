# app/services/accounts

Tài khoản và proxy cào (bảng `platform_accounts`, `platform_proxies` trên D1): đọc/ghi cấu hình, kiểm tra cookie
lúc nhập, User-Agent gốc của phiên, token và sức khoẻ tài khoản.

Truy cập bảng cấu hình nằm ở [config_db/](config_db/README.md), chia theo mảng (tài khoản/proxy, lịch, AI, proxy_settings,
dọn dẹp, Omar). Test monkeypatch phải patch module con.
