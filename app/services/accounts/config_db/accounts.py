"""Tài khoản nền tảng (platform_accounts) và proxy (platform_proxies): đọc/ghi từ trang Settings, sự kiện tài khoản.

Dữ liệu nằm ở D1 (cinemark-config-db); cài đặt chi tiết ở d1_accounts.py, file này giữ tên hàm cũ cho nơi gọi."""

from __future__ import annotations

from app.services.accounts.config_db.d1_accounts import (  # noqa: F401
    count_crawl_ready_accounts,
    create_account,
    create_proxy,
    delete_account,
    delete_proxy,
    get_account,
    list_accounts,
    list_proxies,
    record_account_event,
    set_account_enabled,
    update_account,
    update_account_check_result,
    update_proxy,
)
