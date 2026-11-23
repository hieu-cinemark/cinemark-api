"""Cửa ngõ của các bảng cấu hình trên D1 (cinemark-config-db) - code đã tách theo mảng vào app/services/accounts/config_db/
(17/11/2026, trước đó một file ~1.500 dòng). File này chỉ re-export để mọi chỗ `from ... import platform_config_db as db` cũ
vẫn chạy. Code mới nên import thẳng module con; test monkeypatch phải patch module con (nơi hàm thật sự tra tên).

| Module | Bảng |
|---|---|
| accounts.py | platform_accounts, platform_proxies |
| schedules.py | filter_keywords, crawl_schedules, comment_crawl_schedules |
| ai_config.py | ai_settings, ai_providers, ai_accuracy_runs |
| proxy_settings.py | proxy_settings, proxy_providers (cinemark-api là nguồn sự thật, spider-hub chỉ đọc) |
| cleanup.py | cleanup_settings, cleanup_run_history |
| omar.py | discovery_settings, omar_api_settings |

Mọi lần ghi dựng SQL từ danh sách cột cố định đã duyệt trước cộng giá trị từ model Pydantic - không bao giờ từ dict request thô.
"""

from __future__ import annotations

from app.services.accounts.config_db.accounts import (  # noqa: F401
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
from app.services.accounts.config_db.ai_config import (  # noqa: F401
    delete_ai_provider,
    get_ai_provider,
    get_ai_settings,
    get_latest_accuracy_run,
    insert_accuracy_run,
    list_ai_providers,
    set_active_ai_provider,
    upsert_ai_provider,
    upsert_ai_settings,
)
from app.services.accounts.config_db.cleanup import (  # noqa: F401
    get_cleanup_settings,
    list_cleanup_run_history,
    record_cleanup_run_finish,
    record_cleanup_run_start,
    upsert_cleanup_settings,
)
from app.services.accounts.config_db.omar import (  # noqa: F401
    _omar_cap,
    get_discovery_settings,
    get_omar_api_settings,
    omar_limits,
    omar_requests_cap,
    set_discovery_enabled,
    set_omar_limits,
    upsert_omar_api_key,
)
from app.services.accounts.config_db.proxy_settings import (  # noqa: F401
    RENAMED_PROXY_SETTINGS,
    _crawl_settings_cache,
    crawl_setting,
    delete_proxy_provider,
    ensure_proxy_settings_complete,
    get_proxy_settings,
    list_proxy_providers,
    merge_proxy_settings,
    upsert_proxy_provider,
    upsert_proxy_settings,
)
from app.services.accounts.config_db.schedules import (  # noqa: F401
    create_filter_keyword,
    delete_filter_keyword,
    ensure_default_comment_crawl_schedules,
    ensure_default_crawl_schedules,
    list_comment_crawl_schedules,
    list_crawl_schedules,
    list_filter_keywords,
    mark_crawl_schedule_triggered,
    update_filter_keyword,
    upsert_comment_crawl_schedule,
    upsert_crawl_schedule,
)
