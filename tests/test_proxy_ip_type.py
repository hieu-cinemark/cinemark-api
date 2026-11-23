"""Nhãn IP tĩnh / động của proxy: ghi được qua API, giá trị lạ bị từ chối, PATCH không đụng nhãn khi không gửi."""

import pytest
from pydantic import ValidationError

from app.schemas.settings import ProxyCreate, ProxyUpdate
from app.services.accounts.config_db import d1_accounts


def test_create_defaults_to_unknown_and_is_persisted_column():
    assert ProxyCreate(proxy_url="1.2.3.4:80").ip_type == "unknown"
    assert "ip_type" in {column.name for column in d1_accounts.platform_proxies.c}


def test_invalid_value_is_rejected():
    with pytest.raises(ValidationError):
        ProxyCreate(proxy_url="1.2.3.4:80", ip_type="sticky")


def test_update_leaves_label_alone_unless_sent():
    assert "ip_type" not in ProxyUpdate(enabled=False).model_dump(exclude_unset=True)
    assert ProxyUpdate(ip_type="static").model_dump(exclude_unset=True) == {"ip_type": "static"}
