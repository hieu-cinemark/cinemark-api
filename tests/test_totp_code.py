"""POST /settings/accounts/{id}/totp-code: secret có khoảng trắng vẫn ra mã, secret hỏng báo lỗi rõ ràng."""

import asyncio

import pytest
from pyotp import TOTP

from app.api.routes import settings as routes
from app.core.errors import ValidationError

SECRET = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"


def _call(monkeypatch, secret):
    async def get_account(account_id):
        return {"totp_secret": secret}

    monkeypatch.setattr(routes.db, "get_account", get_account)
    return asyncio.run(routes.get_totp_code(122))


def test_spaced_secret_gives_the_same_code(monkeypatch):
    spaced = " ".join(SECRET[i : i + 4] for i in range(0, len(SECRET), 4))
    assert _call(monkeypatch, spaced).code == TOTP(SECRET).now()


def test_invalid_secret_is_a_validation_error(monkeypatch):
    with pytest.raises(ValidationError):
        _call(monkeypatch, "not-base32!")


def test_missing_secret_is_a_validation_error(monkeypatch):
    with pytest.raises(ValidationError):
        _call(monkeypatch, "   ")
