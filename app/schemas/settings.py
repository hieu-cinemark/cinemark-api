from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class AccountOut(BaseModel):
    id: int
    platform: str
    account_id: str
    password: str
    totp_secret: str
    cookie: str
    token: str
    email: str
    enabled: bool
    created_at: datetime
    updated_at: datetime


class AccountCreate(BaseModel):
    platform: str
    account_id: str
    password: str = ""
    totp_secret: str = ""
    cookie: str = ""
    token: str = ""
    email: str = ""
    enabled: bool = True


class AccountUpdate(BaseModel):
    account_id: str | None = None
    password: str | None = None
    totp_secret: str | None = None
    cookie: str | None = None
    token: str | None = None
    email: str | None = None
    enabled: bool | None = None


class ProxyOut(BaseModel):
    id: int
    platform: str
    proxy_url: str
    username: str
    password: str
    login_use_proxy: bool
    enabled: bool
    created_at: datetime
    updated_at: datetime


class ProxyCreate(BaseModel):
    platform: str = "all"
    proxy_url: str
    username: str = ""
    password: str = ""
    login_use_proxy: bool = False
    enabled: bool = True


class ProxyUpdate(BaseModel):
    platform: str | None = None
    proxy_url: str | None = None
    username: str | None = None
    password: str | None = None
    login_use_proxy: bool | None = None
    enabled: bool | None = None


class CronJob(BaseModel):
    name: str
    schedule: str
    source: str
    description: str
    last_run_at: datetime | None = None
