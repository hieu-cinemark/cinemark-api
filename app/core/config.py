"""Central app settings - one Settings instance, read from environment
variables / .env at import time. Every other module reads config through
`settings`, not `os.getenv(...)` directly, so there's exactly one place
that knows what env vars this service needs."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "development"  # development | production
    log_level: str = "INFO"
    log_format: str = "console"  # "console" (human-readable) | "json" (prod, machine-parseable)

    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None

    kafka_bootstrap_servers: str = "localhost:9092"

    # Same Redis instance spider-hub's RedisCache connects to (see
    # social_crawler/services/redis.py there) - read-only from this side,
    # just to report the Facebook session cache's status. Env var names
    # match spider-hub's for a shared .env to work.
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None

    cors_origins: str = "http://localhost:3000"

    # Cloudflare D1 - lets the ingest consumer read/write cinemark-scraper's
    # D1 database over Cloudflare's HTTP query API (D1 bindings only exist
    # inside a Worker; this is the only way in from a plain VPS process).
    # See app/services/d1.py. All optional - the D1 calls are skipped (not
    # an error) if these aren't set.
    cloudflare_account_id: str | None = None
    cloudflare_api_token: str | None = None
    cloudflare_d1_database_id: str | None = None

    # Log files the dashboard's /logs routes tail - both processes are
    # plain text files written by structlog's ConsoleRenderer (see
    # spider-hub/social_crawler/logger.py and app/core/logging.py). Default
    # assumes the sibling-checkout layout this workspace already uses
    # (cinemark-api/ and spider-hub/ side by side); override in .env if a
    # deployment puts them elsewhere.
    spider_hub_consumer_log_path: str = str(_REPO_ROOT.parent / "spider-hub" / "consumer.log")
    ingest_consumer_log_path: str = str(_REPO_ROOT / "ingest_consumer.log")

    # Same Supabase Postgres instance spider-hub's services/db.py reads
    # account/proxy config from (platform_accounts / platform_proxies
    # tables) - this side gets read/write access too, for the dashboard's
    # Settings page. None (not an error) if unset - GET/POST/PATCH/DELETE
    # on /settings/* just 502 instead of the app failing to boot.
    database_url: str | None = None

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
