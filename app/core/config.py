"""Central app settings - one Settings instance, read from environment
variables / .env at import time. Every other module reads config through
`settings`, not `os.getenv(...)` directly, so there's exactly one place
that knows what env vars this service needs."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "development"  # development | production
    log_level: str = "INFO"
    log_format: str = "console"  # "console" (human-readable) | "json" (prod, machine-parseable)

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

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
