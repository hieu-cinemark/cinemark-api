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

    database_url: str = "postgresql+asyncpg://cinemark:cinemark@localhost:5432/cinemark"
    kafka_bootstrap_servers: str = "localhost:9092"

    jwt_secret_key: str = "dev-only-insecure-secret-change-me"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    refresh_token_expire_days: int = 30

    cors_origins: str = "http://localhost:3000"

    # Cloudflare R2 (S3-compatible) - archives scraped media (post/comment
    # images, video thumbnails, stickers, gifs) since Facebook's own CDN
    # URLs are signed and expire; downloading once at ingest time and
    # re-hosting in R2 keeps them available long-term. All optional - media
    # archiving is skipped (not an error) if these aren't set.
    r2_account_id: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_bucket_name: str | None = None
    r2_public_url: str | None = None  # e.g. https://media.yourdomain.com, if the bucket has a public custom domain

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
