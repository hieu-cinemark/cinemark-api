"""One-time migration: copies Kira/Bee credentials from .env
(KIRA_BASE_URL/KIRA_API_KEY, BEEKNOEE_BASE_URL/BEEKNOEE_API_KEY) plus the
currently-effective model into the new ai_providers Supabase table (see
app/services/platform_config_db.py), so app/ai/client.py can load them
from there instead of env vars. Safe to re-run - upserts by key.

Reads .env directly (not app.core.config.Settings, which no longer
declares these fields - they've already been removed there on the
environment this migration was first run against) so this still works
against any OTHER environment (staging/another server's .env) that hasn't
migrated yet. Delete the KIRA_*/BEEKNOEE_* lines from that environment's
.env once this has run successfully there too.

Usage: .venv/bin/python -m scripts.migrate_ai_provider_credentials
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import dotenv_values

from app.services.platform_config_db import get_ai_provider, get_ai_settings, upsert_ai_provider

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _env(name: str) -> str | None:
    # Real process env wins (matches pydantic-settings' own precedence),
    # falling back to whatever's still in .env.
    return os.getenv(name) or dotenv_values(_ENV_PATH).get(name) or None


async def main() -> None:
    kira_base_url, kira_api_key = _env("KIRA_BASE_URL"), _env("KIRA_API_KEY")
    if kira_base_url and kira_api_key:
        # No model names in code: KIRA_MODEL, else whatever Supabase already
        # holds (the provider row, then the legacy ai_settings.model).
        existing = await get_ai_provider("kira") or {}
        legacy = await get_ai_settings()
        kira_model = (
            _env("KIRA_MODEL") or str(existing.get("model") or "").strip() or str(legacy.get("model") or "").strip()
        )
        await upsert_ai_provider("kira", base_url=kira_base_url, api_key=kira_api_key, model=kira_model)
        print("migrated kira provider credentials")
    else:
        print("skipped kira: KIRA_BASE_URL/KIRA_API_KEY not set in .env")

    bee_base_url, bee_api_key = _env("BEEKNOEE_BASE_URL"), _env("BEEKNOEE_API_KEY")
    if bee_base_url and bee_api_key:
        existing = await get_ai_provider("bee") or {}
        bee_model = _env("BEEKNOEE_MODEL") or str(existing.get("model") or "").strip()
        await upsert_ai_provider("bee", base_url=bee_base_url, api_key=bee_api_key, model=bee_model)
        print("migrated bee provider credentials")
    else:
        print("skipped bee: BEEKNOEE_BASE_URL/BEEKNOEE_API_KEY not set in .env")

    for key in ("kira", "bee"):
        row = await get_ai_provider(key)
        if row:
            print(f"verify {key}: base_url={row['base_url']!r} model={row['model']!r} api_key_set={bool(row['api_key'])}")
            if not row["model"]:
                print(f"  -> {key} has no model yet: set it in the dashboard (Settings > AI providers)")
        else:
            print(f"verify {key}: no row")


if __name__ == "__main__":
    asyncio.run(main())
