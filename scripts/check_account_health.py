"""Runs evaluate_account_health() for every account and persists the
result - the automated counterpart to the dashboard's manual "Check" button
(see app/api/routes/settings.py's POST /settings/accounts/{id}/check, which
does the same two steps for one account). Without this, the health signal
only ever updates when someone remembers to click the button - exactly the
"nobody was watching" gap that let TikTok's missing mapper (and Threads'
accounts) go unnoticed before (see app/workers/ingest_consumer/main.py's
own _note_drop for the same lesson applied to a different signal).

Alerts on Telegram only on a *transition* into warning/disabled - not on
every run - so a sustained outage doesn't re-alert on every single cron
cycle it stays broken (same "alert once, not every occurrence" reasoning as
_note_drop). A manual disable via the dashboard also alerts on its next
cron cycle, once - that's expected, not a bug: it's still true that the
account is now disabled.

Run periodically via cron (see scripts/trigger_scheduled_crawl.sh):
    python -m scripts.check_account_health
"""

from __future__ import annotations

import asyncio

from app.core.logging import get_logger
from app.services.account_health import evaluate_account_health
from app.services.platform_config_db import list_accounts, update_account_check_result
from app.services.telegram import send_telegram_message

logger = get_logger(__name__)

_DEGRADED_STATUSES = {"warning", "disabled"}


async def check() -> None:
    accounts = await list_accounts()
    for account in accounts:
        previous_status = account["last_check_status"]
        status = await evaluate_account_health(account)
        await update_account_check_result(account["id"], status=status)

        newly_degraded = status in _DEGRADED_STATUSES and previous_status not in _DEGRADED_STATUSES
        if newly_degraded:
            await send_telegram_message(
                f"⚠️ Account health: {account['platform']}/{account['account_id']} is now "
                f"'{status}' (was '{previous_status or 'never checked'}') - check the Settings page."
            )

        logger.info(
            "account_health_checked",
            platform=account["platform"],
            account_id=account["account_id"],
            status=status,
            previous_status=previous_status,
        )


if __name__ == "__main__":
    asyncio.run(check())
