#!/bin/bash
# Scheduled entry point (every 6h, on the hour) for the internal
# analytics/health jobs below - account health, volume-anomaly detection,
# AI topic reports. Does NOT trigger any crawls itself anymore: that moved
# to app/services/scheduler.py, an in-process daily scheduler driven by the
# dashboard's "Crawl schedule" card (crawl_schedules table) - editable
# without touching a crontab, unlike the old fixed "every 6h, for every
# platform, no way to change it" loop this script used to run here. See
# that module's docstring for the full rationale, including why it also
# replaces cinemark-scraper's Cloudflare Cron Triggers for Threads/TikTok.
#
# Install once via `crontab -e`:
#   0 */6 * * * /path/to/cinemark-api/scripts/trigger_scheduled_crawl.sh >> /path/to/cinemark-api/scripts/trigger_scheduled_crawl.log 2>&1

set -uo pipefail

log() { echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $*"; }

status=0
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Unlike check_volume_anomaly.py below, this has no time-of-day bias to
# work around (it reads current Redis/enabled state, not a full-day count),
# so it runs on every cycle - the more often it runs, the sooner a
# newly-degraded account gets caught.
log "=== Checking account health ==="
if (cd "$REPO_DIR" && source .venv/bin/activate && python -m scripts.check_account_health); then
    log "Account health check OK"
else
    log "Account health check FAILED"
    status=1
fi

# check_volume_anomaly.py compares *today's* count so far against the
# median of the last 7 full days - run this on every 6h cycle and the
# 00:00/06:00/12:00 runs would compare a partial day against full-day
# baselines, firing a false "volume dropped" alert almost every time
# (today's count so far is naturally far below a full day's median until
# late in the day). Only run it on the last cycle before the day rolls
# over (hour 18, paired with the "0 */6 * * *" schedule this script's own
# header documents) - today's count is still ~6h short of complete then
# too, but that fixed shortfall is a much smaller, steadier bias than
# comparing a few hours' worth of posts against a full day.
current_hour="$(date -u +%H)"
if [ "$current_hour" = "18" ]; then
    log "=== Checking for volume anomalies (last cycle of the day) ==="
    if (cd "$REPO_DIR" && source .venv/bin/activate && python -m scripts.check_volume_anomaly); then
        log "Volume check OK"
    else
        log "Volume check FAILED"
        status=1
    fi
fi

# Regenerating the AI "top 10 topics" social listening report is a Kira-
# heavy batch job (topic clustering + verbatim selection + narrative, one
# movie at a time) and a movie's discussion topics don't meaningfully shift
# within a few hours at current comment volume - so, same "don't run this
# on every 6h cycle" reasoning as check_volume_anomaly.py above, gate it to
# once a day. Deliberately a different hour (20, not 18) so the two
# Kira-consuming batch jobs never stack on the same cron tick.
if [ "$current_hour" = "20" ]; then
    log "=== Regenerating social topic reports (once/day) ==="
    if (cd "$REPO_DIR" && source .venv/bin/activate && python -m scripts.generate_social_topic_reports); then
        log "Social topic report generation OK"
    else
        log "Social topic report generation FAILED"
        status=1
    fi
fi

exit $status

