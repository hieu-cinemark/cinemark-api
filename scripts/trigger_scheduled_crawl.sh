#!/bin/bash
# Scheduled entry point (every 6h, on the hour): triggers a crawl for every
# enabled keyword across every movie, for each spider-hub-backed platform
# (currently just facebook - see app/services/platforms.py), by calling
# POST /<platform>/run with an empty body - the exact same endpoint (and
# Kafka contract) the "run" button in the admin UI uses for a single
# keyword/movie. There's no separate "scheduled run" code path to keep in
# sync with the manual one.
#
# Same cadence cinemark-scraper's own Facebook cron used before spider-hub
# took over as the primary source (see cinemark-scraper/src/index.ts) - "0
# */6 * * *", not a coincidence, keeps the handoff cadence-neutral.
#
# Every keyword gets the same fixed cadence for now - per-keyword frequency
# is a planned upgrade, not implemented yet.
#
# Install once via `crontab -e`:
#   0 */6 * * * /path/to/cinemark-api/scripts/trigger_scheduled_crawl.sh >> /path/to/cinemark-api/scripts/trigger_scheduled_crawl.log 2>&1

set -uo pipefail

CINEMARK_API_URL="${CINEMARK_API_URL:-http://localhost:8000}"

log() { echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $*"; }

# Firing at exactly :00 every 6h, forever, is itself a bot-like signal -
# a random startup delay (0-15 min) means the real request leaves at a
# slightly different moment each cycle without needing a different
# crontab entry. Skippable for manual/local runs via SKIP_STARTUP_JITTER=1.
if [ "${SKIP_STARTUP_JITTER:-0}" != "1" ]; then
    startup_delay=$((RANDOM % 900))
    log "Startup jitter: sleeping ${startup_delay}s before triggering"
    sleep "$startup_delay"
fi

# Add a platform here once it has its own router (app/api/routes/<platform>.py)
# registered in app/main.py - nothing else in this script changes.
#
# No commas between entries - bash arrays are space-separated. This used
# to read (facebook, threads, tiktok), which silently made the first two
# elements "facebook," and "threads," (comma glued onto the word since
# there's no space before it) and sent every scheduled trigger to
# "$CINEMARK_API_URL/facebook,/run" - a 404 - for those two platforms.
# Only tiktok (the one with no trailing comma) was ever actually running
# on this cron.
PLATFORMS=(facebook threads tiktok)

status=0
for platform in "${PLATFORMS[@]}"; do
    log "=== Triggering scheduled crawl: $platform (all enabled keywords) ==="

    response="$(curl -s -w '\n%{http_code}' -X POST "$CINEMARK_API_URL/$platform/run" \
        -H "Content-Type: application/json" -d '{}')"
    http_code="$(echo "$response" | tail -n1)"
    body="$(echo "$response" | sed '$d')"

    if [ "$http_code" != "200" ]; then
        log "Trigger FAILED for $platform (HTTP $http_code): $body"
        status=1
        continue
    fi

    log "Trigger OK for $platform: $body"
done

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
    REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if (cd "$REPO_DIR" && source .venv/bin/activate && python -m scripts.check_volume_anomaly); then
        log "Volume check OK"
    else
        log "Volume check FAILED"
        status=1
    fi
fi

exit $status

