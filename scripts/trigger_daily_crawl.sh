#!/bin/bash
# Daily scheduled entry point: triggers a crawl for every enabled keyword
# across every movie, by calling POST /scraper/run with an empty body - the
# exact same endpoint (and Kafka contract) the "run" button in the admin UI
# uses for a single keyword/movie. There's no separate "scheduled run" code
# path to keep in sync with the manual one.
#
# Install once via `crontab -e`:
#   0 7 * * * /path/to/cinemark-api/scripts/trigger_daily_crawl.sh >> /path/to/cinemark-api/scripts/trigger_daily_crawl.log 2>&1

set -uo pipefail

CINEMARK_API_URL="${CINEMARK_API_URL:-http://localhost:8000}"

log() { echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $*"; }

log "=== Triggering daily crawl (all enabled keywords) ==="

response="$(curl -s -w '\n%{http_code}' -X POST "$CINEMARK_API_URL/scraper/run" \
    -H "Content-Type: application/json" -d '{}')"
http_code="$(echo "$response" | tail -n1)"
body="$(echo "$response" | sed '$d')"

if [ "$http_code" != "200" ]; then
    log "Trigger FAILED (HTTP $http_code): $body"
    exit 1
fi

log "Trigger OK: $body"
