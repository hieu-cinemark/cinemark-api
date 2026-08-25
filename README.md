# cinemark-api

Kafka⇄D1 bridge for spider-hub's social-media crawls. Triggers crawls
(reads which movies/keywords to search for from cinemark-scraper's D1
database, publishes them to Kafka) and ingests the results (consumes
scraped posts off Kafka, writes them back into that same D1 database).

No database of its own - this service has no Postgres/SQL store. Every
piece of state it needs (movies, keywords, posts) already lives in
cinemark-scraper's Cloudflare D1 database; this service reads and writes it
over [D1's HTTP query API](https://developers.cloudflare.com/d1/best-practices/query-d1/#d1-http-api)
(`app/services/d1.py`) since D1 bindings only exist inside a Cloudflare
Worker, not a plain Python process.

## How this fits together

```
                    cinemark-api                            spider-hub
POST /facebook/run ─────►  reads enabled facebook keywords     
(manual, or                from D1 (cinemark-scraper's         
scripts/trigger_            movies/keywords tables)             
scheduled_crawl.sh,        │                                    
every 6h)                  ▼ Kafka "crawl_requests"             
                            ──────────────────────────────► crawl_request_consumer.py
                                                              runs `scrapy crawl facebook_search`
                                                                  │
                            ingest_consumer                       ▼ Kafka "raw_posts"
                            (separate long-running          ◄──────────────────────────────
                            process) consumes this,
                            resolves keyword_id -> movie_id
                            via D1, writes the post into
                            D1's `posts` table
```

See spider-hub's own README for the other half of this picture.

## Adding a platform

Everything platform-specific (how to map spider-hub's raw Kafka payload
into cinemark-scraper's D1 schema) lives in one place:
`app/services/platforms.py`. To add a platform spider-hub already knows how
to crawl (its own `SPIDER_BY_PLATFORM`):

1. Add a mapper function to `PLATFORM_POST_MAPPERS` in `platforms.py`.
2. Create `app/api/routes/<platform>.py`, same shape as `facebook.py`:
   `build_run_route(router, "<platform>")` plus whatever platform-specific
   extras it needs (Facebook's are `/refresh-token` and `/token-status`,
   for its Redis-cached login session - most platforms won't need
   anything like that).
3. Register the new router in `app/main.py`.
4. Add the platform to `PLATFORMS=()` in `scripts/trigger_scheduled_crawl.sh`.

Nothing else - `app/services/d1.py`, `app/api/routes/platform_scraper.py`,
`app/workers/ingest_consumer/` - needs to change; they're all already
platform-agnostic, driven by the registry.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in the values you need, see below
```

See `.env.example` for the full list of settings. Only `KAFKA_BOOTSTRAP_SERVERS`
and the three `CLOUDFLARE_*` vars matter for this service to actually do
anything - everything else has a working default or is optional
(`REDIS_*` just backs the Facebook token-status endpoint).

Cloudflare API tokens can't be scoped to a single D1 database (only to "D1:
Edit" for the whole account) - if that's a concern, restrict the token by
client IP instead when creating it.

## Running

Two separate long-running processes, same codebase:

```bash
# Web (trigger endpoints)
uvicorn app.main:app --reload

# Ingest worker (Kafka raw_posts -> D1)
python -m app.workers.ingest_consumer.main
```

`deploy/systemd/cinemark-ingest-consumer.service` runs the ingest worker as
a service in production (`deploy/systemd/install.sh` installs it). The web
process isn't in `deploy/` yet - run it behind whatever you're already using
(reverse proxy, another systemd unit, etc.).

### Docker

```bash
docker build -t cinemark-api .
docker run -p 8000:8000 --env-file .env cinemark-api                                # web
docker run --env-file .env cinemark-api python -m app.workers.ingest_consumer.main   # worker
```

One image, two roles, picked by the command the container runs with - see
the `Dockerfile`'s own comment. No `.env` is baked into the image; pass real
config via `--env-file`/`-e`/your orchestrator's secrets at run time.

If Kafka is also running in Docker (see the root `docker-compose.yml`),
containers reach it via `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` on the same
compose network - `localhost:9092` (used from the host, or from a container
run standalone like the examples above) only resolves to *that* container
itself, not the broker.

## Scheduled crawls

`scripts/trigger_scheduled_crawl.sh` calls `POST /<platform>/run` with an
empty body for every registered platform, currently every 6h (same cadence
cinemark-scraper's own Facebook cron ran at before spider-hub took over as
the primary source - see cinemark-scraper's `src/index.ts`). Install via:

```bash
crontab -e
# 0 */6 * * * /path/to/cinemark-api/scripts/trigger_scheduled_crawl.sh >> /path/to/cinemark-api/scripts/trigger_scheduled_crawl.log 2>&1
```

Every enabled keyword gets the same fixed cadence for now - per-keyword
frequency is a planned upgrade, not implemented yet.

## API

- `POST /<platform>/run` - trigger a crawl. Body (all fields optional):
  `{keyword_id?, movie_id?, max_pages?, start_date?, end_date?}`. No fields
  = every enabled keyword for that platform (the cron's case). `keyword_id`
  alone = just that keyword. `movie_id` alone = every enabled keyword for
  that movie.
- `POST /facebook/refresh-token` - manually trigger the Facebook session
  refresh spider-hub's own cron already runs every 4h.
- `GET /facebook/token-status` - whether spider-hub's cached Facebook
  session (Redis) is still valid, and how long until it expires.
- `GET /health` - liveness check, no dependencies.

## Testing

`tests/` is currently an empty placeholder - no test suite yet.
