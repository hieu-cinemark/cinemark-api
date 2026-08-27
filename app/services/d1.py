"""Cloudflare D1 HTTP API client - lets this VPS-hosted service read/write a
D1 database without needing a Cloudflare Worker (D1 bindings only exist
inside Workers; from a plain process, D1's REST query API is the only door
in). Talks to the same D1 database cinemark-scraper's Worker owns, using its
existing movies/keywords/posts/post_engagement_snapshots tables (see
cinemark-scraper/src/db/schema.ts) - not a separate table of our own, so a
crawl triggered from here (get_enabled_keywords/get_keyword) and the post it
produces (persist_post) share the exact same movie_id/keyword_id space, no
ID-mapping layer needed.

Platform-agnostic: every function here works off app.services.platforms'
registered_platforms(), not a hardcoded "facebook" literal - see that
module's docstring for what adding a new platform requires.

Best-effort throughout: every function here returns None/[]/False and logs
on failure (missing config, network error, HTTP error) instead of raising -
a D1 write that fails must never take down Kafka ingestion, which is the
only durable delivery guarantee this service has."""

from __future__ import annotations

import json
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import settings
from app.core.logging import get_logger
from app.services.platforms import PostDraft, registered_platforms

logger = get_logger(__name__)

_BASE_URL = "https://api.cloudflare.com/client/v4"

ENGAGEMENT_FIELDS = ("like_count", "reply_count", "repost_count", "quote_count", "reshare_count", "view_count")

# Below this many characters (after trimming), a post's content is treated
# as junk - a bare reaction/emoji/one-word comment with nothing to analyze.
# Tune freely; this is a judgment call, not derived from anything.
MIN_CONTENT_LENGTH = 10


def _configured() -> bool:
    return bool(settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id)


async def d1_query(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]] | None:
    """Runs one SQL statement against the configured D1 database. Returns
    the result rows, or None if D1 isn't configured or the call failed."""
    if not _configured():
        return None

    url = (
        f"{_BASE_URL}/accounts/{settings.cloudflare_account_id}/d1/database/{settings.cloudflare_d1_database_id}/query"
    )
    headers = {"Authorization": f"Bearer {settings.cloudflare_api_token}"}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, headers=headers, json={"sql": sql, "params": params or []})
    except httpx.HTTPError as exc:
        logger.warning("d1_request_failed", error=str(exc))
        return None

    if resp.status_code >= 400:
        logger.warning("d1_request_failed", status=resp.status_code, body=resp.text[:500])
        return None

    data = resp.json()
    if not data.get("success"):
        logger.warning("d1_query_failed", errors=data.get("errors"))
        return None

    results = data.get("result") or []
    return results[0].get("results", []) if results else []


async def get_post_counts_by_platform() -> list[dict[str, Any]]:
    """Total posts ingested per platform, plus the most recent scrape - the
    "how many posts have we collected per platform" figure the dashboard's
    overview page shows. Left as a plain GROUP BY (no platform allowlist)
    so it reflects whatever's actually in the table, including platforms
    scraped by cinemark-scraper's own Worker (tiktok/threads) alongside
    the ones spider-hub feeds through this service."""
    rows = await d1_query(
        "SELECT platform, COUNT(*) AS count, MAX(scraped_at) AS last_scraped_at FROM posts GROUP BY platform"
    )
    return rows or []


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    """Daily post counts per platform for the last `days` days - feeds the
    dashboard's trend chart. scraped_at is stored as an ISO8601 string
    (see persist_post below), so its first 10 characters are always the
    YYYY-MM-DD date - simpler and more portable across D1's SQLite version
    than relying on strftime() to parse the timezone offset."""
    rows = await d1_query(
        """
        SELECT substr(scraped_at, 1, 10) AS day, platform, COUNT(*) AS count
        FROM posts
        WHERE scraped_at >= datetime('now', ?)
        GROUP BY day, platform
        ORDER BY day ASC
        """,
        [f"-{days} days"],
    )
    return rows or []


async def list_movies() -> list[dict[str, Any]]:
    """Every enabled movie - feeds the dashboard's "which movie does this
    new keyword belong to" picker when creating a keyword inline from the
    crawl-trigger form."""
    rows = await d1_query("SELECT id, title FROM movies WHERE enabled = 1 ORDER BY title ASC")
    return rows or []


async def get_or_create_keyword(movie_id: str, platform: str, keyword: str) -> dict[str, Any] | None:
    """Used by the dashboard's inline "type a new keyword" flow (crawl
    trigger form) - looks up an existing (movie_id, platform, keyword) row
    first (that triple has a unique index - see cinemark-scraper's
    src/db/schema.ts) so a repeat submission or a race with another tab
    just returns the same row instead of erroring on the constraint."""
    existing = await d1_query(
        """
        SELECT k.id, k.movie_id, m.title AS movie_title, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.movie_id = ? AND k.platform = ? AND k.keyword = ?
        """,
        [movie_id, platform, keyword],
    )
    if existing:
        return existing[0]

    movie_rows = await d1_query("SELECT title FROM movies WHERE id = ? AND enabled = 1", [movie_id])
    if not movie_rows:
        return None
    movie_title = movie_rows[0]["title"]

    keyword_id = f"kw_{uuid.uuid4()}"
    created_at = datetime.now(tz=timezone.utc).isoformat()
    inserted = await d1_query(
        "INSERT INTO keywords (id, movie_id, platform, keyword, enabled, created_at) VALUES (?, ?, ?, ?, 1, ?)",
        [keyword_id, movie_id, platform, keyword, created_at],
    )
    if inserted is None:
        return None
    return {"id": keyword_id, "movie_id": movie_id, "movie_title": movie_title, "keyword": keyword}


async def list_keywords(platform: str) -> list[dict[str, Any]]:
    """Every enabled keyword for this platform, with its movie's title -
    feeds the dashboard's keyword picker (GET /<platform>/keywords) so a
    manual crawl trigger can target one keyword instead of "every enabled
    keyword for this platform" (see get_enabled_keywords below, still used
    for that fan-out case)."""
    rows = await d1_query(
        """
        SELECT k.id, k.movie_id, m.title AS movie_title, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.platform = ? AND k.enabled = 1 AND m.enabled = 1
        ORDER BY m.title ASC, k.keyword ASC
        """,
        [platform],
    )
    return rows or []


async def get_keyword(keyword_id: str, platform: str) -> dict[str, Any] | None:
    """One enabled keyword by id, on the given platform, joined to its
    movie's enabled flag - mirrors what the deleted Postgres
    KeywordRepository.get() + movie lookup used to do for the manual "run
    one keyword" trigger. The platform argument is required, not
    incidental: each platform gets its own router (see
    app/api/routes/facebook.py + platform_scraper.py) that only ever wants
    keywords for itself - a keyword_id belonging to a different platform
    must not silently match here."""
    rows = await d1_query(
        """
        SELECT k.id, k.movie_id, k.platform, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.id = ? AND k.platform = ? AND k.enabled = 1 AND m.enabled = 1
        """,
        [keyword_id, platform],
    )
    return rows[0] if rows else None


async def get_enabled_keywords(platform: str, movie_id: str | None = None) -> list[dict[str, Any]]:
    """Every enabled keyword on the given platform (optionally scoped to
    one movie) whose movie is also enabled - used for both the "run all
    keywords for a movie" trigger and a platform's daily cron ("run
    everything for this platform") call."""
    conditions = ["k.platform = ?", "k.enabled = 1", "m.enabled = 1"]
    params: list[Any] = [platform]
    if movie_id:
        conditions.insert(0, "k.movie_id = ?")
        params.insert(0, movie_id)
    rows = await d1_query(
        f"""
        SELECT k.id, k.movie_id, k.platform, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE {" AND ".join(conditions)}
        """,
        params,
    )
    return rows or []


def _fold_for_keyword_match(text: str) -> str:
    """Lowercase, strip Vietnamese diacritics, remove spaces - ported from
    cinemark-scraper's src/lib/keyword-match.ts foldForKeywordMatch() so a
    post ingested here agrees with how posts in the same table compute
    keyword_match, whichever platform scraped it."""
    decomposed = unicodedata.normalize("NFD", text)
    without_marks = "".join(c for c in decomposed if not unicodedata.combining(c))
    without_dd = without_marks.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"\s+", "", without_dd.lower())


def _keyword_match_parts(keyword: str) -> list[str]:
    """Normal keyword -> one phrase that must appear in full; `+`-joined
    keyword -> every part must appear (any order) - mirrors
    keywordMatchParts() in keyword-match.ts."""
    if "+" in keyword:
        return [part.strip() for part in keyword.split("+") if part.strip()]
    trimmed = keyword.strip()
    return [trimmed] if trimmed else []


def _contains_keyword(content: str | None, keyword: str | None) -> bool:
    if not content or not keyword:
        return False
    haystack = _fold_for_keyword_match(content)
    parts = _keyword_match_parts(keyword)
    if not parts:
        return False
    for part in parts:
        needle = _fold_for_keyword_match(part)
        if not needle or needle not in haystack:
            return False
    return True


async def persist_post(*, movie_id: str, keyword_id: str, keyword: str, platform: str, draft: PostDraft) -> None:
    """Upsert one scraped post (any registered platform) straight into
    cinemark-scraper's own `posts` table (+ an engagement snapshot on
    change) - ported from its src/jobs/persist-post.ts so both the
    Worker's own scrapers and this Kafka-fed path write through the exact
    same logic. `draft` is already normalized by the platform's mapper
    (see app/services/platforms.py) - this function has no
    platform-specific field knowledge of its own."""
    if not _configured() or platform not in registered_platforms():
        return

    external_id = draft.get("external_id")
    if not external_id:
        return

    content = draft.get("content")

    # Junk filter: too-short content (a bare reaction/emoji has no real
    # signal to analyze) - skip entirely. Does NOT filter on keyword_match:
    # a real commenter writing an abbreviation, an unaccented Vietnamese
    # spelling, or the movie's English name would never literally contain
    # the configured keyword phrase, so gating storage on that match would
    # silently drop real posts. keyword_match is still computed and stored
    # below as a flag for callers to filter on if they choose to, same as
    # before - it's just not a reason to skip storing the post outright.
    if not content or len(content.strip()) < MIN_CONTENT_LENGTH:
        logger.info("post_skipped_junk", platform=platform, external_id=external_id, reason="content_too_short")
        return
    is_keyword_match = _contains_keyword(content, keyword)

    scraped_at = datetime.now(tz=timezone.utc).isoformat()
    media_json = json.dumps(draft.get("media") or {})
    raw_json = json.dumps(draft.get("raw")) if draft.get("raw") is not None else None
    engagement = {field: draft.get(field) or 0 for field in ENGAGEMENT_FIELDS}
    # D1 stores booleans as SQLite integers (0/1) - pass an int, not a JSON
    # bool, so the HTTP API binds it as the same type Drizzle's
    # integer(..., {mode: "boolean"}) column expects. Always 1 here - a
    # non-match already returned above.
    keyword_match = int(is_keyword_match)

    existing_rows = await d1_query(
        "SELECT id, like_count, reply_count, repost_count, quote_count, reshare_count, view_count "
        "FROM posts WHERE platform = ? AND external_id = ?",
        [platform, external_id],
    )
    existing = existing_rows[0] if existing_rows else None

    if existing is None:
        post_id = f"post_{uuid.uuid4()}"
        inserted = await d1_query(
            """
            INSERT INTO posts (
                id, movie_id, keyword_id, platform, external_id, url, author, content, media_json,
                like_count, reply_count, repost_count, quote_count, reshare_count, view_count,
                posted_at, scraped_at, raw_json, keyword_match
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                post_id,
                movie_id,
                keyword_id,
                platform,
                external_id,
                draft.get("url"),
                draft.get("author"),
                draft.get("content"),
                media_json,
                engagement["like_count"],
                engagement["reply_count"],
                engagement["repost_count"],
                engagement["quote_count"],
                engagement["reshare_count"],
                engagement["view_count"],
                draft.get("posted_at"),
                scraped_at,
                raw_json,
                keyword_match,
            ],
        )
        if inserted is None:
            # Insert failed (race with another message for the same
            # external_id hitting the unique index, D1 outage, ...) - the
            # post row doesn't exist, so a snapshot referencing post_id here
            # would be an orphan. Log and stop; the next re-scrape of this
            # post will retry the whole upsert from scratch.
            logger.warning("d1_post_insert_failed", platform=platform, external_id=external_id)
            return
        await d1_query(
            "INSERT INTO post_engagement_snapshots "
            "(id, post_id, recorded_at, like_count, reply_count, repost_count, quote_count, reshare_count, view_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [f"eng_{uuid.uuid4()}", post_id, scraped_at, *engagement.values()],
        )
        return

    post_id = existing["id"]
    changed = any(existing.get(field) != engagement[field] for field in ENGAGEMENT_FIELDS)

    updated = await d1_query(
        """
        UPDATE posts SET
            url = ?, author = ?, content = ?, media_json = ?,
            like_count = ?, reply_count = ?, repost_count = ?, quote_count = ?, reshare_count = ?, view_count = ?,
            posted_at = ?, scraped_at = ?, raw_json = ?, keyword_match = ?
        WHERE id = ?
        """,
        [
            draft.get("url"),
            draft.get("author"),
            draft.get("content"),
            media_json,
            engagement["like_count"],
            engagement["reply_count"],
            engagement["repost_count"],
            engagement["quote_count"],
            engagement["reshare_count"],
            engagement["view_count"],
            draft.get("posted_at"),
            scraped_at,
            raw_json,
            keyword_match,
            post_id,
        ],
    )
    if updated is None:
        # UPDATE failed (D1 outage, ...) - post_id still refers to a real,
        # pre-existing row (unlike the insert branch above), so nothing's
        # orphaned, but the engagement numbers below would reflect this
        # message's payload, not what's actually stored. Skip the snapshot;
        # the next re-scrape retries the whole upsert.
        logger.warning("d1_post_update_failed", platform=platform, external_id=external_id, post_id=post_id)
        return
    if changed:
        await d1_query(
            "INSERT INTO post_engagement_snapshots "
            "(id, post_id, recorded_at, like_count, reply_count, repost_count, quote_count, reshare_count, view_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [f"eng_{uuid.uuid4()}", post_id, scraped_at, *engagement.values()],
        )
