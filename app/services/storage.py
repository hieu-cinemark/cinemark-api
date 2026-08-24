"""Cloudflare R2 (S3-compatible) storage - archives scraped media (post
images, video thumbnails, comment stickers/gifs/images) by downloading them
from Facebook's own CDN and re-uploading to R2. Facebook's media URLs are
signed and expire, so a post/comment persisted today can have a dead
media_url a week later unless it's archived at ingest time.

Best-effort: every function here returns None on failure (missing config,
network error, upload error) instead of raising - archiving is a nice-to-
have, not something that should fail post/comment ingestion."""

from __future__ import annotations

import asyncio
import mimetypes
import uuid

import boto3
import httpx
from botocore.config import Config

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_client = None


def _r2_configured() -> bool:
    return bool(settings.r2_account_id and settings.r2_access_key_id and settings.r2_secret_access_key and settings.r2_bucket_name)


def _get_client():
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )
    return _client


def _put_object(key: str, body: bytes, content_type: str) -> None:
    # boto3 has no native async API - this runs in a thread (see
    # archive_media_url) so it doesn't block the event loop.
    _get_client().put_object(Bucket=settings.r2_bucket_name, Key=key, Body=body, ContentType=content_type)


def public_url(key: str) -> str:
    """Builds a public URL for an object key if R2_PUBLIC_URL (a custom
    domain mapped to the bucket) is configured; otherwise falls back to the
    key itself, since the bucket may be private and callers might resolve
    it another way (e.g. a signed URL generated elsewhere)."""
    if settings.r2_public_url:
        return f"{settings.r2_public_url.rstrip('/')}/{key}"
    return key


async def archive_media_url(source_url: str | None, *, prefix: str) -> str | None:
    """Downloads source_url and uploads it to R2 under `prefix/<uuid>.<ext>`.
    Returns the object key (not a URL - see public_url()), or None if
    source_url is empty, R2 isn't configured, or anything fails."""
    if not source_url or not _r2_configured():
        return None

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as http:
            resp = await http.get(source_url)
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("media_download_failed", url=source_url, error=str(exc))
        return None

    content_type = resp.headers.get("content-type", "application/octet-stream").split(";")[0]
    ext = mimetypes.guess_extension(content_type) or ""
    key = f"{prefix}/{uuid.uuid4()}{ext}"

    try:
        await asyncio.to_thread(_put_object, key, resp.content, content_type)
    except Exception as exc:
        logger.warning("media_upload_failed", key=key, error=str(exc))
        return None

    return key
