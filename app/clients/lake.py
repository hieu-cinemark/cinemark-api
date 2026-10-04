"""Client data lake R2 (qua S3 API). Dùng bởi app/workers/lake_writer và các script
backfill - binding MEDIA_BUCKET phía Worker là một bucket khác, dành cho file người
dùng tải lên."""

from __future__ import annotations

import aioboto3

from app.core.config import settings

_session = aioboto3.Session()


def lake_configured() -> bool:
    return bool(
        settings.r2_endpoint and settings.r2_access_key_id and settings.r2_secret_access_key and settings.lake_bucket
    )


def _client():
    return _session.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name="auto",
    )


async def put_object(key: str, body: bytes, content_type: str = "application/gzip") -> None:
    # Cố ý không đặt ContentEncoding=gzip: các HTTP client (kể cả DuckDB) sẽ tự giải nén
    # gzip, rồi lỗi khi đọc file .gz lần thứ hai.
    async with _client() as s3:
        await s3.put_object(Bucket=settings.lake_bucket, Key=key, Body=body, ContentType=content_type)


async def get_object(key: str) -> bytes:
    async with _client() as s3:
        response = await s3.get_object(Bucket=settings.lake_bucket, Key=key)
        async with response["Body"] as body:
            return await body.read()


async def delete_prefix(prefix: str) -> int:
    """Xoá mọi object dưới `prefix` (1000 object mỗi request, giới hạn của S3). Trả về số
    object đã xoá."""
    keys = await list_keys(prefix)
    async with _client() as s3:
        for start in range(0, len(keys), 1000):
            batch = [{"Key": key} for key in keys[start : start + 1000]]
            await s3.delete_objects(Bucket=settings.lake_bucket, Delete={"Objects": batch})
    return len(keys)


async def list_keys(prefix: str) -> list[str]:
    keys: list[str] = []
    async with _client() as s3:
        paginator = s3.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=settings.lake_bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys
