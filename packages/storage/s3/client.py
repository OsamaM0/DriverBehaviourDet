"""S3 (or MinIO) async helpers for evidence + model artifacts."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import boto3
from botocore.config import Config

from packages.common.config import settings


def _make_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint or None,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


_client = _make_client()


async def put_object(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    def _do() -> None:
        _client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    await asyncio.to_thread(_do)
    return f"s3://{bucket}/{key}"


@asynccontextmanager
async def stream_get(bucket: str, key: str):
    def _do():
        return _client.get_object(Bucket=bucket, Key=key)
    obj = await asyncio.to_thread(_do)
    try:
        yield obj["Body"]
    finally:
        obj["Body"].close()
