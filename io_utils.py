from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from typing import Any

import httpx
from aiobotocore.session import get_session
from botocore.config import Config as BotoCoreConfig

from runtime_files import get_file_record, is_file_ref
from security.egress import check_outbound_url

JsonDict = dict[str, Any]


class FileIoService:
    """
    统一 IO 服务：
    负责将各种来源 (Base64, S3, URL 等) 的 File Reference 解析为
    统一的 bytes 或 stream 供节点消费。
    """

    @classmethod
    async def get_file_bytes(cls, ctx: JsonDict, file_ref: JsonDict) -> tuple[bytes, str, str | None]:
        """
        获取文件的完整二进制内容。

        Returns:
            (content_bytes, filename, mime_type)
        """
        if not is_file_ref(file_ref):
            raise ValueError("Input is not a valid file reference")

        file_id = file_ref.get("id")
        # 优先从 Context 查找完整记录 (可能包含 secret meta)
        record = get_file_record(ctx, file_id) if file_id else None
        # 如果 Context 没找到，就用传入的 ref 本身 (可能只是轻量引用)
        ref_data = record or file_ref

        filename = str(ref_data.get("name") or "unknown_file")
        mime = str(ref_data.get("mime") or "application/octet-stream")
        source = ref_data.get("source")

        # 1. Base64 Inline
        if ref_data.get("content_base64"):
            try:
                data = base64.b64decode(ref_data["content_base64"])
                return data, filename, mime
            except Exception as e:
                raise ValueError(f"Failed to decode base64 content: {e}")

        # 2. S3 Reference
        if source == "s3" or source == "external_s3":
            meta = ref_data.get("meta")
            if meta and meta.get("bucket") and meta.get("key"):
                return await cls._read_s3_bytes(meta), filename, mime

        # 3. URL Reference (Public or Presigned)
        url = ref_data.get("url")
        if url:
            return await cls._read_http_bytes(url), filename, mime

        raise ValueError(f"Unable to resolve content for file: {file_id} (source={source})")

    @classmethod
    async def get_file_stream(cls, ctx: JsonDict, file_ref: JsonDict) -> tuple[AsyncIterator[bytes], str, str | None, int | None]:
        """
        获取文件的异步读取流。

        Returns:
            (async_stream_iterator, filename, mime_type, size)
        """
        if not is_file_ref(file_ref):
            raise ValueError("Input is not a valid file reference")

        file_id = file_ref.get("id")
        record = get_file_record(ctx, file_id) if file_id else None
        ref_data = record or file_ref

        filename = str(ref_data.get("name") or "unknown_file")
        mime = str(ref_data.get("mime") or "application/octet-stream")
        size = ref_data.get("size")
        if size is not None:
            size = int(size)

        source = ref_data.get("source")

        # 1. Base64 Inline (Wrap in async iterator)
        if ref_data.get("content_base64"):
            data = base64.b64decode(ref_data["content_base64"])
            async def _iter():
                yield data
            return _iter(), filename, mime, len(data)

        # 2. S3 Reference
        if source == "s3" or source == "external_s3":
            meta = ref_data.get("meta")
            if meta and meta.get("bucket") and meta.get("key"):
                stream = await cls._get_s3_stream(meta)
                return stream, filename, mime, size

        # 3. URL Reference
        url = ref_data.get("url")
        if url:
            data = await cls._read_http_bytes(url)
            async def _iter_url():
                yield data
            return _iter_url(), filename, mime, len(data)

        raise ValueError(f"Unable to resolve stream for file: {file_id}")

    # --- Internal Helpers ---

    @staticmethod
    async def _read_http_bytes(url: str) -> bytes:
        # 出站网络策略：文件下载同样必须通过校验。
        check_outbound_url(url)
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, follow_redirects=True)
            # 重定向后目标可能指向内网，必须复检。
            if str(resp.url) != url:
                check_outbound_url(str(resp.url))
            resp.raise_for_status()
            return resp.content

    @staticmethod
    async def _create_s3_client(meta: dict[str, Any]):
        session = get_session()
        config = BotoCoreConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3},
            connect_timeout=5,
            read_timeout=60,
        )
        # 兼容 meta 中不同的字段命名习惯，优先取标准字段
        client_kwargs = {
            "region_name": meta.get("region_name") or meta.get("region"),
            "aws_access_key_id": meta.get("access_key_id") or meta.get("aws_access_key_id"),
            "aws_secret_access_key": meta.get("secret_access_key") or meta.get("aws_secret_access_key"),
            "endpoint_url": meta.get("endpoint_url") or meta.get("endpoint"),
            "config": config,
        }
        # 过滤 None 值，避免 boto3 报错
        client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}

        return session.create_client("s3", **client_kwargs)

    @classmethod
    async def _read_s3_bytes(cls, meta: dict[str, Any]) -> bytes:
        bucket = meta.get("bucket")
        key = meta.get("key")
        if not bucket or not key:
            raise ValueError("Incomplete S3 metadata")

        # S3 endpoint 出站校验：允许配置的 S3 端点，但必须通过策略。
        endpoint = meta.get("endpoint_url") or meta.get("endpoint")
        if endpoint:
            check_outbound_url(endpoint)

        async with await cls._create_s3_client(meta) as client:
            response = await client.get_object(Bucket=bucket, Key=key)
            async with response["Body"] as stream:
                return await stream.read()

    @classmethod
    async def _get_s3_stream(cls, meta: dict[str, Any]) -> AsyncIterator[bytes]:
        # 注意: 这里返回的 stream 依赖 client 的上下文。
        # 正确的做法应该让调用者管理 client 生命周期，或者使用 smart stream wrapper。
        # 为简化 MVP，这里暂时使用一次性读取 (TODO: 优化为真正的流式透传)
        # 实际上 aiobotocore 的 stream 在 client 关闭后可能不可读。
        # 这种实现是不完美的，对于极大文件会有问题。
        # 更好的实现是返回一个 context manager。

        # 临时方案：先读进内存再 yield (这就退化了，但安全)。
        # 真正的流式传输需要重构 Service 接口以支持 Context Manager。
        data = await cls._read_s3_bytes(meta)
        async def _yielder():
            yield data
        return _yielder()
